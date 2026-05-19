# Copyright 2019-2026 by Vytautas Liuolia.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""ASGI multipart form media handler components."""

from __future__ import annotations

from collections.abc import AsyncIterator
from collections.abc import Awaitable
from typing import (
    Any,
    TYPE_CHECKING,
)

from falcon._typing import _UNSET
from falcon.asgi.reader import BufferedReader
from falcon.errors import DelimiterError
from falcon.media import multipart
from falcon.typing import AsyncReadableIO
from falcon.util.mediatypes import parse_header

if TYPE_CHECKING:
    from falcon.media.multipart import MultipartParseOptions

_ALLOWED_CONTENT_HEADERS = multipart._ALLOWED_CONTENT_HEADERS
_CRLF = multipart._CRLF
_CRLF_CRLF = multipart._CRLF_CRLF

MultipartParseError = multipart.MultipartParseError


class BodyPart(multipart.BodyPart):
    """Represents a body part in a multipart form in an ASGI application.

    Note:
        :class:`BodyPart` is meant to be instantiated directly only by the
        :class:`MultipartFormHandler` parser.
    """

    if TYPE_CHECKING:

        def __init__(
            self,
            stream: BufferedReader,
            headers: dict[bytes, bytes],
            parse_options: MultipartParseOptions,
        ): ...

    stream: BufferedReader  # type: ignore[assignment]
    """File-like input object for reading the body part of the
    multipart form request, if any. This object provides direct access
    to the server's data stream and is non-seekable. The stream is
    automatically delimited according to the multipart stream boundary.

    With the exception of being buffered to keep track of the boundary,
    the wrapped body part stream interface and behavior mimic
    :attr:`Request.stream <falcon.asgi.Request.stream>`.

    Similarly to :attr:`BoundedStream <falcon.asgi.BoundedStream>`,
    the most efficient way to read the body part content is asynchronous
    iteration over part data chunks:

    .. code:: python

        async for data_chunk in part.stream:
            pass
    """


    async def get_media(self) -> Any:
        """Return a deserialized form of the multipart body part.

        When called, this method will attempt to deserialize the body part
        stream using the Content-Type header as well as the media-type handlers
        configured via :class:`~falcon.media.multipart.MultipartParseOptions`.

        The result will be cached and returned in subsequent calls::

            deserialized_media = await part.get_media()

        Returns:
            object: The deserialized media representation.
        """
        if self._media is _UNSET:
            handler, _, _ = self._parse_options.media_handlers._resolve(
                self.content_type, 'text/plain'
            )

            try:
                self._media = await handler.deserialize_async(
                    self.stream, self.content_type, None
                )
            finally:
                if handler.exhaust_stream:
                    await self.stream.exhaust()

        return self._media


    data: Awaitable[bytes] = property(get_data)  # type: ignore[assignment]
    """Property that acts as a convenience alias for :meth:`~.get_data`.

    The ``await`` keyword must still be added when referencing
    the property::

        # Equivalent to: content = await part.get_data()
        content = await part.data
    """
    media: Awaitable[Any] = property(get_media)  # type: ignore[assignment]
    """Property that acts as a convenience alias for :meth:`~.get_media`.

    The ``await`` keyword must still be added when referencing
    the property::

        # Equivalent to: deserialized_media = await part.get_media()
        deserialized_media = await part.media
    """
    text: Awaitable[bytes] = property(get_text)  # type: ignore[assignment]
    """Property that acts as a convenience alias for :meth:`~.get_text`.

    The ``await`` keyword must still be added when referencing
    the property::

        # Equivalent to: decoded_text = await part.get_text()
        decoded_text = await part.text
    """


class MultipartForm:
    """Iterable object that returns each form part as :class:`BodyPart` instances.

    Typical usage illustrated below::

        async def on_post(self, req: Request, resp: Response) -> None:
            form: MultipartForm = await req.get_media()

            async for part in form:
                if part.name == 'foo':
                    ...
                else:
                    ...

    Note:
        :class:`MultipartForm` is meant to be instantiated directly only by the
        :class:`MultipartFormHandler` parser.
    """

    def __init__(
        self,
        stream: AsyncReadableIO,
        boundary: bytes,
        content_length: int | None,
        parse_options: MultipartParseOptions,
    ) -> None:
        self._stream = (
            stream if isinstance(stream, BufferedReader) else BufferedReader(stream)
        )
        self._boundary = boundary
        # NOTE(vytas): Here self._dash_boundary is not prepended with CRLF
        #   (yet) for parsing the prologue. The CRLF will be prepended later to
        #   construct the inter-part delimiter as per RFC 7578, section 4.1
        #   (see the note below).
        self._dash_boundary = b'--' + boundary
        self._parse_options = parse_options

    def __aiter__(self) -> AsyncIterator[BodyPart]:
        return self._iterate_parts()

