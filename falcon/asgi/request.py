# Copyright 2019-2020 by Kurt Griffiths
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

"""ASGI Request class."""

from __future__ import annotations

from collections.abc import Awaitable
from collections.abc import Mapping
from typing import (
    Any,
    cast,
    Literal,
    NoReturn,
    overload,
)

from falcon import errors
from falcon import request
from falcon import request_helpers as helpers
from falcon._typing import _UNSET
from falcon._typing import AsgiReceive
from falcon._typing import StoreArg
from falcon._typing import UnsetOr
from falcon.asgi_spec import AsgiEvent
from falcon.constants import SINGLETON_HEADERS
from falcon.forwarded import Forwarded
from falcon.util import deprecation
from falcon.util import ETag
from falcon.util.uri import parse_host
from falcon.util.uri import parse_query_string

from . import _request_helpers as asgi_helpers
from .stream import BoundedStream

__all__ = ('Request',)

_SINGLETON_HEADERS_BYTESTR = frozenset([h.encode() for h in SINGLETON_HEADERS])


class Request(request.Request):
    """Represents a client's HTTP request.

    Note:
        `Request` is not meant to be instantiated directly by responders.

    Args:
        scope (dict): ASGI HTTP connection scope passed in from the server (see
            also: `Connection Scope`_).
        receive (awaitable): ASGI awaitable callable that will yield a new
            event dictionary when one is available.

    Keyword Args:
        first_event (dict): First ASGI event received from the client,
            if one was preloaded (default ``None``).
        options (falcon.request.RequestOptions): Set of global request options
            passed from the App handler.

    .. _Connection Scope:
        https://asgi.readthedocs.io/en/latest/specs/www.html#connection-scope

    """

    __slots__ = [
        '_asgi_headers',
        # '_asgi_server_cached',
        # '_cached_headers',
        '_first_event',
        '_receive',
        # '_stream',
        'scope',
    ]

    # PERF(vytas): These boilerplates values will be shadowed when set on an
    #   instance. Avoiding a statement per each of those values allows to speed
    #   up __init__ substantially.
    _asgi_server_cached: tuple[str, int] | None = None
    _cached_access_route: list[str] | None = None
    _cached_forwarded: list[Forwarded] | None = None
    _cached_forwarded_prefix: str | None = None
    _cached_forwarded_uri: str | None = None
    _cached_headers: dict[str, str] | None = None
    # NOTE: _cached_headers_lower is not used
    _cached_prefix: str | None = None
    _cached_relative_uri: str | None = None
    _cached_uri: str | None = None
    _media: UnsetOr[Any] = _UNSET
    _media_error: Exception | None = None
    _stream: BoundedStream | None = None

    scope: dict[str, Any]
    """Reference to the ASGI HTTP connection scope passed in
    from the server (see also: `Connection Scope`_).

    .. _Connection Scope:
        https://asgi.readthedocs.io/en/latest/specs/www.html#connection-scope
    """
    is_websocket: bool
    """Set to ``True`` IFF this request was made as part of a WebSocket handshake."""

    def __init__(
        self,
        scope: dict[str, Any],
        receive: AsgiReceive,
        first_event: AsgiEvent | None = None,
        options: request.RequestOptions | None = None,
    ):
        # =====================================================================
        # Prepare headers
        # =====================================================================

        req_headers: dict[bytes, bytes] = {}
        for header_name, header_value in scope['headers']:
            # NOTE(kgriffs): According to ASGI 3.0, header names are always
            #   lowercased, and both name and value are byte strings. Although
            #   technically header names and values are restricted to US-ASCII
            #   we decode later (just-in-time) using the default 'utf-8' because
            #   it is a little faster than passing an encoding option (except
            #   under Cython).
            #
            #   The reason we wait to decode is that the typical app will not
            #   need to decode all request headers, and we usually can just
            #   leave the header name as a byte string and look it up that way.
            #

            # NOTE(kgriffs): There are no standard request headers that
            #   allow multiple instances to appear in the request while also
            #   disallowing list syntax.
            if (
                header_name not in req_headers
                or header_name in _SINGLETON_HEADERS_BYTESTR
            ):
                req_headers[header_name] = header_value
            else:
                req_headers[header_name] += b',' + header_value

        self._asgi_headers: dict[bytes, bytes] = req_headers
        # PERF(vytas): Fall back to class variable(s) when unset.
        # self._cached_headers = None

        # =====================================================================
        #  Misc.
        # =====================================================================

        # PERF(vytas): Fall back to class variable(s) when unset.
        # self._asgi_server_cached = None  # Lazy
        self.scope = scope
        self.is_websocket = scope['type'] == 'websocket'

        self.options = options if options is not None else request.RequestOptions()

        self.method = 'GET' if self.is_websocket else scope['method']

        self.uri_template = None
        # PERF(vytas): Fall back to class variable(s) when unset.
        # self._media = _UNSET
        # self._media_error = None

        # TODO(kgriffs): ASGI does not specify whether 'path' may be empty,
        #   as was allowed for WSGI.
        path = scope['path'] or '/'

        if (
            self.options.strip_url_path_trailing_slash
            and len(path) != 1
            and path.endswith('/')
        ):
            self.path = path[:-1]
        else:
            self.path = path

        query_string = scope['query_string'].decode()
        self.query_string = query_string
        if query_string:
            self._params = parse_query_string(
                query_string,
                keep_blank=self.options.keep_blank_qs_values,
                csv=self.options.auto_parse_qs_csv,
            )

        else:
            self._params = {}

        # PERF(vytas): Fall back to class variable(s) when unset.
        # self._cached_access_route = None
        # self._cached_forwarded = None
        # self._cached_forwarded_prefix = None
        # self._cached_forwarded_uri = None
        # self._cached_prefix = None
        # self._cached_relative_uri = None
        # self._cached_uri = None

        if self.method == 'GET':
            # NOTE(vytas): We do not really expect the Content-Type to be
            #   non-ASCII, however we assume ISO-8859-1 here for maximum
            #   compatibility with WSGI.

            # PERF(kgriffs): Normally we expect no Content-Type header, so
            #   use this pattern which is a little bit faster than dict.get()
            if b'content-type' in req_headers:
                self.content_type = req_headers[b'content-type'].decode('latin1')
            else:
                self.content_type = None
        else:
            # PERF(kgriffs): This is the most performant pattern when we expect
            #   the key to be present most of the time.
            try:
                self.content_type = req_headers[b'content-type'].decode('latin1')
            except KeyError:
                self.content_type = None

        # =====================================================================
        # The request body stream is created lazily
        # =====================================================================

        # NOTE(kgriffs): The ASGI spec states that "you should not trigger
        #   on a connection opening alone". I take this to mean that the app
        #   should have the opportunity to respond with a 401, for example,
        #   without having to first read any of the body. This is accomplished
        #   in Falcon by only reading the first data event when the app attempts
        #   to read from req.stream for the first time, and in uvicorn
        #   (for example) by not confirming a 100 Continue request unless
        #   the app calls receive() to read the request body.

        # PERF(vytas): Fall back to class variable(s) when unset.
        # self._stream = None
        self._receive: AsgiReceive = receive
        self._first_event: AsgiEvent | None = first_event

        # =====================================================================
        # Create a context object
        # =====================================================================

        self.context = self.context_type()

    # ------------------------------------------------------------------------
    # Properties
    #
    # Much of the logic from the ASGI Request class is duplicated in these
    # property implementations; however, to make the code more DRY we would
    # have to factor out the common logic, which would add overhead to these
    # properties and slow them down. They are simple enough that we should
    # be able to keep them in sync with the WSGI side without too much
    # trouble.
    # ------------------------------------------------------------------------

    # NOTE(vytas): Duplicating docstrings from the WSGI flavor here, otherwise
    #   Sphinx doesn't seem to pick them up.
    auth: str | None = asgi_helpers._header_property('Authorization')
    """Value of the Authorization header, or ``None`` if the header is missing."""
    expect: str | None = asgi_helpers._header_property('Expect')
    """Value of the Expect header, or ``None`` if the header is missing."""
    if_range: str | None = asgi_helpers._header_property('If-Range')
    """Value of the If-Range header, or ``None`` if the header is missing."""
    last_event_id: str | None = asgi_helpers._header_property('Last-Event-ID')
    """Value of the Last-Event-ID header, or ``None`` if the header is missing."""
    referer: str | None = asgi_helpers._header_property('Referer')
    """Value of the Referer header, or ``None`` if the header is missing."""
    user_agent: str | None = asgi_helpers._header_property('User-Agent')
    """Value of the User-Agent header, or ``None`` if the header is missing."""

    @property
    def accept(self) -> str:
        # NOTE(kgriffs): Per RFC, a missing accept header is
        # equivalent to '*/*'
        try:
            return self._asgi_headers[b'accept'].decode('latin1') or '*/*'
        except KeyError:
            return '*/*'


    @property
    def stream(self) -> BoundedStream:  # type: ignore[override]
        """File-like input object for reading the body of the request, if any."""
        pass

    # NOTE(kgriffs): This is provided as an alias in order to ease migration
    #   from WSGI, but is not documented since we do not want people using
    #   it in greenfield ASGI apps.
    @property
    def bounded_stream(self) -> BoundedStream:  # type: ignore[override]
        """Alias to :attr:`~.stream`."""
        pass


    @property
    # NOTE(caselit): Deprecated long ago. Warns since 4.0.
    @deprecation.deprecated(
        'Use `root_path` instead. '
        '(This compatibility alias will be removed in Falcon 5.0.)',
        is_property=True,
    )
    def app(self) -> str:
        """Deprecated alias for :attr:`root_path`."""
        return self.root_path

    @property
    def scheme(self) -> str:
        """URL scheme used for the request.

        One of ``'http'``, ``'https'``, ``'ws'``, or ``'wss'``. Defaults to ``'http'``
        for the ``http`` scope, or ``'ws'`` for the ``websocket`` scope, when
        the ASGI server does not include the scheme in the connection scope.

        Note:
            If the request was proxied, the scheme may not
            match what was originally requested by the client.
            :attr:`forwarded_scheme` can be used, instead,
            to handle such cases.
        """
        pass


    @property
    def host(self) -> str:
        """Host request header field, if present.

        If the Host header is missing, this attribute resolves to the ASGI server's
        listening host name or IP address.
        """
        pass


    @property
    def access_route(self) -> list[str]:
        """IP address of the original client (if known), as
        well as any known addresses of proxies fronting the ASGI server.

        The following request headers are checked, in order of
        preference, to determine the addresses:

            - ``Forwarded``
            - ``X-Forwarded-For``
            - ``X-Real-IP``

        In addition, the value of the "client" field from the ASGI
        connection scope will be appended to the end of the list if
        not already included in one of the above headers. If the
        "client" field is not available, it will default to
        ``'127.0.0.1'``.

        Note:
            Per `RFC 7239`_, the access route may contain "unknown"
            and obfuscated identifiers, in addition to IPv4 and
            IPv6 addresses

            .. _RFC 7239: https://tools.ietf.org/html/rfc7239

        Warning:
            Headers can be forged by any client or proxy. Use this
            property with caution and validate all values before
            using them. Do not rely on the access route to authorize
            requests!
        """  # noqa: D205
        pass

    @property
    def remote_addr(self) -> str:
        """IP address of the closest known client or proxy to
        the ASGI server, or ``'127.0.0.1'`` if unknown.

        This property's value is equivalent to the last element of the
        :attr:`~.access_route` property.
        """  # noqa: D205
        pass



    async def get_media(self, default_when_empty: UnsetOr[Any] = _UNSET) -> Any:
        """Return a deserialized form of the request stream.

        The first time this method is called, the request stream will be
        deserialized using the Content-Type header as well as the media-type
        handlers configured via :class:`falcon.RequestOptions`. The result will
        be cached and returned in subsequent calls::

            deserialized_media = await req.get_media()

        If the matched media handler raises an error while attempting to
        deserialize the request body, the exception will propagate up
        to the caller.

        See also :ref:`media` for more information regarding media handling.

        Note:
            When ``get_media`` is called on a request with an empty body,
            Falcon will let the media handler try to deserialize the body
            and will return the value returned by the handler or propagate
            the exception raised by it. To instead return a different value
            in case of an exception by the handler, specify the argument
            ``default_when_empty``.

        Warning:
            This operation will consume the request stream the first time
            it's called and cache the results. Follow-up calls will just
            retrieve a cached version of the object.

        Args:
            default_when_empty: Fallback value to return when there is no body
                in the request and the media handler raises an error
                (like in the case of the default JSON media handler).
                By default, Falcon uses the value returned by the media handler
                or propagates the raised exception, if any.
                This value is not cached, and will be used only for the current
                call.

        Returns:
            media (object): The deserialized media representation.
        """

        if self._media is not _UNSET:
            return self._media
        if self._media_error is not None:
            if default_when_empty is not _UNSET and isinstance(
                self._media_error, errors.MediaNotFoundError
            ):
                return default_when_empty
            raise self._media_error

        handler, _, deserialize_sync = self.options.media_handlers._resolve(
            self.content_type, self.options.default_media_type
        )

        try:
            if deserialize_sync:
                self._media = deserialize_sync(await self.stream.read())
            else:
                self._media = await handler.deserialize_async(
                    self.stream, self.content_type, self.content_length
                )

        except errors.MediaNotFoundError as err:
            self._media_error = err
            if default_when_empty is not _UNSET:
                return default_when_empty
            raise
        except Exception as err:
            self._media_error = err
            raise
        finally:
            if handler.exhaust_stream:
                await self.stream.exhaust()

        return self._media

    media: Awaitable[Any] = cast(Awaitable[Any], property(get_media))
    """An awaitable property that acts as an alias for
    :meth:`~.get_media`. This can be used to ease the porting of
    a WSGI app to ASGI, although the ``await`` keyword must still be
    added when referencing the property::

        deserialized_media = await req.media
    """



    @property
    def headers(self) -> Mapping[str, str]:
        """Raw HTTP headers from the request with dash-separated
        names normalized to lowercase.

        Note:
            This property differs from the WSGI version of ``Request.headers``
            in that the latter returns *uppercase* names for historical
            reasons. Middleware, such as tracing and logging components, that
            need to be compatible with both WSGI and ASGI apps should
            use :attr:`headers_lower` instead.

        Warning:
            Parsing all the headers to create this dict is done the first
            time this attribute is accessed, and the returned object should
            be treated as read-only. Note that this parsing can be costly,
            so unless you need all the headers in this format, you should
            instead use the ``get_header()`` method or one of the
            convenience attributes to get a value for a specific header.
        """  # noqa: D205
        pass

    @property
    def headers_lower(self) -> Mapping[str, str]:
        """Alias for :attr:`headers` provided to expose a uniform way to
        get lowercased headers for both WSGI and ASGI apps.
        """  # noqa: D205
        pass

    # ------------------------------------------------------------------------
    # Public Methods
    # ------------------------------------------------------------------------

    @overload
    def get_header(
        self, name: str, required: Literal[True], default: str | None = ...
    ) -> str: ...

    @overload
    def get_header(self, name: str, required: bool = ..., *, default: str) -> str: ...

    @overload
    def get_header(
        self, name: str, required: bool = False, default: str | None = ...
    ) -> str | None: ...

    # PERF(kgriffs): Using kwarg cache, in lieu of @lru_cache on a helper method
    #   that is then called from get_header(), was benchmarked to be more
    #   efficient across CPython 3.6/3.8 (regardless of cythonization) and
    #   PyPy 3.6.
    # TODO(vytas): Verify whether this is still the case on 3.12+.
    def get_header(
        self,
        name: str,
        required: bool = False,
        default: str | None = None,
        _name_cache: dict[str, bytes] = {},
    ) -> str | None:
        """Retrieve the raw string value for the given header.

        Args:
            name (str): Header name, case-insensitive (e.g., 'Content-Type')

        Keyword Args:
            required (bool): Set to ``True`` to raise
                ``HTTPBadRequest`` instead of returning gracefully when the
                header is not found (default ``False``).
            default (any): Value to return if the header
                is not found (default ``None``).

        Returns:
            str: The value of the specified header if it exists, or
            the default value if the header is not found and is not
            required.

        Raises:
            HTTPBadRequest: The header was not found in the request, but
                it was required.

        """

        try:
            asgi_name = _name_cache[name]
        except KeyError:
            asgi_name = name.lower().encode('latin1')
            if len(_name_cache) < 64:  # Somewhat arbitrary ceiling to mitigate abuse
                _name_cache[name] = asgi_name

        # Use try..except to optimize for the header existing in most cases
        try:
            # Don't take the time to cache beforehand, using HTTP naming.
            # This will be faster, assuming that most headers are looked
            # up only once, and not all headers will be requested.
            return self._asgi_headers[asgi_name].decode('latin1')

        except KeyError:
            if not required:
                return default

            raise errors.HTTPMissingHeader(name)

    @overload
    def get_param(
        self,
        name: str,
        required: Literal[True],
        store: StoreArg = ...,
        default: str | None = ...,
    ) -> str: ...

    @overload
    def get_param(
        self,
        name: str,
        required: bool = ...,
        store: StoreArg = ...,
        *,
        default: str,
    ) -> str: ...

    @overload
    def get_param(
        self,
        name: str,
        required: bool = False,
        store: StoreArg = None,
        default: str | None = None,
    ) -> str | None: ...

    def get_param(
        self,
        name: str,
        required: bool = False,
        store: StoreArg = None,
        default: str | None = None,
    ) -> str | None:
        """Return the raw value of a query string parameter as a string.

        Note:
            If an HTML form is POSTed to the API using the
            *application/x-www-form-urlencoded* media type, Falcon can
            automatically parse the parameters from the request body via
            :meth:`~falcon.asgi.Request.get_media`.

            See also: :ref:`access_urlencoded_form`

        Note:
            Similar to the way multiple keys in form data are handled, if a
            query parameter is included in the query string multiple times,
            only one of those values will be returned, and it is undefined which
            one. This caveat also applies when
            :attr:`~falcon.RequestOptions.auto_parse_qs_csv` is enabled and the
            given parameter is assigned to a comma-separated list of values
            (e.g., ``foo=a,b,c``).

            When multiple values are expected for a parameter,
            :meth:`~.get_param_as_list` can be used to retrieve all of
            them at once.

        Args:
            name (str): Parameter name, case-sensitive (e.g., 'sort').

        Keyword Args:
            required (bool): Set to ``True`` to raise
                ``HTTPBadRequest`` instead of returning ``None`` when the
                parameter is not found (default ``False``).
            store (dict): A ``dict``-like object in which to place
                the value of the param, but only if the param is present.
            default (any): If the param is not found returns the
                given value instead of ``None``

        Returns:
            str: The value of the param as a string, or ``None`` if param is
            not found and is not required.

        Raises:
            HTTPBadRequest: A required param is missing from the request.
        """
        pass

    @property
    def env(self) -> NoReturn:  # type:ignore[override]
        """The env property is not available in ASGI. Use :attr:`~.scope` instead."""
        raise AttributeError(
            'The env property is not available in ASGI. Use req.scope instead.'
        )

    def log_error(self, message: str) -> NoReturn:
        """Write a message to the server's log.

        Warning:
            Although this method is inherited from the WSGI Request class, it is
            not supported for ASGI apps. Please use the standard library logging
            framework instead.
        """

        # NOTE(kgriffs): Normally the Pythonic thing to do would be to simply
        #   set this method to None so that it can't even be called, but we
        #   raise an error here to help people who are porting from WSGI.
        raise NotImplementedError(
            "ASGI does not support writing to the server's log. "
            'Please use the standard library logging framework '
            'instead.'
        )

    # ------------------------------------------------------------------------
    # Private Helpers
    # ------------------------------------------------------------------------


