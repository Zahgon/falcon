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

"""Buffered stream reader."""

from __future__ import annotations

import functools
import io
from typing import Callable, IO

from falcon.errors import DelimiterError

DEFAULT_CHUNK_SIZE = 32768
"""Default chunk size for :class:`BufferedReader` (32 KiB)."""

_MAX_JOIN_CHUNKS = 128


class BufferedReader:
    def __init__(
        self,
        read: Callable[[int], bytes],
        max_stream_len: int,
        chunk_size: int | None = None,
    ):
        self._read_func = read
        self._chunk_size = chunk_size or DEFAULT_CHUNK_SIZE
        self._max_join_size = self._chunk_size * _MAX_JOIN_CHUNKS

        self._buffer = b''
        self._buffer_len = 0
        self._buffer_pos = 0
        self._max_bytes_remaining = max_stream_len

    def _perform_read(self, size: int) -> bytes:
        # PERF(vytas): In Cython, bind types:
        #   cdef bytes chunk
        #   cdef Py_ssize_t chunk_len
        #   cdef result

        size = min(size, self._max_bytes_remaining)
        if size <= 0:
            return b''

        chunk = self._read_func(size)
        chunk_len = len(chunk)
        self._max_bytes_remaining -= chunk_len
        if chunk_len == size:
            return chunk

        if chunk_len == 0:
            # NOTE(vytas): The EOF.
            self._max_bytes_remaining = 0
            return b''

        result = io.BytesIO(chunk)
        result.seek(chunk_len)

        while True:
            size -= chunk_len
            if size <= 0:
                return result.getvalue()

            chunk = self._read_func(size)
            chunk_len = len(chunk)
            if chunk_len == 0:
                # NOTE(vytas): The EOF.
                self._max_bytes_remaining = 0
                return result.getvalue()

            self._max_bytes_remaining -= chunk_len
            result.write(chunk)



    def _normalize_size(self, size: int | None) -> int:
        # PERF(vytas): In Cython, bind types:
        #   cdef Py_ssize_t result
        #   cdef Py_ssize_t max_size

        max_size = self._max_bytes_remaining + self._buffer_len - self._buffer_pos

        if size is None or size == -1 or size > max_size:
            return max_size
        return size

    def read(self, size: int | None = -1) -> bytes:
        return self._read(self._normalize_size(size))

    def _read(self, size: int) -> bytes:
        # PERF(vytas): In Cython, bind types:
        #   cdef Py_ssize_t read_size
        #   cdef bytes result

        # NOTE(vytas): Dish directly from the buffer, if possible.
        if size <= self._buffer_len - self._buffer_pos:
            if size == self._buffer_len and self._buffer_pos == 0:
                result = self._buffer
                self._buffer_len = 0
                self._buffer = b''
                return result

            self._buffer_pos += size
            return self._buffer[self._buffer_pos - size : self._buffer_pos]

        # NOTE(vytas): Pass through large reads.
        if self._buffer_len == 0 and size >= self._chunk_size:
            return self._perform_read(size)

        # NOTE(vytas): if size > self._buffer_len - self._buffer_pos
        read_size = size - (self._buffer_len - self._buffer_pos)
        result = self._buffer[self._buffer_pos :]

        if read_size >= self._chunk_size:
            self._buffer_len = 0
            self._buffer_pos = 0
            self._buffer = b''
            return result + self._perform_read(read_size)

        self._buffer = self._perform_read(self._chunk_size)
        self._buffer_len = len(self._buffer)
        self._buffer_pos = read_size
        return result + self._buffer[:read_size]




    def pipe(self, destination: IO[bytes] | None = None) -> None:
        while True:
            chunk = self.read(self._chunk_size)
            if not chunk:
                break

            if destination is not None:
                destination.write(chunk)


    def exhaust(self) -> None:
        self.pipe()




    # --- implementing IOBase methods, the duck-typing way ---

    def readable(self) -> bool:
        """Return ``True`` always."""
        pass

    def seekable(self) -> bool:
        """Return ``False`` always."""
        pass

    def writeable(self) -> bool:
        """Return ``False`` always."""
        pass
