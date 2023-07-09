# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from io import SEEK_CUR, SEEK_END, SEEK_SET
from types import TracebackType
from typing import Optional, Type

from pyiceberg.io import InputStream


class MemoryInputStream(InputStream):
    """
    Simple in memory stream that we use to store decompressed blocks.

    Examples:
        >>> stream = MemoryInputStream(b'22memory1925')
        >>> stream.tell()
        0
        >>> stream.read(2)
        b'22'
        >>> stream.tell()
        2
        >>> stream.seek(8)
        >>> stream.read(4)
        b'1925'
        >>> stream.close()
    """

    buffer: bytes
    len: int
    pos: int

    def __init__(self, buffer: bytes):
        self.buffer = buffer
        self.len = len(buffer)
        self.pos = 0

    def read(self, size: int = 0) -> bytes:
        b = self.buffer[self.pos : self.pos + size]
        self.pos += size
        return b

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        if whence == SEEK_SET:
            self.pos = offset
        elif whence == SEEK_CUR:
            self.pos += offset
        elif whence == SEEK_END:
            self.pos = self.len + offset
        else:
            raise ValueError(f"Unknown whence {offset}")

        return self.pos

    def tell(self) -> int:
        return self.pos

    def close(self) -> None:
        del self.buffer
        self.pos = 0

    def __enter__(self) -> MemoryInputStream:
        """Provides setup when opening a MemoryInputStream using a 'with' statement."""
        return self

    def __exit__(
        self, exctype: Optional[Type[BaseException]], excinst: Optional[BaseException], exctb: Optional[TracebackType]
    ) -> None:
        """Performs cleanup when exiting the scope of a 'with' statement."""
        self.close()
