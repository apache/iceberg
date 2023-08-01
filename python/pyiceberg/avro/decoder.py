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
from abc import ABC, abstractmethod
from io import SEEK_CUR
from typing import Dict, List

from pyiceberg.avro import STRUCT_DOUBLE, STRUCT_FLOAT
from pyiceberg.io import InputStream


class BinaryDecoder(ABC):
    """Decodes bytes into Python physical primitives."""

    @abstractmethod
    def __init__(self, input_stream: InputStream) -> None:
        """Create the decoder."""

    @abstractmethod
    def tell(self) -> int:
        """Return the current position."""

    @abstractmethod
    def read(self, n: int) -> bytes:
        """Read n bytes."""

    @abstractmethod
    def skip(self, n: int) -> None:
        """Skip n bytes."""

    def read_boolean(self) -> bool:
        """Reads a value from the stream as a boolean.

        A boolean is written as a single byte
        whose value is either 0 (false) or 1 (true).
        """
        return ord(self.read(1)) == 1

    def read_int(self) -> int:
        """Reads an int/long value.

        int/long values are written using variable-length, zigzag coding.
        """
        b = ord(self.read(1))
        n = b & 0x7F
        shift = 7
        while (b & 0x80) != 0:
            b = ord(self.read(1))
            n |= (b & 0x7F) << shift
            shift += 7
        datum = (n >> 1) ^ -(n & 1)
        return datum

    def read_ints(self, n: int, dest: List[int]) -> None:
        """Reads a list of integers."""
        for _ in range(n):
            dest.append(self.read_int())

    def read_int_int_dict(self, n: int, dest: Dict[int, int]) -> None:
        """Reads a dictionary of integers for keys and values into a destination dictionary."""
        for _ in range(n):
            k = self.read_int()
            v = self.read_int()
            dest[k] = v

    def read_int_bytes_dict(self, n: int, dest: Dict[int, bytes]) -> None:
        """Reads a dictionary of integers for keys and bytes for values into a destination dictionary."""
        for _ in range(n):
            k = self.read_int()
            v = self.read_bytes()
            dest[k] = v

    def read_float(self) -> float:
        """Reads a value from the stream as a float.

        A float is written as 4 bytes.
        The float is converted into a 32-bit integer using a method equivalent to
        Java's floatToIntBits and then encoded in little-endian format.
        """
        return float(STRUCT_FLOAT.unpack(self.read(4))[0])

    def read_double(self) -> float:
        """Reads a value from the stream as a double.

        A double is written as 8 bytes.
        The double is converted into a 64-bit integer using a method equivalent to
        Java's doubleToLongBits and then encoded in little-endian format.
        """
        return float(STRUCT_DOUBLE.unpack(self.read(8))[0])

    def read_bytes(self) -> bytes:
        """Bytes are encoded as a long followed by that many bytes of data."""
        num_bytes = self.read_int()
        return self.read(num_bytes) if num_bytes > 0 else b""

    def read_utf8(self) -> str:
        """Reads an utf-8 encoded string from the stream.

        A string is encoded as a long followed by
        that many bytes of UTF-8 encoded character data.
        """
        return self.read_bytes().decode("utf-8")

    def skip_boolean(self) -> None:
        self.skip(1)

    def skip_int(self) -> None:
        b = ord(self.read(1))
        while (b & 0x80) != 0:
            b = ord(self.read(1))

    def skip_float(self) -> None:
        self.skip(4)

    def skip_double(self) -> None:
        self.skip(8)

    def skip_bytes(self) -> None:
        self.skip(self.read_int())

    def skip_utf8(self) -> None:
        self.skip_bytes()


class StreamingBinaryDecoder(BinaryDecoder):
    """Decodes bytes into Python physical primitives."""

    __slots__ = "_input_stream"
    _input_stream: InputStream

    def __init__(self, input_stream: InputStream) -> None:
        """Reader is a Python object on which we can call read, seek, and tell."""
        super().__init__(input_stream)
        self._input_stream = input_stream

    def tell(self) -> int:
        """Return the current stream position."""
        return self._input_stream.tell()

    def read(self, n: int) -> bytes:
        """Read n bytes."""
        if n < 0:
            raise ValueError(f"Requested {n} bytes to read, expected positive integer.")
        data: List[bytes] = []

        n_remaining = n
        while n_remaining > 0:
            data_read = self._input_stream.read(n_remaining)
            read_len = len(data_read)
            if read_len == n:
                # If we read everything, we return directly
                # otherwise we'll continue to fetch the rest
                return data_read
            elif read_len <= 0:
                raise EOFError(f"EOF: read {read_len} bytes")
            data.append(data_read)
            n_remaining -= read_len

        return b"".join(data)

    def skip(self, n: int) -> None:
        self._input_stream.seek(n, SEEK_CUR)


class InMemoryBinaryDecoder(BinaryDecoder):
    """Implement a BinaryDecoder that reads from an in-memory buffer.

    This may be more efficient if the entire block is already in memory
    as it does not need to interact with the I/O subsystem.
    """

    __slots__ = ["_contents", "_position", "_size"]
    _contents: bytes
    _position: int
    _size: int

    def __init__(self, input_stream: InputStream) -> None:
        """Reader is a Python object on which we can call read, seek, and tell."""
        super().__init__(input_stream)
        self._contents = input_stream.read()
        self._size = len(self._contents)
        self._position = 0

    def tell(self) -> int:
        """Return the current stream position."""
        return self._position

    def read(self, n: int) -> bytes:
        """Read n bytes."""
        if n < 0:
            raise ValueError(f"Requested {n} bytes to read, expected positive integer.")
        if self._position + n > self._size:
            raise EOFError(f"EOF: read {n} bytes")
        r = self._contents[self._position : self._position + n]
        self._position += n
        return r

    def skip(self, n: int) -> None:
        self._position += n

    def read_boolean(self) -> bool:
        """Reads a value from the stream as a boolean.

        A boolean is written as a single byte
        whose value is either 0 (false) or 1 (true).
        """
        r = self._contents[self._position]
        self._position += 1
        return r != 0

    def read_int(self) -> int:
        """Reads a value from the stream as an integer.

        int/long values are written using variable-length, zigzag coding.
        """
        if self._position == self._size:
            raise EOFError("EOF: read 1 byte")
        b = self._contents[self._position]
        self._position += 1
        n = b & 0x7F
        shift = 7
        while b & 0x80:
            b = self._contents[self._position]
            self._position += 1
            n |= (b & 0x7F) << shift
            shift += 7
        return (n >> 1) ^ -(n & 1)

    def read_int_bytes_dict(self, n: int, dest: Dict[int, bytes]) -> None:
        """Reads a dictionary of integers for keys and bytes for values into a destination dict."""
        for _ in range(n):
            k = self.read_int()

            byte_length = self.read_int()
            if byte_length <= 0:
                dest[k] = b""
            else:
                dest[k] = self._contents[self._position : self._position + byte_length]
                self._position += byte_length

    def read_bytes(self) -> bytes:
        """Bytes are encoded as a long followed by that many bytes of data."""
        num_bytes = self.read_int()
        if num_bytes <= 0:
            return b""
        r = self._contents[self._position : self._position + num_bytes]
        self._position += num_bytes
        return r

    def skip_int(self) -> None:
        b = self._contents[self._position]
        self._position += 1
        while b & 0x80:
            b = self._contents[self._position]
            self._position += 1
