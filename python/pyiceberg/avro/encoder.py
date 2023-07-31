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
import decimal
from datetime import time
from uuid import UUID

from pyiceberg.avro import STRUCT_DOUBLE, STRUCT_FLOAT
from pyiceberg.io import OutputStream
from pyiceberg.utils.datetime import time_to_micros
from pyiceberg.utils.decimal import decimal_to_unscaled


class BinaryEncoder:
    """Write leaf values."""

    _output_stream: OutputStream

    def __init__(self, output_stream: OutputStream) -> None:
        self._output_stream = output_stream

    def write(self, b: bytes) -> None:
        self._output_stream.write(b)

    def write_boolean(self, boolean: bool) -> None:
        """A boolean is written as a single byte whose value is either 0 (false) or 1 (true).

        Args:
            boolean: The boolean to write.
        """
        self.write(bytearray([bool(boolean)]))

    def write_int(self, integer: int) -> None:
        """Integer and long values are written using variable-length zig-zag coding."""
        datum = (integer << 1) ^ (integer >> 63)
        while (datum & ~0x7F) != 0:
            self.write(bytearray([(datum & 0x7F) | 0x80]))
            datum >>= 7
        self.write(bytearray([datum]))

    def write_float(self, f: float) -> None:
        """A float is written as 4 bytes."""
        self.write(STRUCT_FLOAT.pack(f))

    def write_double(self, f: float) -> None:
        """A double is written as 8 bytes."""
        self.write(STRUCT_DOUBLE.pack(f))

    def write_decimal_bytes(self, datum: decimal.Decimal) -> None:
        """
        Decimal in bytes are encoded as long.

        Since size of packed value in bytes for signed long is 8, 8 bytes are written.
        """
        unscaled_datum = decimal_to_unscaled(datum)
        size = (unscaled_datum.bit_length() + 7) // 8
        bytes_datum = unscaled_datum.to_bytes(length=size, byteorder="big", signed=True)
        self.write_bytes(bytes_datum)

    def write_decimal_fixed(self, datum: decimal.Decimal, size: int) -> None:
        """Decimal in fixed are encoded as size of fixed bytes."""
        unscaled_datum = decimal_to_unscaled(datum)
        bytes_datum = unscaled_datum.to_bytes(length=size, byteorder="big", signed=True)
        self.write(bytes_datum)

    def write_bytes(self, b: bytes) -> None:
        """Bytes are encoded as a long followed by that many bytes of data."""
        self.write_int(len(b))
        self.write(b)

    def write_utf8(self, s: str) -> None:
        """A string is encoded as a long followed by that many bytes of UTF-8 encoded character data."""
        self.write_bytes(s.encode("utf-8"))

    def write_uuid(self, uuid: UUID) -> None:
        """Write UUID as a fixed[16].

        The uuid logical type represents a random generated universally unique identifier (UUID).
        An uuid logical type annotates an Avro string. The string has to conform with RFC-4122.
        """
        if len(uuid.bytes) != 16:
            raise ValueError(f"Expected UUID to have 16 bytes, got: len({uuid.bytes!r})")
        return self.write(uuid.bytes)

    def write_time_micros(self, dt: time) -> None:
        """
        Encode python time object as long.

        It stores the number of microseconds from midnight, 00:00:00.000000
        """
        self.write_int(time_to_micros(dt))
