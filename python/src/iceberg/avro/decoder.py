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
import struct
from datetime import (
    date,
    datetime,
    time,
    timedelta,
    timezone,
)

from iceberg.io.base import InputStream
from iceberg.utils.datetime import micros_to_time, micros_to_timestamp
from iceberg.utils.decimal import unscaled_to_decimal

STRUCT_FLOAT = struct.Struct("<f")  # little-endian float
STRUCT_DOUBLE = struct.Struct("<d")  # little-endian double
STRUCT_SIGNED_SHORT = struct.Struct(">h")  # big-endian signed short
STRUCT_SIGNED_INT = struct.Struct(">i")  # big-endian signed int
STRUCT_SIGNED_LONG = struct.Struct(">q")  # big-endian signed long


class BinaryDecoder:
    """Read leaf values."""

    _input_stream: InputStream

    def __init__(self, input_stream: InputStream) -> None:
        """
        reader is a Python object on which we can call read, seek, and tell.
        """
        self._input_stream = input_stream

    def read(self, n: int) -> bytes:
        """
        Read n bytes.
        """
        if n < 0:
            raise ValueError(f"Requested {n} bytes to read, expected positive integer.")
        read_bytes = self._input_stream.read(n)
        if len(read_bytes) != n:
            raise ValueError(f"Read {len(read_bytes)} bytes, expected {n} bytes")
        return read_bytes

    def read_boolean(self) -> bool:
        """
        a boolean is written as a single byte
        whose value is either 0 (false) or 1 (true).
        """
        return ord(self.read(1)) == 1

    def read_int(self) -> int:
        """int values are written using variable-length, zigzag coding."""
        return self.read_long()

    def read_long(self) -> int:
        """long values are written using variable-length, zigzag coding."""
        b = ord(self.read(1))
        n = b & 0x7F
        shift = 7
        while (b & 0x80) != 0:
            b = ord(self.read(1))
            n |= (b & 0x7F) << shift
            shift += 7
        datum = (n >> 1) ^ -(n & 1)
        return datum

    def read_float(self) -> float:
        """
        A float is written as 4 bytes.
        The float is converted into a 32-bit integer using a method equivalent to
        Java's floatToIntBits and then encoded in little-endian format.
        """
        return float(STRUCT_FLOAT.unpack(self.read(4))[0])

    def read_double(self) -> float:
        """
        A double is written as 8 bytes.
        The double is converted into a 64-bit integer using a method equivalent to
        Java's doubleToLongBits and then encoded in little-endian format.
        """
        return float(STRUCT_DOUBLE.unpack(self.read(8))[0])

    def read_decimal_from_bytes(self, precision: int, scale: int) -> decimal.Decimal:
        """
        Decimal bytes are decoded as signed short, int or long depending on the
        size of bytes.
        """
        size = self.read_long()
        return self.read_decimal_from_fixed(precision, scale, size)

    def read_decimal_from_fixed(self, precision: int, scale: int, size: int) -> decimal.Decimal:
        """
        Decimal is encoded as fixed. Fixed instances are encoded using the
        number of bytes declared in the schema.
        """
        data = self.read(size)
        unscaled_datum = int.from_bytes(data, byteorder="big", signed=True)
        return unscaled_to_decimal(unscaled_datum, scale)

    def read_bytes(self) -> bytes:
        """
        Bytes are encoded as a long followed by that many bytes of data.
        """
        return self.read(self.read_long())

    def read_utf8(self) -> str:
        """
        A string is encoded as a long followed by
        that many bytes of UTF-8 encoded character data.
        """
        return self.read_bytes().decode("utf-8")

    def read_date_from_int(self) -> date:
        """
        int is decoded as python date object.
        int stores the number of days from
        the unix epoch, 1 January 1970 (ISO calendar).
        """
        days_since_epoch = self.read_int()
        return date(1970, 1, 1) + timedelta(days_since_epoch)

    def read_time_millis_from_int(self) -> time:
        """
        int is decoded as python time object which represents
        the number of milliseconds after midnight, 00:00:00.000.
        """
        millis = self.read_int()
        return micros_to_time(millis * 1000)

    def read_time_micros_from_long(self) -> time:
        """
        long is decoded as python time object which represents
        the number of microseconds after midnight, 00:00:00.000000.
        """
        return micros_to_time(self.read_long())

    def read_timestamp_micros_from_long(self) -> datetime:
        """
        long is decoded as python datetime object which represents
        the number of microseconds from the unix epoch, 1 January 1970.
        """
        return micros_to_timestamp(self.read_long())

    def read_timestamptz_micros_from_long(self):
        """
        long is decoded as python datetime object which represents
        the number of microseconds from the unix epoch, 1 January 1970.

        Adjusted to UTC
        """
        return micros_to_timestamp(self.read_long(), timezone.utc)
