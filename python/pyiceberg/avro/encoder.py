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
from datetime import date, datetime, time

from pyiceberg.avro import STRUCT_DOUBLE, STRUCT_FLOAT
from pyiceberg.io import OutputStream
from pyiceberg.utils.datetime import date_to_days, datetime_to_micros, time_object_to_micros


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
        sign, digits, _ = datum.as_tuple()

        unscaled_datum = 0
        for digit in digits:
            unscaled_datum = (unscaled_datum * 10) + digit

        bits_req = unscaled_datum.bit_length() + 1
        if sign:
            unscaled_datum = (1 << bits_req) - unscaled_datum

        bytes_req = bits_req // 8
        padding_bits = ~((1 << bits_req) - 1) if sign else 0
        packed_bits = padding_bits | unscaled_datum

        bytes_req += 1 if (bytes_req << 3) < bits_req else 0
        self.write_int(bytes_req)
        for index in range(bytes_req - 1, -1, -1):
            bits_to_write = packed_bits >> (8 * index)
            self.write(bytearray([bits_to_write & 0xFF]))

    def write_decimal_fixed(self, datum: decimal.Decimal, size: int) -> None:
        """Decimal in fixed are encoded as size of fixed bytes."""
        sign, digits, _ = datum.as_tuple()

        unscaled_datum = 0
        for digit in digits:
            unscaled_datum = (unscaled_datum * 10) + digit

        bits_req = unscaled_datum.bit_length() + 1
        size_in_bits = size * 8
        offset_bits = size_in_bits - bits_req

        mask = 2**size_in_bits - 1
        bit = 1
        for _ in range(bits_req):
            mask ^= bit
            bit <<= 1

        if bits_req < 8:
            bytes_req = 1
        else:
            bytes_req = bits_req // 8
            if bits_req % 8 != 0:
                bytes_req += 1
        if sign:
            unscaled_datum = (1 << bits_req) - unscaled_datum
            unscaled_datum = mask | unscaled_datum
            for index in range(size - 1, -1, -1):
                bits_to_write = unscaled_datum >> (8 * index)
                self.write(bytearray([bits_to_write & 0xFF]))
        else:
            for _ in range(offset_bits // 8):
                self.write(b"\x00")
            for index in range(bytes_req - 1, -1, -1):
                bits_to_write = unscaled_datum >> (8 * index)
                self.write(bytearray([bits_to_write & 0xFF]))

    def write_bytes(self, b: bytes) -> None:
        """Bytes are encoded as a long followed by that many bytes of data."""
        self.write_int(len(b))
        self.write(struct.pack(f"{len(b)}s", b))

    def write_bytes_fixed(self, b: bytes) -> None:
        """Writes fixed number of bytes."""
        self.write(struct.pack(f"{len(b)}s", b))

    def write_utf8(self, s: str) -> None:
        """A string is encoded as a long followed by that many bytes of UTF-8 encoded character data."""
        self.write_bytes(s.encode("utf-8"))

    def write_date_int(self, d: date) -> None:
        """
        Encode python date object as int.

        It stores the number of days from the unix epoch, 1 January 1970 (ISO calendar).
        """
        self.write_int(date_to_days(d))

    def write_time_millis_int(self, dt: time) -> None:
        """
        Encode python time object as int.

        It stores the number of milliseconds from midnight, 00:00:00.000
        """
        self.write_int(int(time_object_to_micros(dt) / 1000))

    def write_time_micros_long(self, dt: time) -> None:
        """
        Encode python time object as long.

        It stores the number of microseconds from midnight, 00:00:00.000000
        """
        self.write_int(time_object_to_micros(dt))

    def write_timestamp_millis_long(self, dt: datetime) -> None:
        """
        Encode python datetime object as long.

        It stores the number of milliseconds from midnight of unix epoch, 1 January 1970.
        """
        self.write_int(int(datetime_to_micros(dt) / 1000))

    def write_timestamp_micros_long(self, dt: datetime) -> None:
        """
        Encode python datetime object as long.

        It stores the number of microseconds from midnight of unix epoch, 1 January 1970.
        """
        self.write_int(datetime_to_micros(dt))
