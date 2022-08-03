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

from decimal import Decimal
import unittest
import uuid

from iceberg.api.expressions import Literal
from iceberg.api.types import (BinaryType,
                               BooleanType,
                               DateType,
                               DoubleType,
                               FixedType,
                               FloatType,
                               IntegerType,
                               LongType,
                               StringType,
                               TimestampType,
                               TimeType,
                               UUIDType)
from iceberg.api.types.conversions import Conversions
from iceberg.api.types.types import DecimalType


class TestConversions(unittest.TestCase):

    def test_from_bytes(self):
        self.assertEqual(False, Conversions.from_byte_buffer(BooleanType.get(), b'\x00'))
        self.assertEqual(True, Conversions.from_byte_buffer(BooleanType.get(), b'\x01'))
        self.assertEqual(1234, Conversions.from_byte_buffer(IntegerType.get(),
                                                            b'\xd2\x04\x00\x00'))
        self.assertEqual(1234, Conversions.from_byte_buffer(LongType.get(),
                                                            b'\xd2\x04\x00\x00\x00\x00\x00\x00'))
        self.assertAlmostEqual(1.2345, Conversions.from_byte_buffer(FloatType.get(),
                                                                    b'\x19\x04\x9e?'), places=5)
        self.assertAlmostEqual(1.2345, Conversions.from_byte_buffer(DoubleType.get(),
                                                                    b'\x8d\x97\x6e\x12\x83\xc0\xf3\x3f'))
        self.assertEqual(1234, Conversions.from_byte_buffer(DateType.get(),
                                                            b'\xd2\x04\x00\x00'))
        self.assertEqual(100000000000, Conversions.from_byte_buffer(TimeType.get(),
                                                                    b'\x00\xe8vH\x17\x00\x00\x00'))
        self.assertEqual(100000000000, Conversions.from_byte_buffer(TimestampType.with_timezone(),
                                                                    b'\x00\xe8vH\x17\x00\x00\x00'))
        self.assertEqual(100000000000, Conversions.from_byte_buffer(TimestampType.without_timezone(),
                                                                    b'\x00\xe8vH\x17\x00\x00\x00'))
        self.assertEqual("foo", Conversions.from_byte_buffer(StringType.get(), b'foo'))
        self.assertEqual(uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"),
                         Conversions.from_byte_buffer(UUIDType.get(), b'\xf7\x9c>\tg|K\xbd\xa4y?4\x9c\xb7\x85\xe7'))
        self.assertEqual(b'foo', Conversions.from_byte_buffer(FixedType.of_length(3), b'foo'))
        self.assertEqual(b'foo', Conversions.from_byte_buffer(BinaryType.get(), b'foo'))
        self.assertEqual(Decimal(123.45).quantize(Decimal(".01")),
                         Conversions.from_byte_buffer(DecimalType.of(5, 2), b'\x30\x39'))
        self.assertEqual(Decimal(123.4567).quantize(Decimal(".0001")),
                         Conversions.from_byte_buffer(DecimalType.of(5, 4), b'\x00\x12\xd6\x87'))
        self.assertEqual(Decimal(-123.4567).quantize(Decimal(".0001")),
                         Conversions.from_byte_buffer(DecimalType.of(5, 4), b'\xff\xed\x29\x79'))

    def test_to_bytes(self):
        self.assertEqual(b'\x00', Literal.of(False).to_byte_buffer())
        self.assertEqual(b'\x01', Literal.of(True).to_byte_buffer())
        self.assertEqual(b'\xd2\x04\x00\x00', Literal.of(1234).to_byte_buffer())
        self.assertEqual(b'\xd2\x04\x00\x00\x00\x00\x00\x00', Literal.of(1234).to(LongType.get()).to_byte_buffer())
        self.assertEqual(b'\x19\x04\x9e?', Literal.of(1.2345).to_byte_buffer())
        self.assertEqual(b'\x8d\x97\x6e\x12\x83\xc0\xf3\x3f', Literal.of(1.2345).to(DoubleType.get()).to_byte_buffer())
        self.assertEqual(b'\xd2\x04\x00\x00', Literal.of(1234).to(DateType.get()).to_byte_buffer())
        self.assertEqual(b'\x00\xe8vH\x17\x00\x00\x00', Literal.of(100000000000).to(TimeType.get()).to_byte_buffer())
        self.assertEqual(b'\x00\xe8vH\x17\x00\x00\x00',
                         Literal.of(100000000000).to(TimestampType.with_timezone()).to_byte_buffer())
        self.assertEqual(b'\x00\xe8vH\x17\x00\x00\x00',
                         Literal.of(100000000000).to(TimestampType.without_timezone()).to_byte_buffer())
        self.assertEqual(b'foo', Literal.of("foo").to_byte_buffer())
        self.assertEqual(b'\xf7\x9c>\tg|K\xbd\xa4y?4\x9c\xb7\x85\xe7',
                         Literal.of(uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7")).to_byte_buffer())
        self.assertEqual(b'foo', Literal.of(bytes(b'foo')).to_byte_buffer())
        self.assertEqual(b'foo', Literal.of(bytearray(b'foo')).to_byte_buffer())
        # Decimal on 2-bytes
        self.assertEqual(b'\x30\x39', Literal.of(123.45).to(DecimalType.of(5, 2)).to_byte_buffer())
        # Decimal on 3-bytes to test that we use the minimum number of bytes
        self.assertEqual(b'\x12\xd6\x87', Literal.of(123.4567).to(DecimalType.of(7, 4)).to_byte_buffer())
        # Negative decimal to test two's complement
        self.assertEqual(b'\xed\x29\x79', Literal.of(-123.4567).to(DecimalType.of(7, 4)).to_byte_buffer())

    def test_byte_buffer_conversions(self):
        # booleans are stored as 0x00 for 'false' and a non-zero byte for 'true'
        self.assertConversion(False, BooleanType.get(), b'\x00')
        self.assertConversion(True, BooleanType.get(), b'\x01')
        self.assertEqual(b'\x00', Literal.of(False).to_byte_buffer())
        self.assertEqual(b'\x01', Literal.of(True).to_byte_buffer())

        # integers are stored as 4 bytes in little-endian order
        # 84202 is 0...01|01001000|11101010 in binary
        # 11101010 -> 234 (-22), 01001000 -> 72, 00000001 -> 1, 00000000 -> 0
        self.assertConversion(84202, IntegerType.get(), bytes([234, 72, 1, 0]))
        self.assertEqual(bytes([234, 72, 1, 0]), Literal.of(84202).to_byte_buffer())

        # longs are stored as 8 bytes in little-endian order
        # 200L is 0...0|11001000 in binary
        # 11001000 -> 200 (-56), 00000000 -> 0, ... , 00000000 -> 0
        self.assertConversion(200, LongType.get(), bytes([200, 0, 0, 0, 0, 0, 0, 0]))
        self.assertEqual(bytes([200, 0, 0, 0, 0, 0, 0, 0]), Literal.of(200).to(LongType.get()).to_byte_buffer())

        # floats are stored as 4 bytes in little-endian order
        # floating point numbers are represented as sign * 2ˆexponent * mantissa
        # -4.5F is -1 * 2ˆ2 * 1.125 and encoded as 11000000|10010000|0...0 in binary
        # 00000000 -> 0, 00000000 -> 0, 10010000 -> 144 (-112), 11000000 -> 192 (-64),
        self.assertConversion(-4.5, FloatType.get(), bytes([0, 0, 144, 192]))
        self.assertEqual(bytes([0, 0, 144, 192]), Literal.of(-4.5).to_byte_buffer())

        # doubles are stored as 8 bytes in little-endian order
        # floating point numbers are represented as sign * 2ˆexponent * mantissa
        # 6.0 is 1 * 2ˆ4 * 1.5 and encoded as 01000000|00011000|0...0
        # 00000000 -> 0, ... , 00011000 -> 24, 01000000 -> 64
        self.assertConversion(6.0, DoubleType.get(), bytes([0, 0, 0, 0, 0, 0, 24, 64]))
        self.assertEqual(bytes([0, 0, 0, 0, 0, 0, 24, 64]), Literal.of(6.0).to(DoubleType.get()).to_byte_buffer())

        # dates are stored as days from 1970-01-01 in a 4-byte little-endian int
        # 1000 is 0...0|00000011|11101000 in binary
        # 11101000 -> 232 (-24), 00000011 -> 3, ... , 00000000 -> 0
        self.assertConversion(1000, DateType.get(), bytes([232, 3, 0, 0]))
        self.assertEqual(bytes([232, 3, 0, 0]), Literal.of(1000).to(DateType.get()).to_byte_buffer())

        # time is stored as microseconds from midnight in an 8-byte little-endian long
        # 10000L is 0...0|00100111|00010000 in binary
        # 00010000 -> 16, 00100111 -> 39, ... , 00000000 -> 0
        self.assertConversion(10000, TimeType.get(), bytes([16, 39, 0, 0, 0, 0, 0, 0]))
        self.assertEqual(bytes([16, 39, 0, 0, 0, 0, 0, 0]), Literal.of(10000).to(LongType.get()).to(TimeType.get()).to_byte_buffer())

        # timestamps are stored as microseconds from 1970-01-01 00:00:00.000000 in an 8-byte little-endian long
        # 400000L is 0...110|00011010|10000000 in binary
        # 10000000 -> 128 (-128), 00011010 -> 26, 00000110 -> 6, ... , 00000000 -> 0
        self.assertConversion(400000, TimestampType.without_timezone(), bytes([128, 26, 6, 0, 0, 0, 0, 0]))
        self.assertConversion(400000, TimestampType.with_timezone(), bytes([128, 26, 6, 0, 0, 0, 0, 0]))
        self.assertEqual(bytes([128, 26, 6, 0, 0, 0, 0, 0]),
                         Literal.of(400000).to(LongType.get()).to(TimestampType.without_timezone()).to_byte_buffer())
        self.assertEqual(bytes([128, 26, 6, 0, 0, 0, 0, 0]),
                         Literal.of(400000).to(LongType.get()).to(TimestampType.with_timezone()).to_byte_buffer())

        # strings are stored as UTF-8 bytes (without length)
        # 'A' -> 65, 'B' -> 66, 'C' -> 67
        self.assertConversion("ABC", StringType.get(), bytes([65, 66, 67]))
        self.assertEqual(bytes([65, 66, 67]), Literal.of("ABC").to_byte_buffer())

        # uuids are stored as 16-byte big-endian values
        # f79c3e09-677c-4bbd-a479-3f349cb785e7 is encoded as F7 9C 3E 09 67 7C 4B BD A4 79 3F 34 9C B7 85 E7
        # 0xF7 -> 11110111 -> 247 (-9), 0x9C -> 10011100 -> 156 (-100), 0x3E -> 00111110 -> 62,
        # 0x09 -> 00001001 -> 9, 0x67 -> 01100111 -> 103, 0x7C -> 01111100 -> 124,
        # 0x4B -> 01001011 -> 75, 0xBD -> 10111101 -> 189 (-67), 0xA4 -> 10100100 -> 164 (-92),
        # 0x79 -> 01111001 -> 121, 0x3F -> 00111111 -> 63, 0x34 -> 00110100 -> 52,
        # 0x9C -> 10011100 -> 156 (-100), 0xB7 -> 10110111 -> 183 (-73), 0x85 -> 10000101 -> 133 (-123),
        # 0xE7 -> 11100111 -> 231 (-25)
        self.assertConversion(
            uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"),
            UUIDType.get(),
            bytes([247, 156, 62, 9, 103, 124, 75, 189, 164, 121, 63, 52, 156, 183, 133, 231]))
        self.assertEqual(
            bytes([247, 156, 62, 9, 103, 124, 75, 189, 164, 121, 63, 52, 156, 183, 133, 231]),
            Literal.of(uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7")).to_byte_buffer())

        # fixed values are stored directly
        # 'a' -> 97, 'b' -> 98
        self.assertConversion(
            bytes("ab", "utf8"),
            FixedType.of_length(2),
            bytes([97, 98]))
        self.assertEqual(
            bytes([97, 98]),
            Literal.of(bytes("ab", "utf8")).to_byte_buffer())

        # binary values are stored directly
        # 'Z' -> 90
        self.assertConversion(
            bytearray("Z", "utf8"),
            BinaryType.get(),
            bytes([90]))
        self.assertEqual(
            bytes([90]),
            Literal.of(bytearray("Z", "utf8")).to_byte_buffer())

        # decimals are stored as unscaled values in the form of two's-complement big-endian binary,
        # using the minimum number of bytes for the values
        # 345 is 0...1|01011001 in binary
        # 00000001 -> 1, 01011001 -> 89
        self.assertConversion(
            Decimal(3.45).quantize(Decimal(".01")),
            DecimalType.of(3, 2),
            bytes([1, 89]))
        self.assertEqual(
            bytes([1, 89]),
            Literal.of(3.45).to(DecimalType.of(3, 2)).to_byte_buffer())

        # decimal on 3-bytes to test that we use the minimum number of bytes and not a power of 2
        # 1234567 is 00010010|11010110|10000111 in binary
        # 00010010 -> 18, 11010110 -> 214, 10000111 -> 135
        self.assertConversion(
            Decimal(123.4567).quantize(Decimal(".0001")),
            DecimalType.of(7, 4),
            bytes([18, 214, 135]))
        self.assertEqual(
            bytes([18, 214, 135]),
            Literal.of(123.4567).to(DecimalType.of(7, 4)).to_byte_buffer())

        # negative decimal to test two's complement
        # -1234567 is 11101101|00101001|01111001 in binary
        # 11101101 -> 237, 00101001 -> 41, 01111001 -> 121
        self.assertConversion(
            Decimal(-123.4567).quantize(Decimal(".0001")),
            DecimalType.of(7, 4),
            bytes([237, 41, 121]))
        self.assertEqual(
            bytes([237, 41, 121]),
            Literal.of(-123.4567).to(DecimalType.of(7, 4)).to_byte_buffer())

        # test empty byte in decimal
        # 11 is 00001011 in binary
        # 00001011 -> 11
        self.assertConversion(
            Decimal(0.011).quantize(Decimal(".001")),
            DecimalType.of(10, 3),
            bytes([11]))
        self.assertEqual(
            bytes([11]),
            Literal.of(0.011).to(DecimalType.of(10, 3)).to_byte_buffer())

    def assertConversion(self, value, type, expected_binary):
        byte_buffer = Conversions.to_byte_buffer(type.type_id, value)
        self.assertEqual(expected_binary, byte_buffer)
        self.assertEqual(value, Conversions.from_byte_buffer(type, byte_buffer))
