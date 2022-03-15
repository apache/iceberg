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
"""This test module tests PrimitiveType based conversions of values to/from bytes

Notes:
    Boolean:
        - Stored as 0x00 for False and non-zero byte for True
    Integer:
        - Stored as 4 bytes in little-endian order
        - 84202 is 0...01|01001000|11101010 in binary
    Long:
        - Stored as 8 bytes in little-endian order
        - 200L is 0...0|11001000 in binary
        - 11001000 -> 200 (-56), 00000000 -> 0, ... , 00000000 -> 0
    Double:
        - Stored as 8 bytes in little-endian order
        - floating point numbers are represented as sign * 2ˆexponent * mantissa
        - 6.0 is 1 * 2ˆ4 * 1.5 and encoded as 01000000|00011000|0...0
        - 00000000 -> 0, ... , 00011000 -> 24, 01000000 -> 64
    Date:
        - Stored as days from 1970-01-01 in a 4-byte little-endian int
        - 1000 is 0...0|00000011|11101000 in binary
        - 11101000 -> 232 (-24), 00000011 -> 3, ... , 00000000 -> 0
    Time:
        - Stored as microseconds from midnight in an 8-byte little-endian long
        - 10000L is 0...0|00100111|00010000 in binary
        - 00010000 -> 16, 00100111 -> 39, ... , 00000000 -> 0
    Timestamp:
        - Stored as microseconds from 1970-01-01 00:00:00.000000 in an 8-byte little-endian long
        - 400000L is 0...110|00011010|10000000 in binary
        - 10000000 -> 128 (-128), 00011010 -> 26, 00000110 -> 6, ... , 00000000 -> 0
    String:
        - Stored as UTF-8 bytes (without length)
        - 'A' -> 65, 'B' -> 66, 'C' -> 67
    UUID:
        - Stored as 16-byte big-endian values
        - f79c3e09-677c-4bbd-a479-3f349cb785e7 is encoded as F7 9C 3E 09 67 7C 4B BD A4 79 3F 34 9C B7 85 E7
        - 0xF7 -> 11110111 -> 247 (-9), 0x9C -> 10011100 -> 156 (-100), 0x3E -> 00111110 -> 62,
        - 0x09 -> 00001001 -> 9, 0x67 -> 01100111 -> 103, 0x7C -> 01111100 -> 124,
        - 0x4B -> 01001011 -> 75, 0xBD -> 10111101 -> 189 (-67), 0xA4 -> 10100100 -> 164 (-92),
        - 0x79 -> 01111001 -> 121, 0x3F -> 00111111 -> 63, 0x34 -> 00110100 -> 52,
        - 0x9C -> 10011100 -> 156 (-100), 0xB7 -> 10110111 -> 183 (-73), 0x85 -> 10000101 -> 133 (-123),
        - 0xE7 -> 11100111 -> 231 (-25)
    Fixed:
        - Stored directly
        - 'a' -> 97, 'b' -> 98
    Binary:
        - Stored directly
        - 'Z' -> 90
    Decimal:
        - Stored as unscaled values in the form of two's-complement big-endian binary using the minimum number of bytes for the values
        - 345 is 0...1|01011001 in binary
        - 00000001 -> 1, 01011001 -> 89
    Float:
        - Stored as 4 bytes in little-endian order
        - floating point numbers are represented as sign * 2ˆexponent * mantissa
        - -4.5F is -1 * 2ˆ2 * 1.125 and encoded as 11000000|10010000|0...0 in binary
        - 00000000 -> 0, 00000000 -> 0, 10010000 -> 144 (-112), 11000000 -> 192 (-64),
"""
import uuid
from decimal import Decimal

import pytest

from iceberg import conversions
from iceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)


@pytest.mark.parametrize(
    "primitive_type, partition_value_as_str, expected_result",
    [
        (BooleanType(), "true", True),
        (BooleanType(), "false", False),
        (BooleanType(), "TRUE", True),
        (BooleanType(), "FALSE", False),
        (BooleanType(), None, False),
        (IntegerType(), "1", 1),
        (IntegerType(), "9999", 9999),
        (LongType(), "123456789", 123456789),
        (FloatType(), "1.1", 1.1),
        (DoubleType(), "99999.9", 99999.9),
        (DecimalType(5, 2), "123.45", Decimal("123.45")),
        (StringType(), "foo", "foo"),
        (UUIDType(), "f79c3e09-677c-4bbd-a479-3f349cb785e7", uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7")),
        (FixedType(3), "foo", bytearray(b"foo")),
        (BinaryType(), "foo", b"foo"),
        (None, None, None),
    ],
)
def test_from_partition_value_to_py(primitive_type, partition_value_as_str, expected_result):
    """Test converting a partition value to a python built-in"""
    assert conversions.from_partition_value_to_py(primitive_type, partition_value_as_str) == expected_result


@pytest.mark.parametrize(
    "primitive_type, b, result",
    [
        (BooleanType(), b"\x00", False),
        (BooleanType(), b"\x01", True),
        (IntegerType(), b"\xd2\x04\x00\x00", 1234),
        (LongType(), b"\xd2\x04\x00\x00\x00\x00\x00\x00", 1234),
        (DoubleType(), b"\x8d\x97\x6e\x12\x83\xc0\xf3\x3f", 1.2345),
        (DateType(), bytes([232, 3, 0, 0]), 1000),
        (DateType(), b"\xd2\x04\x00\x00", 1234),
        (TimeType(), bytes([16, 39, 0, 0, 0, 0, 0, 0]), 10000),
        (TimeType(), b"\x00\xe8vH\x17\x00\x00\x00", 100000000000),
        (TimestamptzType(), bytes([128, 26, 6, 0, 0, 0, 0, 0]), 400000),
        (TimestamptzType(), b"\x00\xe8vH\x17\x00\x00\x00", 100000000000),
        (TimestampType(), bytes([128, 26, 6, 0, 0, 0, 0, 0]), 400000),
        (TimestampType(), b"\x00\xe8vH\x17\x00\x00\x00", 100000000000),
        (StringType(), bytes([65, 66, 67]), "ABC"),
        (StringType(), b"foo", "foo"),
        (
            UUIDType(),
            bytes([247, 156, 62, 9, 103, 124, 75, 189, 164, 121, 63, 52, 156, 183, 133, 231]),
            uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"),
        ),
        (UUIDType(), b"\xf7\x9c>\tg|K\xbd\xa4y?4\x9c\xb7\x85\xe7", uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7")),
        (FixedType(3), bytes([97, 98]), bytes("ab", "utf8")),
        (FixedType(3), b"foo", b"foo"),
        (BinaryType(), bytearray("Z", "utf8"), bytes([90])),
        (BinaryType(), b"foo", b"foo"),
        (DecimalType(5, 2), b"\x30\x39", Decimal("123.45")),
        (DecimalType(7, 4), b"\x00\x12\xd6\x87", Decimal("123.4567")),
        (DecimalType(7, 4), b"\xff\xed\x29\x79", Decimal("-123.4567")),
    ],
)
def test_from_bytes(primitive_type, b, result):
    """Test converting from bytes"""
    assert conversions.from_bytes(primitive_type, b) == result


@pytest.mark.parametrize(
    "primitive_type, b, approximate_result, approximation",
    [
        (FloatType(), b"\x19\x04\x9e?", 1.2345, 5),
    ],
)
def test_from_bytes_approximately(primitive_type, b, approximate_result, approximation):
    """Test approximate equality when converting from bytes

    Note: This tests approximate equality because floating point numbers in python are implemented
    using a double in C. Therefore a 32-bit (single precision) float is unpacked to 64-bit (double precision)
    float in python which introduces some imprecision.
    """
    assert conversions.from_bytes(primitive_type, b) == pytest.approx(approximate_result, approximation)


@pytest.mark.parametrize(
    "primitive_type, b, result",
    [
        (BooleanType(), b"\x00", False),
        (BooleanType(), b"\x01", True),
        (IntegerType(), bytes([234, 72, 1, 0]), 84202),
        (IntegerType(), b"\xd2\x04\x00\x00", 1234),
        (LongType(), bytes([200, 0, 0, 0, 0, 0, 0, 0]), 200),
        (LongType(), b"\xd2\x04\x00\x00\x00\x00\x00\x00", 1234),
        (DoubleType(), bytes([0, 0, 0, 0, 0, 0, 24, 64]), 6.0),
        (DoubleType(), b"\x8d\x97n\x12\x83\xc0\xf3?", 1.2345),
        (DateType(), bytes([232, 3, 0, 0]), 1000),
        (DateType(), b"\xd2\x04\x00\x00", 1234),
        (TimeType(), bytes([16, 39, 0, 0, 0, 0, 0, 0]), 10000),
        (TimeType(), b"\x00\xe8vH\x17\x00\x00\x00", 100000000000),
        (TimestamptzType(), bytes([128, 26, 6, 0, 0, 0, 0, 0]), 400000),
        (TimestamptzType(), b"\x00\xe8vH\x17\x00\x00\x00", 100000000000),
        (TimestampType(), b"\x00\xe8vH\x17\x00\x00\x00", 100000000000),
        (StringType(), bytes([65, 66, 67]), "ABC"),
        (StringType(), b"foo", "foo"),
        (
            UUIDType(),
            bytes([247, 156, 62, 9, 103, 124, 75, 189, 164, 121, 63, 52, 156, 183, 133, 231]),
            uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"),
        ),
        (UUIDType(), b"\xf7\x9c>\tg|K\xbd\xa4y?4\x9c\xb7\x85\xe7", uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7")),
        (FixedType(3), bytes([97, 98]), bytes("ab", "utf8")),
        (FixedType(3), b"foo", b"foo"),
        (BinaryType(), bytearray("Z", "utf8"), bytes([90])),
        (BinaryType(), b"foo", b"foo"),
        (DecimalType(5, 2), b"\x30\x39", Decimal("123.45")),
        (DecimalType(3, 2), bytes([1, 89]), Decimal("3.45")),
        # decimal on 3-bytes to test that we use the minimum number of bytes and not a power of 2
        # 1234567 is 00010010|11010110|10000111 in binary
        # 00010010 -> 18, 11010110 -> 214, 10000111 -> 135
        (DecimalType(7, 4), bytes([18, 214, 135]), Decimal("123.4567")),
        # negative decimal to test two's complement
        # -1234567 is 11101101|00101001|01111001 in binary
        # 11101101 -> 237, 00101001 -> 41, 01111001 -> 121
        (DecimalType(7, 4), bytes([237, 41, 121]), Decimal("-123.4567")),
        # test empty byte in decimal
        # 11 is 00001011 in binary
        # 00001011 -> 11
        (DecimalType(10, 3), bytes([11]), Decimal("0.011")),
    ],
)
def test_round_trip_conversion(primitive_type, b, result):
    """Test round trip conversions of calling `conversions.from_bytes` and then `conversions.to_bytes` on the result"""
    value_from_bytes = conversions.from_bytes(primitive_type, b)
    assert value_from_bytes == result

    bytes_from_value = conversions.to_bytes(primitive_type, value_from_bytes)
    assert bytes_from_value == b


@pytest.mark.parametrize(
    "primitive_type, b, result",
    [
        (
            DecimalType(38, 21),
            b"\tI\xb0\xf7\x13\xe9\x180s\xb9\x1e~\xa2\xb3j\x83",
            Decimal("12345678912345678.123456789123456789123"),
        ),
        (DecimalType(38, 22), b'\tI\xb0\xf7\x13\xe9\x16\xbb\x01/L\xc3+B)"', Decimal("1234567891234567.1234567891234567891234")),
        (
            DecimalType(38, 23),
            b"\tI\xb0\xf7\x13\xe9\nB\xa1\xad\xe5+3\x15\x9bY",
            Decimal("123456789123456.12345678912345678912345"),
        ),
        (
            DecimalType(38, 24),
            b"\tI\xb0\xf7\x13\xe8\xa2\xbb\xe9g\xba\x86w\xd8\x11\x80",
            Decimal("12345678912345.123456789123456789123456"),
        ),
        (
            DecimalType(38, 25),
            b"\tI\xb0\xf7\x13\xe5k:\xd2x\xdd\x04\xc8p\xaf\x07",
            Decimal("1234567891234.1234567891234567891234567"),
        ),
        (DecimalType(38, 26), b"\tI\xb0\xf7\x13\xcd\x85\xc5\x0387<8f\xd6N", Decimal("123456789123.12345678912345678912345678")),
        (DecimalType(38, 27), b"\tI\xb0\xf7\x131F\xfd\xc7y\xca9|\x04_\x15", Decimal("12345678912.123456789123456789123456789")),
        (
            DecimalType(38, 28),
            b"\tI\xb0\xf7\x10R\x01r\x11\xda\x08[\x08+\xb6\xd3",
            Decimal("1234567891.1234567891234567891234567891"),
        ),
        (
            DecimalType(38, 29),
            b"\tI\xb0\xf7\x13\xe9\x18[7\xc1x\x0b\x91\xb5$@",
            Decimal("123456789.12345678912345678912345678912"),
        ),
        (
            DecimalType(38, 30),
            b"\tI\xb0\xed\x1e\xdf\x80\x03G;\x16\x9b\xf1\x13j\x83",
            Decimal("12345678.123456789123456789123456789123"),
        ),
        (DecimalType(38, 31), b'\tI\xb0\x96+\xac)d(p6)\xea\xc2)"', Decimal("1234567.1234567891234567891234567891234")),
        (
            DecimalType(38, 32),
            b"\tI\xad\xae\xe3h\xe7O\xb5\x14\xbc\xdc+\x95\x9bY",
            Decimal("123456.12345678912345678912345678912345"),
        ),
        (
            DecimalType(38, 33),
            b"\tI\x95\x94>5\x93\xde\xb9.\xefS\xb3\xd8\x11\x80",
            Decimal("12345.123456789123456789123456789123456"),
        ),
        (
            DecimalType(38, 34),
            b"\tH\xd5\xd7\x90x\xdf\x08\x1a\xf6C\t\x06p\xaf\x07",
            Decimal("1234.1234567891234567891234567891234567"),
        ),
        (DecimalType(38, 35), b"\tCE\x82\x85\xc7Vf$M\x16\x82@f\xd6N", Decimal("123.12345678912345678912345678912345678")),
        (DecimalType(21, 16), b"\x06\xb1:\xe3\xc4N\x94\xaf\x07", Decimal("12345.1234567891234567")),
        (DecimalType(22, 17), b"B\xecL\xe5\xab\x11\xce\xd6N", Decimal("12345.12345678912345678")),
        (DecimalType(23, 18), b"\x02\x9d;\x00\xf8\xae\xb2\x14_\x15", Decimal("12345.123456789123456789")),
        (DecimalType(24, 19), b"\x1a$N\t\xb6\xd2\xf4\xcb\xb6\xd3", Decimal("12345.1234567891234567891")),
        (DecimalType(25, 20), b"\x01\x05k\x0ca$=\x8f\xf5$@", Decimal("12345.12345678912345678912")),
        (DecimalType(26, 21), b"\n6.{\xcbjg\x9f\x93j\x83", Decimal("12345.123456789123456789123")),
        (DecimalType(27, 22), b'f\x1d\xd0\xd5\xf2(\x0c;\xc2)"', Decimal("12345.1234567891234567891234")),
        (DecimalType(28, 23), b"\x03\xfd*([u\x90zU\x95\x9bY", Decimal("12345.12345678912345678912345")),
        (DecimalType(29, 24), b"'\xe3\xa5\x93\x92\x97\xa4\xc7W\xd8\x11\x80", Decimal("12345.123456789123456789123456")),
        (DecimalType(30, 25), b"\x01\x8e\xe4w\xc3\xb9\xeco\xc9np\xaf\x07", Decimal("12345.1234567891234567891234567")),
        (DecimalType(31, 26), b"\x0f\x94\xec\xad\xa5C<]\xdePf\xd6N", Decimal("12345.12345678912345678912345678")),
    ],
)
def test_round_trip_conversion_large_decimals(primitive_type, b, result):
    """Test round trip conversions of calling `conversions.from_bytes` and then `conversions.to_bytes` on the result"""
    value_from_bytes = conversions.from_bytes(primitive_type, b)
    assert value_from_bytes == result

    bytes_from_value = conversions.to_bytes(primitive_type, value_from_bytes)
    assert bytes_from_value == b


@pytest.mark.parametrize(
    "primitive_type, b, result",
    [
        (FloatType(), bytes([0, 0, 144, 192]), -4.5),
        (FloatType(), b"\x19\x04\x9e?", 1.2345),
    ],
)
def test_round_trip_conversion_approximation(primitive_type, b, result):
    """Test approximate round trip conversions of calling `conversions.from_bytes` and then `conversions.to_bytes` on the result"""
    value_from_bytes = conversions.from_bytes(primitive_type, b)
    assert value_from_bytes == pytest.approx(result)

    bytes_from_value = conversions.to_bytes(primitive_type, value_from_bytes)
    assert bytes_from_value == b


@pytest.mark.parametrize(
    "primitive_type, expected_max_value",
    [
        (DecimalType(6, 2), Decimal("9999.99")),
        (DecimalType(10, 10), Decimal(".9999999999")),
        (DecimalType(2, 1), Decimal("9.9")),
        (DecimalType(38, 37), Decimal("9.9999999999999999999999999999999999999")),
        (DecimalType(20, 1), Decimal("9999999999999999999.9")),
    ],
)
def test_max_value_round_trip_conversion(primitive_type, expected_max_value):
    """Test round trip conversions of maximum DecimalType values"""
    b = conversions.to_bytes(primitive_type, expected_max_value)
    value_from_bytes = conversions.from_bytes(primitive_type, b)

    assert value_from_bytes == expected_max_value


@pytest.mark.parametrize(
    "primitive_type, expected_min_value",
    [
        (DecimalType(6, 2), Decimal("-9999.99")),
        (DecimalType(10, 10), Decimal("-.9999999999")),
        (DecimalType(2, 1), Decimal("-9.9")),
        (DecimalType(38, 37), Decimal("-9.9999999999999999999999999999999999999")),
        (DecimalType(20, 1), Decimal("-9999999999999999999.9")),
    ],
)
def test_min_value_round_trip_conversion(primitive_type, expected_min_value):
    """Test round trip conversions of minimum DecimalType values"""
    b = conversions.to_bytes(primitive_type, expected_min_value)
    value_from_bytes = conversions.from_bytes(primitive_type, b)

    assert value_from_bytes == expected_min_value


def test_raise_on_unregistered_type():
    """Test raising when a conversion is attempted for a type that has no registered method"""

    class FooUnknownType:
        def __repr__(self):
            return "FooUnknownType()"

    with pytest.raises(TypeError) as exc_info:
        conversions.from_partition_value_to_py(FooUnknownType(), "foo")
    assert (f"Cannot convert partition string to python built-in, type FooUnknownType() not supported: 'foo'") in str(
        exc_info.value
    )

    with pytest.raises(TypeError) as exc_info:
        conversions.to_bytes(FooUnknownType(), "foo")
    assert ("Cannot serialize value, type FooUnknownType() not supported: 'foo'") in str(exc_info.value)

    with pytest.raises(TypeError) as exc_info:
        conversions.from_bytes(FooUnknownType(), b"foo")
    assert ("Cannot deserialize bytes, type FooUnknownType() not supported: b'foo'") in str(exc_info.value)


@pytest.mark.parametrize(
    "primitive_type, value, expected_error_message",
    [
        (DecimalType(7, 3), Decimal("123.4567"), "Cannot serialize value, scale of value does not match type decimal(7, 3): 4"),
        (
            DecimalType(18, 8),
            Decimal("123456789.123456789"),
            "Cannot serialize value, scale of value does not match type decimal(18, 8): 9",
        ),
        (
            DecimalType(36, 34),
            Decimal("1.23456789123456789123456789123456789"),
            "Cannot serialize value, scale of value does not match type decimal(36, 34): 35",
        ),
        (
            DecimalType(7, 2),
            Decimal("1234567.89"),
            "Cannot serialize value, precision of value is greater than precision of type decimal(7, 2): 9",
        ),
        (
            DecimalType(17, 9),
            Decimal("123456789.123456789"),
            "Cannot serialize value, precision of value is greater than precision of type decimal(17, 9): 18",
        ),
        (
            DecimalType(35, 35),
            Decimal("1.23456789123456789123456789123456789"),
            "Cannot serialize value, precision of value is greater than precision of type decimal(35, 35): 36",
        ),
    ],
)
def test_raise_on_incorrect_precision_or_scale(primitive_type, value, expected_error_message):
    with pytest.raises(ValueError) as exc_info:
        conversions.to_bytes(primitive_type, value)

    assert expected_error_message in str(exc_info.value)
