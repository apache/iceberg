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
        (DateType(), b"\xd2\x04\x00\x00", 1234),
        (TimeType(), b"\x00\xe8vH\x17\x00\x00\x00", 100000000000),
        (TimestamptzType(), b"\x00\xe8vH\x17\x00\x00\x00", 100000000000),
        (TimestampType(), b"\x00\xe8vH\x17\x00\x00\x00", 100000000000),
        (StringType(), b"foo", "foo"),
        (UUIDType(), b"\xf7\x9c>\tg|K\xbd\xa4y?4\x9c\xb7\x85\xe7", uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7")),
        (FixedType(3), b"foo", b"foo"),
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
    """Test approximate equality when converting from bytes"""
    assert conversions.from_bytes(primitive_type, b) == pytest.approx(approximate_result, approximation)


@pytest.mark.parametrize(
    "primitive_type, b, result",
    [
        (BooleanType(), b"\x00", False),
        (BooleanType(), b"\x01", True),
        (IntegerType(), b"\xd2\x04\x00\x00", 1234),
        (LongType(), b"\xd2\x04\x00\x00\x00\x00\x00\x00", 1234),
        (DateType(), b"\xd2\x04\x00\x00", 1234),
        (TimeType(), b"\x00\xe8vH\x17\x00\x00\x00", 100000000000),
        (TimestamptzType(), b"\x00\xe8vH\x17\x00\x00\x00", 100000000000),
        (TimestampType(), b"\x00\xe8vH\x17\x00\x00\x00", 100000000000),
        (StringType(), b"foo", "foo"),
        (UUIDType(), b"\xf7\x9c>\tg|K\xbd\xa4y?4\x9c\xb7\x85\xe7", uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7")),
        (FixedType(3), b"foo", b"foo"),
        (BinaryType(), b"foo", b"foo"),
        (DecimalType(5, 2), b"\x30\x39", Decimal("123.45")),
        (DecimalType(3, 2), bytes([1, 89]), Decimal("3.45")),
        (DecimalType(7, 4), bytes([18, 214, 135]), Decimal("123.4567")),
        (DecimalType(7, 4), bytes([237, 41, 121]), Decimal("-123.4567")),
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
        (FloatType(), b"\x19\x04\x9e?", 1.2345),
        (DoubleType(), b"\x8d\x97n\x12\x83\xc0\xf3?", 1.2345),
    ],
)
def test_round_trip_conversion_approximation(primitive_type, b, result):
    """Test approximate round trip conversions of calling `conversions.from_bytes` and then `conversions.to_bytes` on the result"""
    value_from_bytes = conversions.from_bytes(primitive_type, b)
    assert value_from_bytes == pytest.approx(result)

    bytes_from_value = conversions.to_bytes(primitive_type, value_from_bytes)
    assert bytes_from_value == b


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
