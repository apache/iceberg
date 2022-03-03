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
    "type_, partition_value_as_str, expected_result",
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
        (StringType(), "foo", "foo"),
        (UUIDType(), "f79c3e09-677c-4bbd-a479-3f349cb785e7", uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7")),
        (FixedType(3), "foo", bytearray(b"foo")),
        (BinaryType(), "foo", b"foo"),
        (None, "__HIVE_DEFAULT_PARTITION__", None),
        (None, None, None),
    ],
)
def test_from_partition_value_to_py(type_, partition_value_as_str, expected_result):
    """Test converting a partition value to a python built-in"""
    assert conversions.from_partition_value_to_py(type_, partition_value_as_str) == expected_result


@pytest.mark.parametrize(
    "type_, partition_value_as_str, expected_result",
    [
        (DecimalType(5, 2), "123.45", Decimal(123.45)),
    ],
)
def test_from_partition_value_to_py_approximated(type_, partition_value_as_str, expected_result):
    """Test approximate equality when converting a partition value to a python built-in"""
    assert conversions.from_partition_value_to_py(type_, partition_value_as_str) == pytest.approx(expected_result)


@pytest.mark.parametrize(
    "type_, b, result",
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
        (DecimalType(5, 2), b"\x30\x39", Decimal(123.45).quantize(Decimal(".01"))),
        (DecimalType(5, 4), b"\x00\x12\xd6\x87", Decimal(123.4567).quantize(Decimal(".0001"))),
        (DecimalType(5, 4), b"\xff\xed\x29\x79", Decimal(-123.4567).quantize(Decimal(".0001"))),
    ],
)
def test_from_bytes(type_, b, result):
    """Test converting from bytes"""
    assert conversions.from_bytes(type_, b) == result


@pytest.mark.parametrize(
    "type_, b, approximate_result, approximation",
    [
        (FloatType(), b"\x19\x04\x9e?", 1.2345, 5),
        (DoubleType(), b"\x8d\x97\x6e\x12\x83\xc0\xf3\x3f", 1.2345, 1e-6),
    ],
)
def test_from_bytes_approximately(type_, b, approximate_result, approximation):
    """Test approximate equality when converting from bytes"""
    assert conversions.from_bytes(type_, b) == pytest.approx(approximate_result, approximation)


@pytest.mark.parametrize(
    "type_, b, result",
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
        (DecimalType(5, 2), b"\x30\x39", Decimal(123.45).quantize(Decimal(".01"))),
        (DecimalType(3, 2), bytes([1, 89]), Decimal(3.45).quantize(Decimal(".01"))),
        (DecimalType(7, 4), bytes([18, 214, 135]), Decimal(123.4567).quantize(Decimal(".0001"))),
        (DecimalType(7, 4), bytes([237, 41, 121]), Decimal(-123.4567).quantize(Decimal(".0001"))),
    ],
)
def test_round_trip_conversion(type_, b, result):
    """Test round trip conversions of calling `conversions.from_bytes` and then `conversions.to_bytes` on the result"""
    value_from_bytes = conversions.from_bytes(type_, b)
    assert value_from_bytes == result

    bytes_from_value = conversions.to_bytes(type_, value_from_bytes)
    assert bytes_from_value == b


@pytest.mark.parametrize(
    "type_, b, result",
    [
        (FloatType(), b"\x19\x04\x9e?", 1.2345),
        (DoubleType(), b"\x8d\x97n\x12\x83\xc0\xf3?", 1.2345),
    ],
)
def test_round_trip_conversion_approximation(type_, b, result):
    """Test approximate round trip conversions of calling `conversions.from_bytes` and then `conversions.to_bytes` on the result"""
    value_from_bytes = conversions.from_bytes(type_, b)
    assert value_from_bytes == pytest.approx(result)

    bytes_from_value = conversions.to_bytes(type_, value_from_bytes)
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
