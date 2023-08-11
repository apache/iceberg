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
# pylint:disable=protected-access

import io
import struct
from typing import Dict, List

import pytest
from _decimal import Decimal

from pyiceberg.avro.encoder import BinaryEncoder
from pyiceberg.avro.resolver import construct_writer
from pyiceberg.avro.writer import (
    BinaryWriter,
    BooleanWriter,
    DateWriter,
    DecimalWriter,
    DoubleWriter,
    FixedWriter,
    FloatWriter,
    IntegerWriter,
    StringWriter,
    TimestamptzWriter,
    TimestampWriter,
    TimeWriter,
    UUIDWriter,
)
from pyiceberg.typedef import Record
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)


def zigzag_encode(datum: int) -> bytes:
    result = []
    datum = (datum << 1) ^ (datum >> 63)
    while (datum & ~0x7F) != 0:
        result.append(struct.pack("B", (datum & 0x7F) | 0x80))
        datum >>= 7
    result.append(struct.pack("B", datum))
    return b"".join(result)


def test_fixed_writer() -> None:
    assert construct_writer(FixedType(22)) == FixedWriter(22)


def test_decimal_writer() -> None:
    assert construct_writer(DecimalType(19, 25)) == DecimalWriter(19, 25)


def test_boolean_writer() -> None:
    assert construct_writer(BooleanType()) == BooleanWriter()


def test_integer_writer() -> None:
    assert construct_writer(IntegerType()) == IntegerWriter()


def test_long_writer() -> None:
    assert construct_writer(LongType()) == IntegerWriter()


def test_float_writer() -> None:
    assert construct_writer(FloatType()) == FloatWriter()


def test_double_writer() -> None:
    assert construct_writer(DoubleType()) == DoubleWriter()


def test_date_writer() -> None:
    assert construct_writer(DateType()) == DateWriter()


def test_time_writer() -> None:
    assert construct_writer(TimeType()) == TimeWriter()


def test_timestamp_writer() -> None:
    assert construct_writer(TimestampType()) == TimestampWriter()


def test_timestamptz_writer() -> None:
    assert construct_writer(TimestamptzType()) == TimestamptzWriter()


def test_string_writer() -> None:
    assert construct_writer(StringType()) == StringWriter()


def test_binary_writer() -> None:
    assert construct_writer(BinaryType()) == BinaryWriter()


def test_unknown_type() -> None:
    class UnknownType(PrimitiveType):
        __root__ = "UnknownType"

    with pytest.raises(ValueError) as exc_info:
        construct_writer(UnknownType())

    assert "Unknown type:" in str(exc_info.value)


def test_uuid_writer() -> None:
    assert construct_writer(UUIDType()) == UUIDWriter()


def test_write_simple_struct() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    schema = StructType(
        NestedField(1, "id", IntegerType(), required=True), NestedField(2, "property", StringType(), required=True)
    )

    class MyStruct(Record):
        id: int
        property: str

    my_struct = MyStruct(id=12, property="awesome")

    enc_str = b"awesome"

    construct_writer(schema).write(encoder, my_struct)

    assert output.getbuffer() == b"".join([b"\x18", zigzag_encode(len(enc_str)), enc_str])


def test_write_struct_with_dict() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    schema = StructType(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "properties", MapType(3, IntegerType(), 4, IntegerType()), required=True),
    )

    class MyStruct(Record):
        id: int
        properties: Dict[int, int]

    my_struct = MyStruct(id=12, properties={1: 2, 3: 4})

    construct_writer(schema).write(encoder, my_struct)

    assert output.getbuffer() == b"".join(
        [
            b"\x18",
            zigzag_encode(len(my_struct.properties)),
            zigzag_encode(1),
            zigzag_encode(2),
            zigzag_encode(3),
            zigzag_encode(4),
            b"\x00",
        ]
    )


def test_write_struct_with_list() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    schema = StructType(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "properties", ListType(3, IntegerType()), required=True),
    )

    class MyStruct(Record):
        id: int
        properties: List[int]

    my_struct = MyStruct(id=12, properties=[1, 2, 3, 4])

    construct_writer(schema).write(encoder, my_struct)

    assert output.getbuffer() == b"".join(
        [
            b"\x18",
            zigzag_encode(len(my_struct.properties)),
            zigzag_encode(1),
            zigzag_encode(2),
            zigzag_encode(3),
            zigzag_encode(4),
            b"\x00",
        ]
    )


def test_write_decimal() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    schema = StructType(
        NestedField(1, "decimal", DecimalType(10, 2), required=True),
    )

    class MyStruct(Record):
        decimal: Decimal

    construct_writer(schema).write(encoder, MyStruct(Decimal("1000.12")))

    assert output.getvalue() == b"\x00\x00\x01\x86\xac"
