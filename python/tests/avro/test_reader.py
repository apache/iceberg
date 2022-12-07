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
import json

import pytest

from pyiceberg.avro.file import AvroFile
from pyiceberg.avro.reader import (
    AvroStruct,
    BinaryReader,
    BooleanReader,
    DateReader,
    DecimalReader,
    DoubleReader,
    FixedReader,
    FloatReader,
    IntegerReader,
    StringReader,
    TimeReader,
    TimestampReader,
    TimestamptzReader,
    primitive_reader,
)
from pyiceberg.manifest import _convert_pos_to_dict
from pyiceberg.schema import Schema
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
)
from tests.io.test_io import LocalInputFile


def test_read_header(generated_manifest_entry_file: str, iceberg_manifest_entry_schema: Schema) -> None:
    with AvroFile(LocalInputFile(generated_manifest_entry_file)) as reader:
        header = reader._read_header()

    assert header.magic == b"Obj\x01"
    assert json.loads(header.meta["avro.schema"]) == {
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {"field-id": 0, "name": "status", "type": "int"},
            {"field-id": 1, "default": None, "name": "snapshot_id", "type": ["null", "long"]},
            {
                "field-id": 2,
                "name": "data_file",
                "type": {
                    "type": "record",
                    "name": "r2",
                    "fields": [
                        {"field-id": 100, "doc": "Location URI with FS scheme", "name": "file_path", "type": "string"},
                        {
                            "field-id": 101,
                            "doc": "File format name: avro, orc, or parquet",
                            "name": "file_format",
                            "type": "string",
                        },
                        {
                            "field-id": 102,
                            "name": "partition",
                            "type": {
                                "type": "record",
                                "name": "r102",
                                "fields": [{"field-id": 1000, "default": None, "name": "VendorID", "type": ["null", "int"]}],
                            },
                        },
                        {"field-id": 103, "doc": "Number of records in the file", "name": "record_count", "type": "long"},
                        {"field-id": 104, "doc": "Total file size in bytes", "name": "file_size_in_bytes", "type": "long"},
                        {"field-id": 105, "name": "block_size_in_bytes", "type": "long"},
                        {
                            "field-id": 108,
                            "doc": "Map of column id to total size on disk",
                            "default": None,
                            "name": "column_sizes",
                            "type": [
                                "null",
                                {
                                    "logicalType": "map",
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "k117_v118",
                                        "fields": [
                                            {"field-id": 117, "name": "key", "type": "int"},
                                            {"field-id": 118, "name": "value", "type": "long"},
                                        ],
                                    },
                                },
                            ],
                        },
                        {
                            "field-id": 109,
                            "doc": "Map of column id to total count, including null and NaN",
                            "default": None,
                            "name": "value_counts",
                            "type": [
                                "null",
                                {
                                    "logicalType": "map",
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "k119_v120",
                                        "fields": [
                                            {"field-id": 119, "name": "key", "type": "int"},
                                            {"field-id": 120, "name": "value", "type": "long"},
                                        ],
                                    },
                                },
                            ],
                        },
                        {
                            "field-id": 110,
                            "doc": "Map of column id to null value count",
                            "default": None,
                            "name": "null_value_counts",
                            "type": [
                                "null",
                                {
                                    "logicalType": "map",
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "k121_v122",
                                        "fields": [
                                            {"field-id": 121, "name": "key", "type": "int"},
                                            {"field-id": 122, "name": "value", "type": "long"},
                                        ],
                                    },
                                },
                            ],
                        },
                        {
                            "field-id": 137,
                            "doc": "Map of column id to number of NaN values in the column",
                            "default": None,
                            "name": "nan_value_counts",
                            "type": [
                                "null",
                                {
                                    "logicalType": "map",
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "k138_v139",
                                        "fields": [
                                            {"field-id": 138, "name": "key", "type": "int"},
                                            {"field-id": 139, "name": "value", "type": "long"},
                                        ],
                                    },
                                },
                            ],
                        },
                        {
                            "field-id": 125,
                            "doc": "Map of column id to lower bound",
                            "default": None,
                            "name": "lower_bounds",
                            "type": [
                                "null",
                                {
                                    "logicalType": "map",
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "k126_v127",
                                        "fields": [
                                            {"field-id": 126, "name": "key", "type": "int"},
                                            {"field-id": 127, "name": "value", "type": "bytes"},
                                        ],
                                    },
                                },
                            ],
                        },
                        {
                            "field-id": 128,
                            "doc": "Map of column id to upper bound",
                            "default": None,
                            "name": "upper_bounds",
                            "type": [
                                "null",
                                {
                                    "logicalType": "map",
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "k129_v130",
                                        "fields": [
                                            {"field-id": 129, "name": "key", "type": "int"},
                                            {"field-id": 130, "name": "value", "type": "bytes"},
                                        ],
                                    },
                                },
                            ],
                        },
                        {
                            "field-id": 131,
                            "doc": "Encryption key metadata blob",
                            "default": None,
                            "name": "key_metadata",
                            "type": ["null", "bytes"],
                        },
                        {
                            "field-id": 132,
                            "doc": "Splittable offsets",
                            "default": None,
                            "name": "split_offsets",
                            "type": ["null", {"element-id": 133, "type": "array", "items": "long"}],
                        },
                        {
                            "field-id": 140,
                            "doc": "Sort order ID",
                            "default": None,
                            "name": "sort_order_id",
                            "type": ["null", "int"],
                        },
                    ],
                },
            },
        ],
    }

    assert header.get_schema() == iceberg_manifest_entry_schema


def test_read_manifest_entry_file(generated_manifest_entry_file: str) -> None:
    with AvroFile(LocalInputFile(generated_manifest_entry_file)) as reader:
        # Consume the generator
        records = list(reader)

    assert len(records) == 2, f"Expected 2 records, got {len(records)}"
    assert records[0] == AvroStruct(
        _data=[
            1,
            8744736658442914487,
            AvroStruct(
                _data=[
                    "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=null/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00001.parquet",
                    "PARQUET",
                    AvroStruct(_data=[None]),
                    19513,
                    388872,
                    67108864,
                    {
                        1: 53,
                        2: 98153,
                        3: 98693,
                        4: 53,
                        5: 53,
                        6: 53,
                        7: 17425,
                        8: 18528,
                        9: 53,
                        10: 44788,
                        11: 35571,
                        12: 53,
                        13: 1243,
                        14: 2355,
                        15: 12750,
                        16: 4029,
                        17: 110,
                        18: 47194,
                        19: 2948,
                    },
                    {
                        1: 19513,
                        2: 19513,
                        3: 19513,
                        4: 19513,
                        5: 19513,
                        6: 19513,
                        7: 19513,
                        8: 19513,
                        9: 19513,
                        10: 19513,
                        11: 19513,
                        12: 19513,
                        13: 19513,
                        14: 19513,
                        15: 19513,
                        16: 19513,
                        17: 19513,
                        18: 19513,
                        19: 19513,
                    },
                    {
                        1: 19513,
                        2: 0,
                        3: 0,
                        4: 19513,
                        5: 19513,
                        6: 19513,
                        7: 0,
                        8: 0,
                        9: 19513,
                        10: 0,
                        11: 0,
                        12: 19513,
                        13: 0,
                        14: 0,
                        15: 0,
                        16: 0,
                        17: 0,
                        18: 0,
                        19: 0,
                    },
                    {16: 0, 17: 0, 18: 0, 19: 0, 10: 0, 11: 0, 12: 0, 13: 0, 14: 0, 15: 0},
                    {
                        2: b"2020-04-01 00:00",
                        3: b"2020-04-01 00:12",
                        7: b"\x03\x00\x00\x00",
                        8: b"\x01\x00\x00\x00",
                        10: b"\xf6(\\\x8f\xc2\x05S\xc0",
                        11: b"\x00\x00\x00\x00\x00\x00\x00\x00",
                        13: b"\x00\x00\x00\x00\x00\x00\x00\x00",
                        14: b"\x00\x00\x00\x00\x00\x00\xe0\xbf",
                        15: b")\\\x8f\xc2\xf5(\x08\xc0",
                        16: b"\x00\x00\x00\x00\x00\x00\x00\x00",
                        17: b"\x00\x00\x00\x00\x00\x00\x00\x00",
                        18: b"\xf6(\\\x8f\xc2\xc5S\xc0",
                        19: b"\x00\x00\x00\x00\x00\x00\x04\xc0",
                    },
                    {
                        2: b"2020-04-30 23:5:",
                        3: b"2020-05-01 00:41",
                        7: b"\t\x01\x00\x00",
                        8: b"\t\x01\x00\x00",
                        10: b"\xcd\xcc\xcc\xcc\xcc,_@",
                        11: b"\x1f\x85\xebQ\\\xe2\xfe@",
                        13: b"\x00\x00\x00\x00\x00\x00\x12@",
                        14: b"\x00\x00\x00\x00\x00\x00\xe0?",
                        15: b"q=\n\xd7\xa3\xf01@",
                        16: b"\x00\x00\x00\x00\x00`B@",
                        17: b"333333\xd3?",
                        18: b"\x00\x00\x00\x00\x00\x18b@",
                        19: b"\x00\x00\x00\x00\x00\x00\x04@",
                    },
                    None,
                    [4],
                    0,
                ]
            ),
        ]
    )


def test_read_manifest_file_file(generated_manifest_file_file: str) -> None:
    with AvroFile(LocalInputFile(generated_manifest_file_file)) as reader:
        # Consume the generator
        records = list(reader)

    assert len(records) == 1, f"Expected 1 records, got {len(records)}"
    actual = records[0]
    expected = AvroStruct(
        _data=[
            actual.get(0),
            7989,
            0,
            9182715666859759686,
            3,
            0,
            0,
            [AvroStruct(_data=[True, False, b"\x01\x00\x00\x00", b"\x02\x00\x00\x00"])],
            237993,
            0,
            0,
        ]
    )
    assert actual == expected


def test_null_list_convert_pos_to_dict() -> None:
    data = _convert_pos_to_dict(
        Schema(
            NestedField(name="field", field_id=1, field_type=ListType(element_id=2, element=StringType(), element_required=False))
        ),
        AvroStruct([None]),
    )
    assert data["field"] is None


def test_null_dict_convert_pos_to_dict() -> None:
    data = _convert_pos_to_dict(
        Schema(
            NestedField(
                name="field",
                field_id=1,
                field_type=MapType(key_id=2, key_type=StringType(), value_id=3, value_type=StringType(), value_required=False),
            )
        ),
        AvroStruct([None]),
    )
    assert data["field"] is None


def test_null_struct_convert_pos_to_dict() -> None:
    data = _convert_pos_to_dict(
        Schema(
            NestedField(
                name="field",
                field_id=1,
                field_type=StructType(
                    NestedField(2, "required_field", StringType(), True), NestedField(3, "optional_field", IntegerType())
                ),
                required=False,
            )
        ),
        AvroStruct([None]),
    )
    assert data["field"] is None


def test_fixed_reader() -> None:
    assert primitive_reader(FixedType(22)) == FixedReader(22)


def test_decimal_reader() -> None:
    assert primitive_reader(DecimalType(19, 25)) == DecimalReader(19, 25)


def test_boolean_reader() -> None:
    assert primitive_reader(BooleanType()) == BooleanReader()


def test_integer_reader() -> None:
    assert primitive_reader(IntegerType()) == IntegerReader()


def test_long_reader() -> None:
    assert primitive_reader(LongType()) == IntegerReader()


def test_float_reader() -> None:
    assert primitive_reader(FloatType()) == FloatReader()


def test_double_reader() -> None:
    assert primitive_reader(DoubleType()) == DoubleReader()


def test_date_reader() -> None:
    assert primitive_reader(DateType()) == DateReader()


def test_time_reader() -> None:
    assert primitive_reader(TimeType()) == TimeReader()


def test_timestamp_reader() -> None:
    assert primitive_reader(TimestampType()) == TimestampReader()


def test_timestamptz_reader() -> None:
    assert primitive_reader(TimestamptzType()) == TimestamptzReader()


def test_string_reader() -> None:
    assert primitive_reader(StringType()) == StringReader()


def test_binary_reader() -> None:
    assert primitive_reader(BinaryType()) == BinaryReader()


def test_unknown_type() -> None:
    class UnknownType(PrimitiveType):
        __root__ = "UnknownType"

    with pytest.raises(ValueError) as exc_info:
        primitive_reader(UnknownType())

    assert "Unknown type:" in str(exc_info.value)
