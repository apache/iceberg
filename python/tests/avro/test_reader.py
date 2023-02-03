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

from pyiceberg.avro.decoder import BinaryDecoder
from pyiceberg.avro.file import AvroFile
from pyiceberg.avro.reader import (
    BinaryReader,
    BooleanReader,
    DateReader,
    DecimalReader,
    DoubleReader,
    FixedReader,
    FloatReader,
    IntegerReader,
    StringReader,
    StructReader,
    TimeReader,
    TimestampReader,
    TimestamptzReader,
    UUIDReader,
)
from pyiceberg.avro.resolver import construct_reader
from pyiceberg.io.memory import MemoryInputStream
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import MANIFEST_ENTRY_SCHEMA, DataFile, ManifestEntry
from pyiceberg.schema import Schema
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
    LongType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)


def test_read_header(generated_manifest_entry_file: str, iceberg_manifest_entry_schema: Schema) -> None:
    with AvroFile[ManifestEntry](
        PyArrowFileIO().new_input(generated_manifest_entry_file),
        MANIFEST_ENTRY_SCHEMA,
        {-1: ManifestEntry, 2: DataFile},
    ) as reader:
        header = reader.header

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
                                "fields": [
                                    {"field-id": 1000, "default": None, "name": "VendorID", "type": ["null", "int"]},
                                    {
                                        "field-id": 1001,
                                        "default": None,
                                        "name": "tpep_pickup_datetime",
                                        "type": ["null", {"type": "int", "logicalType": "date"}],
                                    },
                                ],
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


def test_fixed_reader() -> None:
    assert construct_reader(FixedType(22)) == FixedReader(22)


def test_decimal_reader() -> None:
    assert construct_reader(DecimalType(19, 25)) == DecimalReader(19, 25)


def test_boolean_reader() -> None:
    assert construct_reader(BooleanType()) == BooleanReader()


def test_integer_reader() -> None:
    assert construct_reader(IntegerType()) == IntegerReader()


def test_long_reader() -> None:
    assert construct_reader(LongType()) == IntegerReader()


def test_float_reader() -> None:
    assert construct_reader(FloatType()) == FloatReader()


def test_double_reader() -> None:
    assert construct_reader(DoubleType()) == DoubleReader()


def test_date_reader() -> None:
    assert construct_reader(DateType()) == DateReader()


def test_time_reader() -> None:
    assert construct_reader(TimeType()) == TimeReader()


def test_timestamp_reader() -> None:
    assert construct_reader(TimestampType()) == TimestampReader()


def test_timestamptz_reader() -> None:
    assert construct_reader(TimestamptzType()) == TimestamptzReader()


def test_string_reader() -> None:
    assert construct_reader(StringType()) == StringReader()


def test_binary_reader() -> None:
    assert construct_reader(BinaryType()) == BinaryReader()


def test_unknown_type() -> None:
    class UnknownType(PrimitiveType):
        __root__ = "UnknownType"

    with pytest.raises(ValueError) as exc_info:
        construct_reader(UnknownType())

    assert "Unknown type:" in str(exc_info.value)


def test_uuid_reader() -> None:
    assert construct_reader(UUIDType()) == UUIDReader()


def test_read_struct() -> None:
    mis = MemoryInputStream(b"\x18")
    decoder = BinaryDecoder(mis)

    struct = StructType(NestedField(1, "id", IntegerType(), required=True))
    result = StructReader(((0, IntegerReader()),), Record, struct).read(decoder)
    assert repr(result) == "Record[id=12]"


def test_read_struct_lambda() -> None:
    mis = MemoryInputStream(b"\x18")
    decoder = BinaryDecoder(mis)

    struct = StructType(NestedField(1, "id", IntegerType(), required=True))
    # You can also pass in an arbitrary function that returns a struct
    result = StructReader(
        ((0, IntegerReader()),), lambda struct: Record(struct=struct), struct  # pylint: disable=unnecessary-lambda
    ).read(decoder)
    assert repr(result) == "Record[id=12]"


def test_read_not_struct_type() -> None:
    mis = MemoryInputStream(b"\x18")
    decoder = BinaryDecoder(mis)

    struct = StructType(NestedField(1, "id", IntegerType(), required=True))
    with pytest.raises(ValueError) as exc_info:
        _ = StructReader(((0, IntegerReader()),), str, struct).read(decoder)  # type: ignore

    assert "Incompatible with StructProtocol: <class 'str'>" in str(exc_info.value)


def test_read_struct_exception_handling() -> None:
    mis = MemoryInputStream(b"\x18")
    decoder = BinaryDecoder(mis)

    def raise_err(struct: StructType) -> None:
        raise TypeError("boom")

    struct = StructType(NestedField(1, "id", IntegerType(), required=True))
    # You can also pass in an arbitrary function that returns a struct

    with pytest.raises(ValueError) as exc_info:
        _ = StructReader(((0, IntegerReader()),), raise_err, struct).read(decoder)  # type: ignore

    assert "Unable to initialize struct:" in str(exc_info.value)
