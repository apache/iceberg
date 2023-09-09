#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
import inspect
from datetime import date, datetime, time
from enum import Enum
from tempfile import TemporaryDirectory
from typing import Any
from uuid import UUID

import pytest
from _decimal import Decimal
from fastavro import reader, writer

import pyiceberg.avro.file as avro
from pyiceberg.avro.codecs import DeflateCodec
from pyiceberg.avro.file import META_SCHEMA, AvroFileHeader
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    MANIFEST_ENTRY_SCHEMA,
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestEntry,
    ManifestEntryStatus,
)
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)
from pyiceberg.utils.schema_conversion import AvroSchemaConversion


def get_deflate_compressor() -> None:
    header = AvroFileHeader(struct=META_SCHEMA)
    header[0] = bytes(0)
    header[1] = {"avro.codec": "deflate"}
    header[2] = bytes(16)
    assert header.compression_codec() == DeflateCodec


def get_null_compressor() -> None:
    header = AvroFileHeader(struct=META_SCHEMA)
    header[0] = bytes(0)
    header[1] = {"avro.codec": "null"}
    header[2] = bytes(16)
    assert header.compression_codec() is None


def test_unknown_codec() -> None:
    header = AvroFileHeader(struct=META_SCHEMA)
    header[0] = bytes(0)
    header[1] = {"avro.codec": "unknown"}
    header[2] = bytes(16)

    with pytest.raises(ValueError) as exc_info:
        header.compression_codec()

    assert "Unsupported codec: unknown" in str(exc_info.value)


def test_missing_schema() -> None:
    header = AvroFileHeader(struct=META_SCHEMA)
    header[0] = bytes(0)
    header[1] = {}
    header[2] = bytes(16)

    with pytest.raises(ValueError) as exc_info:
        header.get_schema()

    assert "No schema found in Avro file headers" in str(exc_info.value)


# helper function to serialize our objects to dicts to enable
# direct comparison with the dicts returned by fastavro
def todict(obj: Any) -> Any:
    if isinstance(obj, dict):
        data = []
        for k, v in obj.items():
            data.append({"key": k, "value": v})
        return data
    elif isinstance(obj, Enum):
        return obj.value
    elif hasattr(obj, "__iter__") and not isinstance(obj, str) and not isinstance(obj, bytes):
        return [todict(v) for v in obj]
    elif hasattr(obj, "__dict__"):
        return {key: todict(value) for key, value in inspect.getmembers(obj) if not callable(value) and not key.startswith("_")}
    else:
        return obj


def test_write_manifest_entry_with_iceberg_read_with_fastavro() -> None:
    data_file = DataFile(
        content=DataFileContent.DATA,
        file_path="s3://some-path/some-file.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=131327,
        file_size_in_bytes=220669226,
        column_sizes={1: 220661854},
        value_counts={1: 131327},
        null_value_counts={1: 0},
        nan_value_counts={},
        lower_bounds={1: b"aaaaaaaaaaaaaaaa"},
        upper_bounds={1: b"zzzzzzzzzzzzzzzz"},
        key_metadata=b"\xde\xad\xbe\xef",
        split_offsets=[4, 133697593],
        equality_ids=[],
        sort_order_id=4,
        spec_id=3,
    )
    entry = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        snapshot_id=8638475580105682862,
        data_sequence_number=0,
        file_sequence_number=0,
        data_file=data_file,
    )

    additional_metadata = {"foo": "bar"}

    with TemporaryDirectory() as tmpdir:
        tmp_avro_file = tmpdir + "/manifest_entry.avro"

        with avro.AvroOutputFile[ManifestEntry](
            PyArrowFileIO().new_output(tmp_avro_file), MANIFEST_ENTRY_SCHEMA, "manifest_entry", additional_metadata
        ) as out:
            out.write_block([entry])

        with open(tmp_avro_file, "rb") as fo:
            r = reader(fo=fo)

            for k, v in additional_metadata.items():
                assert k in r.metadata
                assert v == r.metadata[k]

            it = iter(r)

            fa_entry = next(it)

        assert todict(entry) == fa_entry


def test_write_manifest_entry_with_fastavro_read_with_iceberg() -> None:
    data_file = DataFile(
        content=DataFileContent.DATA,
        file_path="s3://some-path/some-file.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=131327,
        file_size_in_bytes=220669226,
        column_sizes={1: 220661854},
        value_counts={1: 131327},
        null_value_counts={1: 0},
        nan_value_counts={},
        lower_bounds={1: b"aaaaaaaaaaaaaaaa"},
        upper_bounds={1: b"zzzzzzzzzzzzzzzz"},
        key_metadata=b"\xde\xad\xbe\xef",
        split_offsets=[4, 133697593],
        equality_ids=[],
        sort_order_id=4,
        spec_id=3,
    )
    entry = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        snapshot_id=8638475580105682862,
        data_sequence_number=0,
        file_sequence_number=0,
        data_file=data_file,
    )

    with TemporaryDirectory() as tmpdir:
        tmp_avro_file = tmpdir + "/manifest_entry.avro"

        schema = AvroSchemaConversion().iceberg_to_avro(MANIFEST_ENTRY_SCHEMA, schema_name="manifest_entry")

        with open(tmp_avro_file, "wb") as out:
            writer(out, schema, [todict(entry)])

        with avro.AvroFile[ManifestEntry](
            PyArrowFileIO().new_input(tmp_avro_file),
            MANIFEST_ENTRY_SCHEMA,
            {-1: ManifestEntry, 2: DataFile},
        ) as avro_reader:
            it = iter(avro_reader)
            avro_entry = next(it)

            assert entry == avro_entry


@pytest.mark.parametrize("is_required", [True, False])
def test_all_primitive_types(is_required: bool) -> None:
    all_primitives_schema = Schema(
        NestedField(field_id=1, name="field_fixed", field_type=FixedType(16), required=is_required),
        NestedField(field_id=2, name="field_decimal", field_type=DecimalType(6, 2), required=is_required),
        NestedField(field_id=3, name="field_bool", field_type=BooleanType(), required=is_required),
        NestedField(field_id=4, name="field_int", field_type=IntegerType(), required=True),
        NestedField(field_id=5, name="field_long", field_type=LongType(), required=is_required),
        NestedField(field_id=6, name="field_float", field_type=FloatType(), required=is_required),
        NestedField(field_id=7, name="field_double", field_type=DoubleType(), required=is_required),
        NestedField(field_id=8, name="field_date", field_type=DateType(), required=is_required),
        NestedField(field_id=9, name="field_time", field_type=TimeType(), required=is_required),
        NestedField(field_id=10, name="field_timestamp", field_type=TimestampType(), required=is_required),
        NestedField(field_id=11, name="field_timestamptz", field_type=TimestamptzType(), required=is_required),
        NestedField(field_id=12, name="field_string", field_type=StringType(), required=is_required),
        NestedField(field_id=13, name="field_uuid", field_type=UUIDType(), required=is_required),
        schema_id=1,
    )

    class AllPrimitivesRecord(Record):
        field_fixed: bytes
        field_decimal: Decimal
        field_bool: bool
        field_int: int
        field_long: int
        field_float: float
        field_double: float
        field_date: date
        field_time: time
        field_timestamp: datetime
        field_timestamptz: datetime
        field_string: str
        field_uuid: UUID

        def __init__(self, *data: Any, **named_data: Any) -> None:
            super().__init__(*data, **{"struct": all_primitives_schema.as_struct(), **named_data})

    record = AllPrimitivesRecord(
        b"\x124Vx\x124Vx\x124Vx\x124Vx",
        Decimal("123.45"),
        True,
        123,
        429496729622,
        123.22000122070312,
        429496729622.314,
        19052,
        69922000000,
        1677629965000000,
        1677629965000000,
        "this is a sentence",
        UUID("12345678-1234-5678-1234-567812345678"),
    )

    with TemporaryDirectory() as tmpdir:
        tmp_avro_file = tmpdir + "/all_primitives.avro"
        # write to disk
        with avro.AvroOutputFile[AllPrimitivesRecord](
            PyArrowFileIO().new_output(tmp_avro_file), all_primitives_schema, "all_primitives_schema"
        ) as out:
            out.write_block([record])

        # read from disk
        with avro.AvroFile[AllPrimitivesRecord](
            PyArrowFileIO().new_input(tmp_avro_file),
            all_primitives_schema,
            {-1: AllPrimitivesRecord},
        ) as avro_reader:
            it = iter(avro_reader)
            avro_entry = next(it)

    for idx, field in enumerate(all_primitives_schema.as_struct()):
        assert record[idx] == avro_entry[idx], f"Invalid {field}"
