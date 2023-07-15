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
from enum import Enum
from tempfile import TemporaryDirectory
from typing import Any

import pytest
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
from pyiceberg.typedef import Record
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
