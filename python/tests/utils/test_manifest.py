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
# pylint: disable=redefined-outer-name,arguments-renamed,fixme
from tempfile import TemporaryDirectory
from typing import Dict

import fastavro
import pytest

from pyiceberg.io import load_file_io
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntryStatus,
    ManifestFile,
    PartitionFieldSummary,
    read_manifest_list,
    write_manifest,
    write_manifest_list,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Snapshot
from pyiceberg.table.snapshots import Operation, Summary
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField


def _verify_metadata_with_fastavro(avro_file: str, expected_metadata: Dict[str, str]) -> None:
    with open(avro_file, "rb") as f:
        reader = fastavro.reader(f)
        metadata = reader.metadata
        for k, v in expected_metadata.items():
            assert k in metadata
            assert metadata[k] == v


def test_read_manifest_entry(generated_manifest_entry_file: str) -> None:
    manifest = ManifestFile(
        manifest_path=generated_manifest_entry_file, manifest_length=0, partition_spec_id=0, sequence_number=None, partitions=[]
    )
    manifest_entries = manifest.fetch_manifest_entry(PyArrowFileIO())
    manifest_entry = manifest_entries[0]

    assert manifest_entry.status == ManifestEntryStatus.ADDED
    assert manifest_entry.snapshot_id == 8744736658442914487
    assert manifest_entry.data_sequence_number is None
    assert isinstance(manifest_entry.data_file, DataFile)

    data_file = manifest_entry.data_file

    assert data_file.content is DataFileContent.DATA
    assert (
        data_file.file_path
        == "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=null/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00001.parquet"
    )
    assert data_file.file_format == FileFormat.PARQUET
    assert repr(data_file.partition) == "Record[VendorID=1, tpep_pickup_datetime=1925]"
    assert data_file.record_count == 19513
    assert data_file.file_size_in_bytes == 388872
    assert data_file.column_sizes == {
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
    }
    assert data_file.value_counts == {
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
    }
    assert data_file.null_value_counts == {
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
    }
    assert data_file.nan_value_counts == {16: 0, 17: 0, 18: 0, 19: 0, 10: 0, 11: 0, 12: 0, 13: 0, 14: 0, 15: 0}
    assert data_file.lower_bounds == {
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
    }
    assert data_file.upper_bounds == {
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
    }
    assert data_file.key_metadata is None
    assert data_file.split_offsets == [4]
    assert data_file.equality_ids is None
    assert data_file.sort_order_id == 0


def test_read_manifest_list(generated_manifest_file_file_v1: str) -> None:
    input_file = PyArrowFileIO().new_input(generated_manifest_file_file_v1)
    manifest_list = list(read_manifest_list(input_file))[0]

    assert manifest_list.manifest_length == 7989
    assert manifest_list.partition_spec_id == 0
    assert manifest_list.added_snapshot_id == 9182715666859759686
    assert manifest_list.added_files_count == 3
    assert manifest_list.existing_files_count == 0
    assert manifest_list.deleted_files_count == 0

    assert isinstance(manifest_list.partitions, list)

    partitions_summary = manifest_list.partitions[0]
    assert isinstance(partitions_summary, PartitionFieldSummary)

    assert partitions_summary.contains_null is True
    assert partitions_summary.contains_nan is False
    assert partitions_summary.lower_bound == b"\x01\x00\x00\x00"
    assert partitions_summary.upper_bound == b"\x02\x00\x00\x00"

    assert manifest_list.added_rows_count == 237993
    assert manifest_list.existing_rows_count == 0
    assert manifest_list.deleted_rows_count == 0


def test_read_manifest_v1(generated_manifest_file_file_v1: str) -> None:
    io = load_file_io()

    snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        timestamp_ms=1602638573590,
        manifest_list=generated_manifest_file_file_v1,
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )
    manifest_list = snapshot.manifests(io)[0]

    assert manifest_list.manifest_length == 7989
    assert manifest_list.partition_spec_id == 0
    assert manifest_list.content == ManifestContent.DATA
    assert manifest_list.sequence_number == 0
    assert manifest_list.min_sequence_number == 0
    assert manifest_list.added_snapshot_id == 9182715666859759686
    assert manifest_list.added_files_count == 3
    assert manifest_list.existing_files_count == 0
    assert manifest_list.deleted_files_count == 0
    assert manifest_list.added_rows_count == 237993
    assert manifest_list.existing_rows_count == 0
    assert manifest_list.deleted_rows_count == 0
    assert manifest_list.key_metadata is None

    assert isinstance(manifest_list.partitions, list)

    partition = manifest_list.partitions[0]

    assert isinstance(partition, PartitionFieldSummary)

    assert partition.contains_null is True
    assert partition.contains_nan is False
    assert partition.lower_bound == b"\x01\x00\x00\x00"
    assert partition.upper_bound == b"\x02\x00\x00\x00"

    entries = manifest_list.fetch_manifest_entry(io)

    assert isinstance(entries, list)

    entry = entries[0]

    assert entry.data_sequence_number == 0
    assert entry.file_sequence_number == 0
    assert entry.snapshot_id == 8744736658442914487
    assert entry.status == ManifestEntryStatus.ADDED


def test_read_manifest_v2(generated_manifest_file_file_v2: str) -> None:
    io = load_file_io()

    snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        timestamp_ms=1602638573590,
        manifest_list=generated_manifest_file_file_v2,
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )
    manifest_list = snapshot.manifests(io)[0]

    assert manifest_list.manifest_length == 7989
    assert manifest_list.partition_spec_id == 0
    assert manifest_list.content == ManifestContent.DELETES
    assert manifest_list.sequence_number == 3
    assert manifest_list.min_sequence_number == 3
    assert manifest_list.added_snapshot_id == 9182715666859759686
    assert manifest_list.added_files_count == 3
    assert manifest_list.existing_files_count == 0
    assert manifest_list.deleted_files_count == 0
    assert manifest_list.added_rows_count == 237993
    assert manifest_list.existing_rows_count == 0
    assert manifest_list.deleted_rows_count == 0
    assert manifest_list.key_metadata is None

    assert isinstance(manifest_list.partitions, list)

    partition = manifest_list.partitions[0]

    assert isinstance(partition, PartitionFieldSummary)

    assert partition.contains_null is True
    assert partition.contains_nan is False
    assert partition.lower_bound == b"\x01\x00\x00\x00"
    assert partition.upper_bound == b"\x02\x00\x00\x00"

    entries = manifest_list.fetch_manifest_entry(io)

    assert isinstance(entries, list)

    entry = entries[0]

    assert entry.data_sequence_number == 3
    assert entry.file_sequence_number == 3
    assert entry.snapshot_id == 8744736658442914487
    assert entry.status == ManifestEntryStatus.ADDED


@pytest.mark.parametrize("format_version", [1, 2])
def test_write_manifest(generated_manifest_file_file_v1: str, generated_manifest_file_file_v2: str, format_version: int) -> None:
    io = load_file_io()
    snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        timestamp_ms=1602638573590,
        manifest_list=generated_manifest_file_file_v1 if format_version == 1 else generated_manifest_file_file_v2,
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )
    demo_manifest_file = snapshot.manifests(io)[0]
    manifest_entries = demo_manifest_file.fetch_manifest_entry(io)
    test_schema = Schema(
        NestedField(1, "VendorID", IntegerType(), False), NestedField(2, "tpep_pickup_datetime", IntegerType(), False)
    )
    test_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1, transform=IdentityTransform(), name="VendorID"),
        PartitionField(source_id=2, field_id=2, transform=IdentityTransform(), name="tpep_pickup_datetime"),
        spec_id=demo_manifest_file.partition_spec_id,
    )
    with TemporaryDirectory() as tmpdir:
        tmp_avro_file = tmpdir + "/test_write_manifest.avro"
        output = io.new_output(tmp_avro_file)
        with write_manifest(
            format_version=format_version,  # type: ignore
            spec=test_spec,
            schema=test_schema,
            output_file=output,
            snapshot_id=8744736658442914487,
        ) as writer:
            for entry in manifest_entries:
                writer.add_entry(entry)
            new_manifest = writer.to_manifest_file()
            with pytest.raises(RuntimeError):
                writer.add_entry(manifest_entries[0])

        expected_metadata = {
            "schema": test_schema.json(),
            "partition-spec": test_spec.json(),
            "partition-spec-id": str(test_spec.spec_id),
            "format-version": str(format_version),
        }
        if format_version == 2:
            expected_metadata["content"] = "data"
        _verify_metadata_with_fastavro(
            tmp_avro_file,
            expected_metadata,
        )
        new_manifest_entries = new_manifest.fetch_manifest_entry(io)

        manifest_entry = new_manifest_entries[0]

        assert manifest_entry.status == ManifestEntryStatus.ADDED
        assert manifest_entry.snapshot_id == 8744736658442914487
        assert manifest_entry.data_sequence_number == 0 if format_version == 1 else 3
        assert isinstance(manifest_entry.data_file, DataFile)

        data_file = manifest_entry.data_file

        assert data_file.content is DataFileContent.DATA
        assert (
            data_file.file_path
            == "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=null/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00001.parquet"
        )
        assert data_file.file_format == FileFormat.PARQUET
        assert data_file.partition == Record(VendorID=1, tpep_pickup_datetime=1925)
        assert data_file.record_count == 19513
        assert data_file.file_size_in_bytes == 388872
        if format_version == 1:
            assert data_file.block_size_in_bytes == 67108864
        else:
            assert data_file.block_size_in_bytes is None
        assert data_file.column_sizes == {
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
        }
        assert data_file.value_counts == {
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
        }
        assert data_file.null_value_counts == {
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
        }
        assert data_file.nan_value_counts == {16: 0, 17: 0, 18: 0, 19: 0, 10: 0, 11: 0, 12: 0, 13: 0, 14: 0, 15: 0}
        assert data_file.lower_bounds == {
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
        }
        assert data_file.upper_bounds == {
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
        }
        assert data_file.key_metadata is None
        assert data_file.split_offsets == [4]
        assert data_file.equality_ids is None
        assert data_file.sort_order_id == 0


@pytest.mark.parametrize("format_version", [1, 2])
def test_write_manifest_list(
    generated_manifest_file_file_v1: str, generated_manifest_file_file_v2: str, format_version: int
) -> None:
    io = load_file_io()

    snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        timestamp_ms=1602638573590,
        manifest_list=generated_manifest_file_file_v1 if format_version == 1 else generated_manifest_file_file_v2,
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )

    demo_manifest_list = snapshot.manifests(io)
    with TemporaryDirectory() as tmp_dir:
        path = tmp_dir + "/manifest-list.avro"
        output = io.new_output(path)
        with write_manifest_list(
            format_version=format_version, output_file=output, snapshot_id=25, parent_snapshot_id=19, sequence_number=0  # type: ignore
        ) as writer:
            writer.add_manifests(demo_manifest_list)
        new_manifest_list = list(read_manifest_list(io.new_input(path)))

        expected_metadata = {"snapshot-id": "25", "parent-snapshot-id": "19", "format-version": str(format_version)}
        if format_version == 2:
            expected_metadata["sequence-number"] = "0"
        _verify_metadata_with_fastavro(path, expected_metadata)

        manifest_file = new_manifest_list[0]

        assert manifest_file.manifest_length == 7989
        assert manifest_file.partition_spec_id == 0
        assert manifest_file.content == ManifestContent.DATA if format_version == 1 else ManifestContent.DELETES
        assert manifest_file.sequence_number == 0 if format_version == 1 else 3
        assert manifest_file.min_sequence_number == 0 if format_version == 1 else 3
        assert manifest_file.added_snapshot_id == 9182715666859759686
        assert manifest_file.added_files_count == 3
        assert manifest_file.existing_files_count == 0
        assert manifest_file.deleted_files_count == 0
        assert manifest_file.added_rows_count == 237993
        assert manifest_file.existing_rows_count == 0
        assert manifest_file.deleted_rows_count == 0
        assert manifest_file.key_metadata is None

        assert isinstance(manifest_file.partitions, list)

        partition = manifest_file.partitions[0]

        assert isinstance(partition, PartitionFieldSummary)

        assert partition.contains_null is True
        assert partition.contains_nan is False
        assert partition.lower_bound == b"\x01\x00\x00\x00"
        assert partition.upper_bound == b"\x02\x00\x00\x00"

        entries = manifest_file.fetch_manifest_entry(io)

        assert isinstance(entries, list)

        entry = entries[0]

        assert entry.data_sequence_number == 0 if format_version == 1 else 3
        assert entry.file_sequence_number == 0 if format_version == 1 else 3
        assert entry.snapshot_id == 8744736658442914487
        assert entry.status == ManifestEntryStatus.ADDED
