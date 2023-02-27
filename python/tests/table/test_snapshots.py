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
# pylint:disable=redefined-outer-name,eval-used
import pytest

from pyiceberg.io import load_file_io
from pyiceberg.manifest import DataFile
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table, SortOrder
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.table.refs import SnapshotRefType, SnapshotRef
from pyiceberg.table.snapshots import (
    Operation,
    Snapshot,
    Summary,
    is_parent_ancestor_of,
    is_ancestor_of,
    current_ancestors,
    current_ancestor_ids,
    oldest_ancestor,
    oldest_ancestor_of,
    oldest_ancestor_after,
    snapshots_ids_between,
    ancestor_ids,
    ancestors_between,
    new_files,
    snapshot_after,
    snapshot_id_as_of_time,
    schema_for,
    schema_as_of_time,
)
from pyiceberg.typedef import Record


@pytest.fixture
def snapshot() -> Snapshot:
    return Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        sequence_number=200,
        timestamp_ms=1602638573590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )


@pytest.fixture
def snapshot_with_properties() -> Snapshot:
    return Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        sequence_number=200,
        timestamp_ms=1602638573590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND, foo="bar"),
        schema_id=3,
    )


SNAPSHOT_A_ID = 0xA
SNAPSHOT_A_TIME = 1677222221000

SNAPSHOT_B_ID = 0xB
SNAPSHOT_B_TIME = 1677222222000

SNAPSHOT_C_ID = 0xC
SNAPSHOT_C_TIME = 1677222223000

SNAPSHOT_D_ID = 0xD
SNAPSHOT_D_TIME = 1677222224000


@pytest.fixture
def table_with_snapshots(table_schema_simple: Schema) -> Table:
    return Table(
        identifier=("database", "table"),
        metadata=TableMetadataV2(
            location="s3://bucket/warehouse/table/",
            table_uuid="9c12d441-03fe-4693-9a96-a0705ddf69c1",
            last_updated_ms=1602638573590,
            last_column_id=3,
            schemas=[table_schema_simple],
            current_schema_id=1,
            last_partition_id=1000,
            properties={"owner": "javaberg"},
            partition_specs=[PartitionSpec()],
            default_spec_id=0,
            current_snapshot_id=SNAPSHOT_D_ID,
            snapshots=[
                Snapshot(
                    snapshot_id=SNAPSHOT_A_ID,
                    parent_snapshot_id=None,
                    sequence_number=1,
                    timestamp_ms=SNAPSHOT_A_TIME,
                    manifest_list="s3://bucket/table/snapshot-a.avro",
                ),
                Snapshot(
                    snapshot_id=SNAPSHOT_B_ID,
                    parent_snapshot_id=SNAPSHOT_A_ID,
                    sequence_number=2,
                    timestamp_ms=SNAPSHOT_B_TIME,
                    manifest_list="s3://bucket/table/snapshot-b.avro",
                ),
                Snapshot(
                    snapshot_id=SNAPSHOT_D_ID,
                    parent_snapshot_id=SNAPSHOT_B_ID,
                    sequence_number=3,
                    timestamp_ms=SNAPSHOT_D_TIME,
                    manifest_list="s3://bucket/table/snapshot-c.avro",
                ),
                Snapshot(
                    snapshot_id=SNAPSHOT_C_ID,
                    parent_snapshot_id=SNAPSHOT_A_ID,
                    sequence_number=4,
                    timestamp_ms=SNAPSHOT_C_TIME,
                    manifest_list="s3://bucket/table/snapshot1.avro",
                ),
            ],
            snapshot_log=[],
            metadata_log=[],
            sort_orders=[SortOrder(order_id=0)],
            default_sort_order_id=0,
            refs={"b1": SnapshotRef(snapshot_id=SNAPSHOT_C_ID, snapshot_ref_type=SnapshotRefType.BRANCH)},
            format_version=2,
            last_sequence_number=0,
        ),
        metadata_location="s3://bucket/table/metadata.json",
        io=None,
    )


def test_serialize_summary() -> None:
    assert Summary(Operation.APPEND).json() == """{"operation": "append"}"""


def test_serialize_summary_with_properties() -> None:
    assert Summary(Operation.APPEND, property="yes").json() == """{"operation": "append", "property": "yes"}"""


def test_serialize_snapshot(snapshot: Snapshot) -> None:
    assert (
        snapshot.json()
        == """{"snapshot-id": 25, "parent-snapshot-id": 19, "sequence-number": 200, "timestamp-ms": 1602638573590, "manifest-list": "s3:/a/b/c.avro", "summary": {"operation": "append"}, "schema-id": 3}"""
    )


def test_serialize_snapshot_without_sequence_number() -> None:
    snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        timestamp_ms=1602638573590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )
    actual = snapshot.json()
    expected = """{"snapshot-id": 25, "parent-snapshot-id": 19, "timestamp-ms": 1602638573590, "manifest-list": "s3:/a/b/c.avro", "summary": {"operation": "append"}, "schema-id": 3}"""
    assert actual == expected


def test_serialize_snapshot_with_properties(snapshot_with_properties: Snapshot) -> None:
    assert (
        snapshot_with_properties.json()
        == """{"snapshot-id": 25, "parent-snapshot-id": 19, "sequence-number": 200, "timestamp-ms": 1602638573590, "manifest-list": "s3:/a/b/c.avro", "summary": {"operation": "append", "foo": "bar"}, "schema-id": 3}"""
    )


def test_deserialize_summary() -> None:
    summary = Summary.parse_raw("""{"operation": "append"}""")
    assert summary.operation == Operation.APPEND


def test_deserialize_summary_with_properties() -> None:
    summary = Summary.parse_raw("""{"operation": "append", "property": "yes"}""")
    assert summary.operation == Operation.APPEND
    assert summary.additional_properties == {"property": "yes"}


def test_deserialize_snapshot(snapshot: Snapshot) -> None:
    payload = """{"snapshot-id": 25, "parent-snapshot-id": 19, "sequence-number": 200, "timestamp-ms": 1602638573590, "manifest-list": "s3:/a/b/c.avro", "summary": {"operation": "append"}, "schema-id": 3}"""
    actual = Snapshot.parse_raw(payload)
    assert actual == snapshot


def test_deserialize_snapshot_with_properties(snapshot_with_properties: Snapshot) -> None:
    payload = """{"snapshot-id": 25, "parent-snapshot-id": 19, "sequence-number": 200, "timestamp-ms": 1602638573590, "manifest-list": "s3:/a/b/c.avro", "summary": {"operation": "append", "foo": "bar"}, "schema-id": 3}"""
    snapshot = Snapshot.parse_raw(payload)
    assert snapshot == snapshot_with_properties


def test_snapshot_repr(snapshot: Snapshot) -> None:
    assert (
        repr(snapshot)
        == """Snapshot(snapshot_id=25, parent_snapshot_id=19, sequence_number=200, timestamp_ms=1602638573590, manifest_list='s3:/a/b/c.avro', summary=Summary(Operation.APPEND), schema_id=3)"""
    )
    assert snapshot == eval(repr(snapshot))


def test_snapshot_with_properties_repr(snapshot_with_properties: Snapshot) -> None:
    assert (
        repr(snapshot_with_properties)
        == """Snapshot(snapshot_id=25, parent_snapshot_id=19, sequence_number=200, timestamp_ms=1602638573590, manifest_list='s3:/a/b/c.avro', summary=Summary(Operation.APPEND, **{'foo': 'bar'}), schema_id=3)"""
    )
    assert snapshot_with_properties == eval(repr(snapshot_with_properties))


def test_is_ancestor_of(table_with_snapshots: Table):
    assert is_ancestor_of(table_with_snapshots, SNAPSHOT_B_ID, SNAPSHOT_A_ID)
    assert not is_ancestor_of(table_with_snapshots, SNAPSHOT_C_ID, SNAPSHOT_B_ID)


def test_is_parent_ancestor_of(table_with_snapshots: Table):
    assert is_parent_ancestor_of(table_with_snapshots, SNAPSHOT_B_ID, SNAPSHOT_A_ID)
    assert not is_parent_ancestor_of(table_with_snapshots, SNAPSHOT_C_ID, SNAPSHOT_B_ID)


def test_current_ancestors(table_with_snapshots: Table):
    assert list(current_ancestors(table_with_snapshots)) == [
        Snapshot(
            snapshot_id=13,
            parent_snapshot_id=11,
            sequence_number=3,
            timestamp_ms=1677222224000,
            manifest_list="s3://bucket/table/snapshot-c.avro",
            summary=None,
            schema_id=None,
        ),
        Snapshot(
            snapshot_id=11,
            parent_snapshot_id=10,
            sequence_number=2,
            timestamp_ms=1677222222000,
            manifest_list="s3://bucket/table/snapshot-b.avro",
            summary=None,
            schema_id=None,
        ),
        Snapshot(
            snapshot_id=10,
            parent_snapshot_id=None,
            sequence_number=1,
            timestamp_ms=1677222221000,
            manifest_list="s3://bucket/table/snapshot-a.avro",
            summary=None,
            schema_id=None,
        ),
    ]


def test_current_ancestors_ids(table_with_snapshots: Table):
    assert list(current_ancestor_ids(table_with_snapshots)) == [13, 11, 10]


def test_oldest_ancestor(table_with_snapshots: Table):
    assert oldest_ancestor(table_with_snapshots) == Snapshot(
        snapshot_id=10,
        parent_snapshot_id=None,
        sequence_number=1,
        timestamp_ms=1677222221000,
        manifest_list="s3://bucket/table/snapshot-a.avro",
        summary=None,
        schema_id=None,
    )


def test_oldest_ancestor_of(table_with_snapshots: Table):
    assert oldest_ancestor_of(table_with_snapshots, SNAPSHOT_B_ID) == Snapshot(
        snapshot_id=10,
        parent_snapshot_id=None,
        sequence_number=1,
        timestamp_ms=1677222221000,
        manifest_list="s3://bucket/table/snapshot-a.avro",
        summary=None,
        schema_id=None,
    )


def test_oldest_ancestor_after(table_with_snapshots: Table):
    assert oldest_ancestor_after(table_with_snapshots, SNAPSHOT_B_TIME) == Snapshot(
        snapshot_id=11,
        parent_snapshot_id=10,
        sequence_number=2,
        timestamp_ms=1677222222000,
        manifest_list="s3://bucket/table/snapshot-b.avro",
        summary=None,
        schema_id=None,
    )


def test_snapshots_ids_between(table_with_snapshots: Table):
    assert list(snapshots_ids_between(table_with_snapshots, SNAPSHOT_A_ID, SNAPSHOT_D_ID)) == [SNAPSHOT_D_ID, SNAPSHOT_B_ID]


def test_ancestor_ids(table_with_snapshots: Table):
    assert list(ancestor_ids(SNAPSHOT_B_ID, table_with_snapshots.snapshot_by_id)) == [SNAPSHOT_B_ID, SNAPSHOT_A_ID]


def test_ancestors_between(table_with_snapshots: Table):
    assert list(ancestors_between(SNAPSHOT_D_ID, SNAPSHOT_A_ID, table_with_snapshots.snapshot_by_id)) == [
        Snapshot(
            snapshot_id=13,
            parent_snapshot_id=11,
            sequence_number=3,
            timestamp_ms=1677222224000,
            manifest_list="s3://bucket/table/snapshot-c.avro",
            summary=None,
            schema_id=None,
        ),
        Snapshot(
            snapshot_id=11,
            parent_snapshot_id=10,
            sequence_number=2,
            timestamp_ms=1677222222000,
            manifest_list="s3://bucket/table/snapshot-b.avro",
            summary=None,
            schema_id=None,
        ),
    ]

    assert list(ancestors_between(SNAPSHOT_D_ID, None, table_with_snapshots.snapshot_by_id)) == [
        Snapshot(
            snapshot_id=13,
            parent_snapshot_id=11,
            sequence_number=3,
            timestamp_ms=1677222224000,
            manifest_list="s3://bucket/table/snapshot-c.avro",
            summary=None,
            schema_id=None,
        ),
        Snapshot(
            snapshot_id=11,
            parent_snapshot_id=10,
            sequence_number=2,
            timestamp_ms=1677222222000,
            manifest_list="s3://bucket/table/snapshot-b.avro",
            summary=None,
            schema_id=None,
        ),
        Snapshot(
            snapshot_id=10,
            parent_snapshot_id=None,
            sequence_number=1,
            timestamp_ms=1677222221000,
            manifest_list="s3://bucket/table/snapshot-a.avro",
            summary=None,
            schema_id=None,
        ),
    ]


def test_new_files(table_schema_simple: Schema, generated_manifest_file_file: str):
    io = load_file_io({})
    table = Table(
        identifier=("database", "table"),
        metadata=TableMetadataV2(
            location="s3://bucket/warehouse/table/",
            table_uuid="9c12d441-03fe-4693-9a96-a0705ddf69c1",
            last_updated_ms=1602638573590,
            last_column_id=3,
            schemas=[table_schema_simple],
            current_schema_id=1,
            last_partition_id=1000,
            properties={"owner": "javaberg"},
            partition_specs=[PartitionSpec()],
            default_spec_id=0,
            current_snapshot_id=SNAPSHOT_D_ID,
            snapshots=[
                Snapshot(
                    snapshot_id=SNAPSHOT_A_ID,
                    parent_snapshot_id=None,
                    sequence_number=1,
                    timestamp_ms=SNAPSHOT_A_TIME,
                    manifest_list=generated_manifest_file_file,
                ),
                Snapshot(
                    snapshot_id=SNAPSHOT_B_ID,
                    parent_snapshot_id=SNAPSHOT_A_ID,
                    sequence_number=2,
                    timestamp_ms=SNAPSHOT_B_TIME,
                    manifest_list=generated_manifest_file_file,
                ),
                Snapshot(
                    snapshot_id=SNAPSHOT_D_ID,
                    parent_snapshot_id=SNAPSHOT_B_ID,
                    sequence_number=3,
                    timestamp_ms=SNAPSHOT_D_TIME,
                    manifest_list=generated_manifest_file_file,
                ),
                Snapshot(
                    snapshot_id=SNAPSHOT_C_ID,
                    parent_snapshot_id=SNAPSHOT_A_ID,
                    sequence_number=4,
                    timestamp_ms=SNAPSHOT_C_TIME,
                    manifest_list=generated_manifest_file_file,
                ),
            ],
            snapshot_log=[],
            metadata_log=[],
            sort_orders=[SortOrder(order_id=0)],
            default_sort_order_id=0,
            refs={"b1": SnapshotRef(snapshot_id=SNAPSHOT_C_ID, snapshot_ref_type=SnapshotRefType.BRANCH)},
            format_version=2,
            last_sequence_number=0,
        ),
        metadata_location="s3://bucket/table/metadata.json",
        io=io,
    )

    files = list(new_files(SNAPSHOT_A_ID, SNAPSHOT_D_ID, table.snapshot_by_id, io))

    assert len(files) == 4
    assert (
        files[0].file_path
        == "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=null/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00001.parquet"
    )
    assert (
        files[1].file_path
        == "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=1/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00002.parquet"
    )
    assert (
        files[2].file_path
        == "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=null/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00001.parquet"
    )
    assert (
        files[3].file_path
        == "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=1/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00002.parquet"
    )


def test_snapshot_after(table_with_snapshots: Table):
    assert snapshot_after(table_with_snapshots, SNAPSHOT_B_ID).snapshot_id == SNAPSHOT_D_ID


def test_snapshot_id_as_of_time(table_with_snapshots: Table):
    assert snapshot_id_as_of_time(table_with_snapshots, SNAPSHOT_B_TIME) == SNAPSHOT_A_ID


def test_schema_for(table_with_snapshots: Table, table_schema_simple: Schema):
    assert schema_for(table_with_snapshots, SNAPSHOT_A_ID) == table_schema_simple


def test_schema_as_of_time(table_with_snapshots: Table, table_schema_simple: Schema):
    assert schema_as_of_time(table_with_snapshots, SNAPSHOT_B_TIME) == table_schema_simple
