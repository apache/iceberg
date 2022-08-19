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
# pylint:disable=redefined-outer-name
from typing import Any, Dict

import pytest

from pyiceberg.schema import Schema
from pyiceberg.table import PartitionSpec, Table, TableMetadataV2
from pyiceberg.table.partitioning import PartitionField
from pyiceberg.table.snapshots import (
    Operation,
    Snapshot,
    SnapshotLogEntry,
    Summary,
)
from pyiceberg.table.sorting import (
    NullOrder,
    SortDirection,
    SortField,
    SortOrder,
)
from pyiceberg.transforms import BucketTransform, IdentityTransform
from pyiceberg.types import LongType, NestedField


@pytest.fixture
def table(example_table_metadata_v2: Dict[str, Any]) -> Table:
    table_metadata = TableMetadataV2(**example_table_metadata_v2)
    return Table(
        identifier=("database", "table"),
        metadata=table_metadata,
        metadata_location=f"{table_metadata.location}/uuid.metadata.json",
    )


def test_schema(table):
    assert table.schema == Schema(
        fields=(
            NestedField(field_id=1, name="x", field_type=LongType(), required=True),
            NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
            NestedField(field_id=3, name="z", field_type=LongType(), required=True),
        ),
        schema_id=1,
        identifier_field_ids=[1, 2],
    )


def test_schemas(table):
    assert table.schemas == {
        0: Schema(
            fields=(NestedField(field_id=1, name="x", field_type=LongType(), required=True),),
            schema_id=0,
            identifier_field_ids=[],
        ),
        1: Schema(
            fields=(
                NestedField(field_id=1, name="x", field_type=LongType(), required=True),
                NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
                NestedField(field_id=3, name="z", field_type=LongType(), required=True),
            ),
            schema_id=1,
            identifier_field_ids=[1, 2],
        ),
    }


def test_spec(table):
    assert table.spec == PartitionSpec(
        spec_id=0, fields=(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="x"),)
    )


def test_specs(table):
    assert table.specs == {
        0: PartitionSpec(spec_id=0, fields=(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="x"),))
    }


def test_sort_order(table):
    assert table.sort_order == SortOrder(
        order_id=3,
        fields=[
            SortField(source_id=2, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),
            SortField(
                source_id=3,
                transform=BucketTransform(num_buckets=4),
                direction=SortDirection.DESC,
                null_order=NullOrder.NULLS_LAST,
            ),
        ],
    )


def test_sort_orders(table):
    assert table.sort_orders == {
        3: SortOrder(
            order_id=3,
            fields=[
                SortField(
                    source_id=2, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST
                ),
                SortField(
                    source_id=3,
                    transform=BucketTransform(num_buckets=4),
                    direction=SortDirection.DESC,
                    null_order=NullOrder.NULLS_LAST,
                ),
            ],
        )
    }


def test_location(table):
    assert table.location == "s3://bucket/test/location"


def test_current_snapshot(table):
    assert table.current_snapshot == Snapshot(
        snapshot_id=3055729675574597004,
        parent_snapshot_id=3051729675574597004,
        sequence_number=1,
        timestamp_ms=1555100955770,
        manifest_list="s3://a/b/2.avro",
        summary=Summary(operation=Operation.APPEND),
        schema_id=1,
    )


def test_snapshot_by_id(table):
    assert table.snapshot_by_id(3055729675574597004) == Snapshot(
        snapshot_id=3055729675574597004,
        parent_snapshot_id=3051729675574597004,
        sequence_number=1,
        timestamp_ms=1555100955770,
        manifest_list="s3://a/b/2.avro",
        summary=Summary(operation=Operation.APPEND),
        schema_id=1,
    )


def test_snapshot_by_id_does_not_exist(table):
    assert table.snapshot_by_id(-1) is None


def test_snapshot_by_name(table):
    assert table.snapshot_by_name("test") == Snapshot(
        snapshot_id=3051729675574597004,
        parent_snapshot_id=None,
        sequence_number=0,
        timestamp_ms=1515100955770,
        manifest_list="s3://a/b/1.avro",
        summary=Summary(operation=Operation.APPEND),
        schema_id=None,
    )


def test_snapshot_by_name_does_not_exist(table):
    assert table.snapshot_by_name("doesnotexist") is None


def test_history(table):
    assert table.history == [
        SnapshotLogEntry(snapshot_id="3051729675574597004", timestamp_ms=1515100955770),
        SnapshotLogEntry(snapshot_id="3055729675574597004", timestamp_ms=1555100955770),
    ]
