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

import io
import json
from typing import Any, Dict
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

from pyiceberg.exceptions import ValidationError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromByteStream
from pyiceberg.table import SortOrder
from pyiceberg.table.metadata import (
    TableMetadataUtil,
    TableMetadataV1,
    TableMetadataV2,
    new_table_metadata,
)
from pyiceberg.table.refs import SnapshotRef, SnapshotRefType
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    BooleanType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)

EXAMPLE_TABLE_METADATA_V1 = {
    "format-version": 1,
    "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
    "location": "s3://bucket/test/location",
    "last-updated-ms": 1602638573874,
    "last-column-id": 3,
    "schema": {
        "type": "struct",
        "fields": [
            {"id": 1, "name": "x", "required": True, "type": "long"},
            {"id": 2, "name": "y", "required": True, "type": "long", "doc": "comment"},
            {"id": 3, "name": "z", "required": True, "type": "long"},
        ],
    },
    "partition-spec": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}],
    "properties": {},
    "current-snapshot-id": -1,
    "snapshots": [{"snapshot-id": 1925, "timestamp-ms": 1602638573822}],
}


def test_from_dict_v1() -> None:
    """Test initialization of a TableMetadata instance from a dictionary"""
    TableMetadataUtil.parse_obj(EXAMPLE_TABLE_METADATA_V1)


def test_from_dict_v2(example_table_metadata_v2: Dict[str, Any]) -> None:
    """Test initialization of a TableMetadata instance from a dictionary"""
    TableMetadataUtil.parse_obj(example_table_metadata_v2)


def test_from_byte_stream(example_table_metadata_v2: Dict[str, Any]) -> None:
    """Test generating a TableMetadata instance from a file-like byte stream"""
    data = bytes(json.dumps(example_table_metadata_v2), encoding="utf-8")
    byte_stream = io.BytesIO(data)
    FromByteStream.table_metadata(byte_stream=byte_stream)


def test_v2_metadata_parsing(example_table_metadata_v2: Dict[str, Any]) -> None:
    """Test retrieving values from a TableMetadata instance of version 2"""
    table_metadata = TableMetadataUtil.parse_obj(example_table_metadata_v2)

    assert table_metadata.format_version == 2
    assert table_metadata.table_uuid == UUID("9c12d441-03fe-4693-9a96-a0705ddf69c1")
    assert table_metadata.location == "s3://bucket/test/location"
    assert table_metadata.last_sequence_number == 34
    assert table_metadata.last_updated_ms == 1602638573590
    assert table_metadata.last_column_id == 3
    assert table_metadata.schemas[0].schema_id == 0
    assert table_metadata.current_schema_id == 1
    assert table_metadata.partition_specs[0].spec_id == 0
    assert table_metadata.default_spec_id == 0
    assert table_metadata.last_partition_id == 1000
    assert table_metadata.properties["read.split.target.size"] == "134217728"
    assert table_metadata.current_snapshot_id == 3055729675574597004
    assert table_metadata.snapshots[0].snapshot_id == 3051729675574597004
    assert table_metadata.snapshot_log[0].timestamp_ms == 1515100955770
    assert table_metadata.sort_orders[0].order_id == 3
    assert table_metadata.default_sort_order_id == 3


def test_v1_metadata_parsing_directly() -> None:
    """Test retrieving values from a TableMetadata instance of version 1"""
    table_metadata = TableMetadataV1(**EXAMPLE_TABLE_METADATA_V1)

    assert isinstance(table_metadata, TableMetadataV1)

    # The version 1 will automatically be bumped to version 2
    assert table_metadata.format_version == 1
    assert table_metadata.table_uuid == UUID("d20125c8-7284-442c-9aea-15fee620737c")
    assert table_metadata.location == "s3://bucket/test/location"
    assert table_metadata.last_updated_ms == 1602638573874
    assert table_metadata.last_column_id == 3
    assert table_metadata.schemas == [
        Schema(
            NestedField(field_id=1, name="x", field_type=LongType(), required=True),
            NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
            NestedField(field_id=3, name="z", field_type=LongType(), required=True),
            schema_id=0,
            identifier_field_ids=[],
        )
    ]
    assert table_metadata.schemas[0].schema_id == 0
    assert table_metadata.current_schema_id == 0
    assert table_metadata.partition_specs == [
        PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="x"))
    ]
    assert table_metadata.default_spec_id == 0
    assert table_metadata.last_partition_id == 1000
    assert table_metadata.current_snapshot_id is None
    assert table_metadata.default_sort_order_id == 0


def test_parsing_correct_types(example_table_metadata_v2: Dict[str, Any]) -> None:
    table_metadata = TableMetadataV2(**example_table_metadata_v2)
    assert isinstance(table_metadata.schemas[0], Schema)
    assert isinstance(table_metadata.schemas[0].fields[0], NestedField)
    assert isinstance(table_metadata.schemas[0].fields[0].field_type, LongType)


def test_updating_metadata(example_table_metadata_v2: Dict[str, Any]) -> None:
    """Test creating a new TableMetadata instance that's an updated version of
    an existing TableMetadata instance"""
    table_metadata = TableMetadataV2(**example_table_metadata_v2)

    new_schema = {
        "type": "struct",
        "schema-id": 1,
        "fields": [
            {"id": 1, "name": "foo", "required": True, "type": "string"},
            {"id": 2, "name": "bar", "required": True, "type": "string"},
            {"id": 3, "name": "baz", "required": True, "type": "string"},
        ],
    }

    mutable_table_metadata = table_metadata.dict()
    mutable_table_metadata["schemas"].append(new_schema)
    mutable_table_metadata["current-schema-id"] = 1

    table_metadata = TableMetadataV2(**mutable_table_metadata)

    assert table_metadata.current_schema_id == 1
    assert table_metadata.schemas[-1] == Schema(**new_schema)


def test_serialize_v1() -> None:
    table_metadata = TableMetadataV1(**EXAMPLE_TABLE_METADATA_V1)
    table_metadata_json = table_metadata.json()
    expected = """{"location": "s3://bucket/test/location", "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c", "last-updated-ms": 1602638573874, "last-column-id": 3, "schemas": [{"type": "struct", "fields": [{"id": 1, "name": "x", "type": "long", "required": true}, {"id": 2, "name": "y", "type": "long", "required": true, "doc": "comment"}, {"id": 3, "name": "z", "type": "long", "required": true}], "schema-id": 0, "identifier-field-ids": []}], "current-schema-id": 0, "partition-specs": [{"spec-id": 0, "fields": [{"source-id": 1, "field-id": 1000, "transform": "identity", "name": "x"}]}], "default-spec-id": 0, "last-partition-id": 1000, "properties": {}, "snapshots": [{"snapshot-id": 1925, "timestamp-ms": 1602638573822}], "snapshot-log": [], "metadata-log": [], "sort-orders": [{"order-id": 0, "fields": []}], "default-sort-order-id": 0, "refs": {}, "format-version": 1, "schema": {"type": "struct", "fields": [{"id": 1, "name": "x", "type": "long", "required": true}, {"id": 2, "name": "y", "type": "long", "required": true, "doc": "comment"}, {"id": 3, "name": "z", "type": "long", "required": true}], "schema-id": 0, "identifier-field-ids": []}, "partition-spec": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}"""
    assert table_metadata_json == expected


def test_serialize_v2(example_table_metadata_v2: Dict[str, Any]) -> None:
    table_metadata = TableMetadataV2(**example_table_metadata_v2).json()
    expected = """{"location": "s3://bucket/test/location", "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1", "last-updated-ms": 1602638573590, "last-column-id": 3, "schemas": [{"type": "struct", "fields": [{"id": 1, "name": "x", "type": "long", "required": true}], "schema-id": 0, "identifier-field-ids": []}, {"type": "struct", "fields": [{"id": 1, "name": "x", "type": "long", "required": true}, {"id": 2, "name": "y", "type": "long", "required": true, "doc": "comment"}, {"id": 3, "name": "z", "type": "long", "required": true}], "schema-id": 1, "identifier-field-ids": [1, 2]}], "current-schema-id": 1, "partition-specs": [{"spec-id": 0, "fields": [{"source-id": 1, "field-id": 1000, "transform": "identity", "name": "x"}]}], "default-spec-id": 0, "last-partition-id": 1000, "properties": {"read.split.target.size": "134217728"}, "current-snapshot-id": 3055729675574597004, "snapshots": [{"snapshot-id": 3051729675574597004, "sequence-number": 0, "timestamp-ms": 1515100955770, "manifest-list": "s3://a/b/1.avro", "summary": {"operation": "append"}}, {"snapshot-id": 3055729675574597004, "parent-snapshot-id": 3051729675574597004, "sequence-number": 1, "timestamp-ms": 1555100955770, "manifest-list": "s3://a/b/2.avro", "summary": {"operation": "append"}, "schema-id": 1}], "snapshot-log": [{"snapshot-id": "3051729675574597004", "timestamp-ms": 1515100955770}, {"snapshot-id": "3055729675574597004", "timestamp-ms": 1555100955770}], "metadata-log": [{"metadata-file": "s3://bucket/.../v1.json", "timestamp-ms": 1515100}], "sort-orders": [{"order-id": 3, "fields": [{"source-id": 2, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}, {"source-id": 3, "transform": "bucket[4]", "direction": "desc", "null-order": "nulls-last"}]}], "default-sort-order-id": 3, "refs": {"test": {"snapshot-id": 3051729675574597004, "type": "tag", "max-ref-age-ms": 10000000}, "main": {"snapshot-id": 3055729675574597004, "type": "branch"}}, "format-version": 2, "last-sequence-number": 34}"""
    assert table_metadata == expected


def test_migrate_v1_schemas() -> None:
    table_metadata = TableMetadataV1(**EXAMPLE_TABLE_METADATA_V1)

    assert isinstance(table_metadata, TableMetadataV1)
    assert len(table_metadata.schemas) == 1
    assert table_metadata.schemas[0] == table_metadata.schema_


def test_migrate_v1_partition_specs() -> None:
    # Copy the example, and add a spec
    table_metadata = TableMetadataV1(**EXAMPLE_TABLE_METADATA_V1)
    assert isinstance(table_metadata, TableMetadataV1)
    assert len(table_metadata.partition_specs) == 1
    # Spec ID gets added automatically
    assert table_metadata.partition_specs == [
        PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="x")),
    ]


def test_invalid_format_version() -> None:
    """Test the exception when trying to load an unknown version"""
    table_metadata_invalid_format_version = {
        "format-version": -1,
        "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
        "location": "s3://bucket/test/location",
        "last-updated-ms": 1602638573874,
        "last-column-id": 3,
        "schema": {
            "type": "struct",
            "fields": [
                {"id": 1, "name": "x", "required": True, "type": "long"},
                {"id": 2, "name": "y", "required": True, "type": "long", "doc": "comment"},
                {"id": 3, "name": "z", "required": True, "type": "long"},
            ],
        },
        "partition-spec": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}],
        "properties": {},
        "current-snapshot-id": -1,
        "snapshots": [],
    }

    with pytest.raises(ValidationError) as exc_info:
        TableMetadataUtil.parse_obj(table_metadata_invalid_format_version)

    assert "Unknown format version: -1" in str(exc_info.value)


def test_current_schema_not_found() -> None:
    """Test that we raise an exception when the schema can't be found"""

    table_metadata_schema_not_found = {
        "format-version": 2,
        "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
        "location": "s3://bucket/test/location",
        "last-updated-ms": 1602638573874,
        "last-column-id": 3,
        "schemas": [
            {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": True, "type": "long"}]},
            {
                "type": "struct",
                "schema-id": 1,
                "identifier-field-ids": [1, 2],
                "fields": [
                    {"id": 1, "name": "x", "required": True, "type": "long"},
                    {"id": 2, "name": "y", "required": True, "type": "long", "doc": "comment"},
                    {"id": 3, "name": "z", "required": True, "type": "long"},
                ],
            },
        ],
        "current-schema-id": 2,
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
        "last-partition-id": 1000,
        "default-sort-order-id": 0,
        "properties": {},
        "current-snapshot-id": -1,
        "snapshots": [],
    }

    with pytest.raises(ValidationError) as exc_info:
        TableMetadataUtil.parse_obj(table_metadata_schema_not_found)

    assert "current-schema-id 2 can't be found in the schemas" in str(exc_info.value)


def test_sort_order_not_found() -> None:
    """Test that we raise an exception when the schema can't be found"""

    table_metadata_schema_not_found = {
        "format-version": 2,
        "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
        "location": "s3://bucket/test/location",
        "last-updated-ms": 1602638573874,
        "last-column-id": 3,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "identifier-field-ids": [1, 2],
                "fields": [
                    {"id": 1, "name": "x", "required": True, "type": "long"},
                    {"id": 2, "name": "y", "required": True, "type": "long", "doc": "comment"},
                    {"id": 3, "name": "z", "required": True, "type": "long"},
                ],
            },
        ],
        "default-sort-order-id": 4,
        "sort-orders": [
            {
                "order-id": 3,
                "fields": [
                    {"transform": "identity", "source-id": 2, "direction": "asc", "null-order": "nulls-first"},
                    {"transform": "bucket[4]", "source-id": 3, "direction": "desc", "null-order": "nulls-last"},
                ],
            }
        ],
        "current-schema-id": 0,
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
        "last-partition-id": 1000,
        "properties": {},
        "current-snapshot-id": -1,
        "snapshots": [],
    }

    with pytest.raises(ValidationError) as exc_info:
        TableMetadataUtil.parse_obj(table_metadata_schema_not_found)

    assert "default-sort-order-id 4 can't be found" in str(exc_info.value)


def test_sort_order_unsorted() -> None:
    """Test that we raise an exception when the schema can't be found"""

    table_metadata_schema_not_found = {
        "format-version": 2,
        "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
        "location": "s3://bucket/test/location",
        "last-updated-ms": 1602638573874,
        "last-column-id": 3,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "identifier-field-ids": [1, 2],
                "fields": [
                    {"id": 1, "name": "x", "required": True, "type": "long"},
                    {"id": 2, "name": "y", "required": True, "type": "long", "doc": "comment"},
                    {"id": 3, "name": "z", "required": True, "type": "long"},
                ],
            },
        ],
        "default-sort-order-id": 0,
        "sort-orders": [],
        "current-schema-id": 0,
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
        "last-partition-id": 1000,
        "properties": {},
        "current-snapshot-id": -1,
        "snapshots": [],
    }

    table_metadata = TableMetadataUtil.parse_obj(table_metadata_schema_not_found)

    # Most important here is that we correctly handle sort-order-id 0
    assert len(table_metadata.sort_orders) == 0


def test_invalid_partition_spec() -> None:
    table_metadata_spec_not_found = {
        "format-version": 2,
        "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
        "location": "s3://bucket/test/location",
        "last-sequence-number": 34,
        "last-updated-ms": 1602638573590,
        "last-column-id": 3,
        "current-schema-id": 1,
        "schemas": [
            {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": True, "type": "long"}]},
            {
                "type": "struct",
                "schema-id": 1,
                "identifier-field-ids": [1, 2],
                "fields": [
                    {"id": 1, "name": "x", "required": True, "type": "long"},
                    {"id": 2, "name": "y", "required": True, "type": "long", "doc": "comment"},
                    {"id": 3, "name": "z", "required": True, "type": "long"},
                ],
            },
        ],
        "sort-orders": [],
        "default-sort-order-id": 0,
        "default-spec-id": 1,
        "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
        "last-partition-id": 1000,
    }
    with pytest.raises(ValidationError) as exc_info:
        TableMetadataUtil.parse_obj(table_metadata_spec_not_found)

    assert "default-spec-id 1 can't be found" in str(exc_info.value)


def test_v1_writing_metadata() -> None:
    """
    https://iceberg.apache.org/spec/#version-2

    Writing v1 metadata:
        - Table metadata field last-sequence-number should not be written
    """

    table_metadata = TableMetadataV1(**EXAMPLE_TABLE_METADATA_V1)
    metadata_v1_json = table_metadata.json()
    metadata_v1 = json.loads(metadata_v1_json)

    assert "last-sequence-number" not in metadata_v1


def test_v1_metadata_for_v2() -> None:
    """
    https://iceberg.apache.org/spec/#version-2

    Reading v1 metadata for v2:
        - Table metadata field last-sequence-number must default to 0
    """

    table_metadata = TableMetadataV1(**EXAMPLE_TABLE_METADATA_V1).to_v2()

    assert table_metadata.last_sequence_number == 0


def test_v1_write_metadata_for_v2() -> None:
    """
    https://iceberg.apache.org/spec/#version-2

    Table metadata JSON:
        - last-sequence-number was added and is required; default to 0 when reading v1 metadata
        - table-uuid is now required
        - current-schema-id is now required
        - schemas is now required
        - partition-specs is now required
        - default-spec-id is now required
        - last-partition-id is now required
        - sort-orders is now required
        - default-sort-order-id is now required
        - schema is no longer required and should be omitted; use schemas and current-schema-id instead
        - partition-spec is no longer required and should be omitted; use partition-specs and default-spec-id instead
    """

    minimal_example_v1 = {
        "format-version": 1,
        "location": "s3://bucket/test/location",
        "last-updated-ms": 1602638573874,
        "last-column-id": 3,
        "schema": {
            "type": "struct",
            "fields": [
                {"id": 1, "name": "x", "required": True, "type": "long"},
                {"id": 2, "name": "y", "required": True, "type": "long", "doc": "comment"},
                {"id": 3, "name": "z", "required": True, "type": "long"},
            ],
        },
        "partition-spec": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}],
        "properties": {},
        "current-snapshot-id": -1,
        "snapshots": [{"snapshot-id": 1925, "timestamp-ms": 1602638573822}],
    }

    table_metadata = TableMetadataV1(**minimal_example_v1).to_v2()
    metadata_v2_json = table_metadata.json()
    metadata_v2 = json.loads(metadata_v2_json)

    assert metadata_v2["last-sequence-number"] == 0
    assert UUID(metadata_v2["table-uuid"]) is not None
    assert metadata_v2["current-schema-id"] == 0
    assert metadata_v2["schemas"] == [
        {
            "fields": [
                {"id": 1, "name": "x", "required": True, "type": "long"},
                {"doc": "comment", "id": 2, "name": "y", "required": True, "type": "long"},
                {"id": 3, "name": "z", "required": True, "type": "long"},
            ],
            "identifier-field-ids": [],
            "schema-id": 0,
            "type": "struct",
        }
    ]
    assert metadata_v2["partition-specs"] == [
        {
            "spec-id": 0,
            "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}],
        }
    ]
    assert metadata_v2["default-spec-id"] == 0
    assert metadata_v2["last-partition-id"] == 1000
    assert metadata_v2["sort-orders"] == [{"order-id": 0, "fields": []}]
    assert metadata_v2["default-sort-order-id"] == 0
    # Deprecated fields
    assert "schema" not in metadata_v2
    assert "partition-spec" not in metadata_v2


def test_v2_ref_creation(example_table_metadata_v2: Dict[str, Any]) -> None:
    table_metadata = TableMetadataV2(**example_table_metadata_v2)
    assert table_metadata.refs == {
        "main": SnapshotRef(
            snapshot_id=3055729675574597004,
            snapshot_ref_type=SnapshotRefType.BRANCH,
            min_snapshots_to_keep=None,
            max_snapshot_age_ms=None,
            max_ref_age_ms=None,
        ),
        "test": SnapshotRef(
            snapshot_id=3051729675574597004,
            snapshot_ref_type=SnapshotRefType.TAG,
            min_snapshots_to_keep=None,
            max_snapshot_age_ms=None,
            max_ref_age_ms=10000000,
        ),
    }


def test_metadata_v1() -> None:
    valid_v1 = {
        "format-version": 1,
        "table-uuid": "bf289591-dcc0-4234-ad4f-5c3eed811a29",
        "location": "s3://tabular-wh-us-west-2-dev/8bcb0838-50fc-472d-9ddb-8feb89ef5f1e/bf289591-dcc0-4234-ad4f-5c3eed811a29",
        "last-updated-ms": 1657810967051,
        "last-column-id": 3,
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "identifier-field-ids": [2],
            "fields": [
                {"id": 1, "name": "foo", "required": False, "type": "string"},
                {"id": 2, "name": "bar", "required": True, "type": "int"},
                {"id": 3, "name": "baz", "required": False, "type": "boolean"},
            ],
        },
        "current-schema-id": 0,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "identifier-field-ids": [2],
                "fields": [
                    {"id": 1, "name": "foo", "required": False, "type": "string"},
                    {"id": 2, "name": "bar", "required": True, "type": "int"},
                    {"id": 3, "name": "baz", "required": False, "type": "boolean"},
                ],
            }
        ],
        "partition-spec": [],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {
            "write.delete.parquet.compression-codec": "zstd",
            "write.metadata.compression-codec": "gzip",
            "write.summary.partition-limit": "100",
            "write.parquet.compression-codec": "zstd",
        },
        "current-snapshot-id": -1,
        "refs": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
    }
    TableMetadataV1(**valid_v1)


@patch("time.time", MagicMock(return_value=12345))
def test_make_metadata_fresh() -> None:
    schema = Schema(
        NestedField(field_id=10, name="foo", field_type=StringType(), required=False),
        NestedField(field_id=22, name="bar", field_type=IntegerType(), required=True),
        NestedField(field_id=33, name="baz", field_type=BooleanType(), required=False),
        NestedField(
            field_id=41,
            name="qux",
            field_type=ListType(element_id=56, element_type=StringType(), element_required=True),
            required=True,
        ),
        NestedField(
            field_id=6,
            name="quux",
            field_type=MapType(
                key_id=77,
                key_type=StringType(),
                value_id=88,
                value_type=MapType(key_id=91, key_type=StringType(), value_id=102, value_type=IntegerType(), value_required=True),
                value_required=True,
            ),
            required=True,
        ),
        NestedField(
            field_id=113,
            name="location",
            field_type=ListType(
                element_id=124,
                element_type=StructType(
                    NestedField(field_id=132, name="latitude", field_type=FloatType(), required=False),
                    NestedField(field_id=143, name="longitude", field_type=FloatType(), required=False),
                ),
                element_required=True,
            ),
            required=True,
        ),
        NestedField(
            field_id=155,
            name="person",
            field_type=StructType(
                NestedField(field_id=169, name="name", field_type=StringType(), required=False),
                NestedField(field_id=178, name="age", field_type=IntegerType(), required=True),
            ),
            required=False,
        ),
        schema_id=10,
        identifier_field_ids=[22],
    )

    partition_spec = PartitionSpec(
        PartitionField(source_id=22, field_id=1022, transform=IdentityTransform(), name="bar"), spec_id=10
    )

    sort_order = SortOrder(
        SortField(source_id=10, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_LAST),
        order_id=10,
    )

    actual = new_table_metadata(
        schema=schema, partition_spec=partition_spec, sort_order=sort_order, location="s3://", properties={}
    )

    expected = TableMetadataV2(
        location="s3://",
        table_uuid=actual.table_uuid,
        last_updated_ms=actual.last_updated_ms,
        last_column_id=17,
        schemas=[
            Schema(
                NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
                NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
                NestedField(
                    field_id=4,
                    name="qux",
                    field_type=ListType(type="list", element_id=8, element_type=StringType(), element_required=True),
                    required=True,
                ),
                NestedField(
                    field_id=5,
                    name="quux",
                    field_type=MapType(
                        type="map",
                        key_id=9,
                        key_type=StringType(),
                        value_id=10,
                        value_type=MapType(
                            type="map",
                            key_id=11,
                            key_type=StringType(),
                            value_id=12,
                            value_type=IntegerType(),
                            value_required=True,
                        ),
                        value_required=True,
                    ),
                    required=True,
                ),
                NestedField(
                    field_id=6,
                    name="location",
                    field_type=ListType(
                        type="list",
                        element_id=13,
                        element_type=StructType(
                            NestedField(field_id=14, name="latitude", field_type=FloatType(), required=False),
                            NestedField(field_id=15, name="longitude", field_type=FloatType(), required=False),
                        ),
                        element_required=True,
                    ),
                    required=True,
                ),
                NestedField(
                    field_id=7,
                    name="person",
                    field_type=StructType(
                        NestedField(field_id=16, name="name", field_type=StringType(), required=False),
                        NestedField(field_id=17, name="age", field_type=IntegerType(), required=True),
                    ),
                    required=False,
                ),
                schema_id=0,
                identifier_field_ids=[2],
            )
        ],
        current_schema_id=0,
        partition_specs=[PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="bar"))],
        default_spec_id=0,
        last_partition_id=1000,
        properties={},
        current_snapshot_id=None,
        snapshots=[],
        snapshot_log=[],
        metadata_log=[],
        sort_orders=[
            SortOrder(
                SortField(
                    source_id=1, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_LAST
                ),
                order_id=1,
            )
        ],
        default_sort_order_id=1,
        refs={},
        format_version=2,
        last_sequence_number=0,
    )

    assert actual.dict() == expected.dict()
