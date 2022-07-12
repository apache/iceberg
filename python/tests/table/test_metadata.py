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
from uuid import UUID

import pytest

from pyiceberg.exceptions import ValidationError
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromByteStream
from pyiceberg.table.metadata import TableMetadata, TableMetadataV1, TableMetadataV2
from pyiceberg.table.refs import SnapshotRef, SnapshotRefType
from pyiceberg.types import LongType, NestedField

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
EXAMPLE_TABLE_METADATA_V2 = {
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
    "default-spec-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
    "last-partition-id": 1000,
    "default-sort-order-id": 3,
    "sort-orders": [
        {
            "order-id": 3,
            "fields": [
                {"transform": "identity", "source-id": 2, "direction": "asc", "null-order": "nulls-first"},
                {"transform": "bucket[4]", "source-id": 3, "direction": "desc", "null-order": "nulls-last"},
            ],
        }
    ],
    "properties": {"read.split.target.size": 134217728},
    "current-snapshot-id": 3055729675574597004,
    "snapshots": [
        {
            "snapshot-id": 3051729675574597004,
            "timestamp-ms": 1515100955770,
            "sequence-number": 0,
            "summary": {"operation": "append"},
            "manifest-list": "s3://a/b/1.avro",
        },
        {
            "snapshot-id": 3055729675574597004,
            "parent-snapshot-id": 3051729675574597004,
            "timestamp-ms": 1555100955770,
            "sequence-number": 1,
            "summary": {"operation": "append"},
            "manifest-list": "s3://a/b/2.avro",
            "schema-id": 1,
        },
    ],
    "snapshot-log": [
        {"snapshot-id": 3051729675574597004, "timestamp-ms": 1515100955770},
        {"snapshot-id": 3055729675574597004, "timestamp-ms": 1555100955770},
    ],
    "metadata-log": [],
}


@pytest.mark.parametrize(
    "metadata",
    [
        EXAMPLE_TABLE_METADATA_V1,
        EXAMPLE_TABLE_METADATA_V2,
    ],
)
def test_from_dict(metadata: dict):
    """Test initialization of a TableMetadata instance from a dictionary"""
    TableMetadata.parse_obj(metadata)


def test_from_byte_stream():
    """Test generating a TableMetadata instance from a file-like byte stream"""
    data = bytes(json.dumps(EXAMPLE_TABLE_METADATA_V2), encoding="utf-8")
    byte_stream = io.BytesIO(data)
    FromByteStream.table_metadata(byte_stream=byte_stream)


def test_v2_metadata_parsing():
    """Test retrieving values from a TableMetadata instance of version 2"""
    table_metadata = TableMetadata.parse_obj(EXAMPLE_TABLE_METADATA_V2)

    assert table_metadata.format_version == 2
    assert table_metadata.table_uuid == UUID("9c12d441-03fe-4693-9a96-a0705ddf69c1")
    assert table_metadata.location == "s3://bucket/test/location"
    assert table_metadata.last_sequence_number == 34
    assert table_metadata.last_updated_ms == 1602638573590
    assert table_metadata.last_column_id == 3
    assert table_metadata.schemas[0].schema_id == 0
    assert table_metadata.current_schema_id == 1
    assert table_metadata.partition_specs[0]["spec-id"] == 0
    assert table_metadata.default_spec_id == 0
    assert table_metadata.last_partition_id == 1000
    assert table_metadata.properties["read.split.target.size"] == "134217728"
    assert table_metadata.current_snapshot_id == 3055729675574597004
    assert table_metadata.snapshots[0].snapshot_id == 3051729675574597004
    assert table_metadata.snapshot_log[0]["timestamp-ms"] == 1515100955770
    assert table_metadata.sort_orders[0].order_id == 3
    assert table_metadata.default_sort_order_id == 3


def test_v1_metadata_parsing_directly():
    """Test retrieving values from a TableMetadata instance of version 1"""
    table_metadata = TableMetadataV1(**EXAMPLE_TABLE_METADATA_V1)

    assert isinstance(table_metadata, TableMetadataV1)

    # The version 1 will automatically be bumped to version 2
    assert table_metadata.format_version == 1
    assert table_metadata.table_uuid == UUID("d20125c8-7284-442c-9aea-15fee620737c")
    assert table_metadata.location == "s3://bucket/test/location"
    assert table_metadata.last_updated_ms == 1602638573874
    assert table_metadata.last_column_id == 3
    assert table_metadata.schemas[0].schema_id == 0
    assert table_metadata.current_schema_id == 0
    assert table_metadata.default_spec_id == 0
    assert table_metadata.last_partition_id == 1000
    assert table_metadata.current_snapshot_id is None
    assert table_metadata.default_sort_order_id == 0


def test_parsing_correct_types():
    table_metadata = TableMetadataV2(**EXAMPLE_TABLE_METADATA_V2)
    assert isinstance(table_metadata.schemas[0], Schema)
    assert isinstance(table_metadata.schemas[0].fields[0], NestedField)
    assert isinstance(table_metadata.schemas[0].fields[0].field_type, LongType)


def test_updating_metadata():
    """Test creating a new TableMetadata instance that's an updated version of
    an existing TableMetadata instance"""
    table_metadata = TableMetadataV2(**EXAMPLE_TABLE_METADATA_V2)

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

    new_table_metadata = TableMetadataV2(**mutable_table_metadata)

    assert new_table_metadata.current_schema_id == 1
    assert new_table_metadata.schemas[-1] == Schema(**new_schema)


def test_serialize_v1():
    table_metadata = TableMetadataV1(**EXAMPLE_TABLE_METADATA_V1).json()
    expected = """{"location": "s3://bucket/test/location", "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c", "last-updated-ms": 1602638573874, "last-column-id": 3, "schemas": [{"fields": [{"id": 1, "name": "x", "type": "long", "required": true}, {"id": 2, "name": "y", "type": "long", "required": true, "doc": "comment"}, {"id": 3, "name": "z", "type": "long", "required": true}], "schema-id": 0, "identifier-field-ids": []}], "current-schema-id": 0, "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}], "default-spec-id": 0, "last-partition-id": 1000, "properties": {}, "snapshots": [{"snapshot-id": 1925, "timestamp-ms": 1602638573822}], "snapshot-log": [], "metadata-log": [], "sort-orders": [{"order-id": 0, "fields": []}], "default-sort-order-id": 0, "refs": {}, "format-version": 1, "schema": {"fields": [{"id": 1, "name": "x", "type": "long", "required": true}, {"id": 2, "name": "y", "type": "long", "required": true, "doc": "comment"}, {"id": 3, "name": "z", "type": "long", "required": true}], "schema-id": 0, "identifier-field-ids": []}, "partition-spec": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}"""
    assert table_metadata == expected


def test_serialize_v2():
    table_metadata = TableMetadataV2(**EXAMPLE_TABLE_METADATA_V2).json()
    expected = """{"location": "s3://bucket/test/location", "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1", "last-updated-ms": 1602638573590, "last-column-id": 3, "schemas": [{"fields": [{"id": 1, "name": "x", "type": "long", "required": true}], "schema-id": 0, "identifier-field-ids": []}, {"fields": [{"id": 1, "name": "x", "type": "long", "required": true}, {"id": 2, "name": "y", "type": "long", "required": true, "doc": "comment"}, {"id": 3, "name": "z", "type": "long", "required": true}], "schema-id": 1, "identifier-field-ids": [1, 2]}], "current-schema-id": 1, "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}], "default-spec-id": 0, "last-partition-id": 1000, "properties": {"read.split.target.size": "134217728"}, "current-snapshot-id": 3055729675574597004, "snapshots": [{"snapshot-id": 3051729675574597004, "sequence-number": 0, "timestamp-ms": 1515100955770, "manifest-list": "s3://a/b/1.avro", "summary": {"operation": "append"}}, {"snapshot-id": 3055729675574597004, "parent-snapshot-id": 3051729675574597004, "sequence-number": 1, "timestamp-ms": 1555100955770, "manifest-list": "s3://a/b/2.avro", "summary": {"operation": "append"}, "schema-id": 1}], "snapshot-log": [{"snapshot-id": 3051729675574597004, "timestamp-ms": 1515100955770}, {"snapshot-id": 3055729675574597004, "timestamp-ms": 1555100955770}], "metadata-log": [], "sort-orders": [{"order-id": 3, "fields": [{"source-id": 2, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}, {"source-id": 3, "transform": "bucket[4]", "direction": "desc", "null-order": "nulls-last"}]}], "default-sort-order-id": 3, "refs": {"main": {"snapshot-id": 3055729675574597004, "type": "branch"}}, "format-version": 2, "last-sequence-number": 34}"""
    assert table_metadata == expected


def test_migrate_v1_schemas():
    table_metadata = TableMetadataV1(**EXAMPLE_TABLE_METADATA_V1)

    assert isinstance(table_metadata, TableMetadataV1)
    assert len(table_metadata.schemas) == 1
    assert table_metadata.schemas[0] == table_metadata.schema_


def test_migrate_v1_partition_specs():
    # Copy the example, and add a spec
    table_metadata = TableMetadataV1(**EXAMPLE_TABLE_METADATA_V1)
    assert isinstance(table_metadata, TableMetadataV1)
    assert len(table_metadata.partition_specs) == 1
    # Spec ID gets added automatically
    assert table_metadata.partition_specs == [
        {"spec-id": 0, "fields": [{"field-id": 1000, "name": "x", "source-id": 1, "transform": "identity"}]}
    ]


def test_invalid_format_version():
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
        TableMetadata.parse_obj(table_metadata_invalid_format_version)

    assert "Unknown format version: -1" in str(exc_info.value)


def test_current_schema_not_found():
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
        TableMetadata.parse_obj(table_metadata_schema_not_found)

    assert "current-schema-id 2 can't be found in the schemas" in str(exc_info.value)


def test_sort_order_not_found():
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
        TableMetadata.parse_obj(table_metadata_schema_not_found)

    assert "default-sort-order-id 4 can't be found" in str(exc_info.value)


def test_sort_order_unsorted():
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

    table_metadata = TableMetadata.parse_obj(table_metadata_schema_not_found)

    # Most important here is that we correctly handle sort-order-id 0
    assert len(table_metadata.sort_orders) == 0


def test_invalid_partition_spec():
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
        TableMetadata.parse_obj(table_metadata_spec_not_found)

    assert "default-spec-id 1 can't be found" in str(exc_info.value)


def test_v1_writing_metadata():
    """
    https://iceberg.apache.org/spec/#version-2

    Writing v1 metadata:
        - Table metadata field last-sequence-number should not be written
    """

    table_metadata = TableMetadataV1(**EXAMPLE_TABLE_METADATA_V1)
    metadata_v1_json = table_metadata.json()
    metadata_v1 = json.loads(metadata_v1_json)

    assert "last-sequence-number" not in metadata_v1


def test_v1_metadata_for_v2():
    """
    https://iceberg.apache.org/spec/#version-2

    Reading v1 metadata for v2:
        - Table metadata field last-sequence-number must default to 0
    """

    table_metadata = TableMetadataV1(**EXAMPLE_TABLE_METADATA_V1).to_v2()

    assert table_metadata.last_sequence_number == 0


def test_v1_write_metadata_for_v2():
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
        }
    ]
    assert metadata_v2["partition-specs"] == [
        {"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}
    ]
    assert metadata_v2["default-spec-id"] == 0
    assert metadata_v2["last-partition-id"] == 1000
    assert metadata_v2["sort-orders"] == [{"order-id": 0, "fields": []}]
    assert metadata_v2["default-sort-order-id"] == 0
    # Deprecated fields
    assert "schema" not in metadata_v2
    assert "partition-spec" not in metadata_v2


def test_v2_ref_creation():
    table_metadata = TableMetadataV2(**EXAMPLE_TABLE_METADATA_V2)
    assert table_metadata.refs == {"main": SnapshotRef(snapshot_id=3055729675574597004, snapshot_ref_type=SnapshotRefType.BRANCH)}
