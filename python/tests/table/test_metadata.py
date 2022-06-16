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

from iceberg.schema import Schema
from iceberg.serializers import FromByteStream
from iceberg.table.metadata import TableMetadata, TableMetadataV1, TableMetadataV2
from iceberg.types import NestedField, StringType

EXAMPLE_TABLE_METADATA_V1 = {
    "format-version": 1,
    "table-uuid": UUID("aefee669-d568-4f9c-b732-3c0cfd3bc7b0"),
    "location": "s3://foo/bar/baz.metadata.json",
    "last-updated-ms": 1600000000000,
    "last-column-id": 4,
    "schema": {
        "schema-id": 0,
        "fields": [
            {"id": 1, "name": "foo", "required": True, "type": "string"},
            {"id": 2, "name": "bar", "required": True, "type": "string"},
            {"id": 3, "name": "baz", "required": True, "type": "string"},
            {"id": 4, "name": "qux", "required": True, "type": "string"},
        ],
        "identifier-field-ids": [],
    },
    "schemas": [
        {
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "foo", "required": True, "type": "string"},
                {"id": 2, "name": "bar", "required": True, "type": "string"},
                {"id": 3, "name": "baz", "required": True, "type": "string"},
                {"id": 4, "name": "qux", "required": True, "type": "string"},
            ],
            "identifier-field-ids": [],
        },
    ],
    "current-schema-id": 0,
    "partition-spec": {},
    "default-spec-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "last-partition-id": 999,
    "default-sort-order-id": 0,
    "sort-orders": [{"order-id": 0, "fields": []}],
    "properties": {"owner": "root", "write.format.default": "parquet"},
    "current-snapshot-id": 7681945274687743099,
    "snapshots": [
        {
            "snapshot-id": 7681945274687743099,
            "timestamp-ms": 1637943123188,
            "summary": {
                "operation": "append",
                "added-data-files": "6",
                "added-records": "237993",
                "added-files-size": "3386901",
                "changed-partition-count": "1",
                "total-records": "237993",
                "total-files-size": "3386901",
                "total-data-files": "6",
                "total-delete-files": "0",
                "total-position-deletes": "0",
                "total-equality-deletes": "0",
            },
            "manifest-list": "s3://foo/bar/baz/snap-2874264644797652805-1-9cb3c3cf-5a04-40c1-bdd9-d8d7e38cd8e3.avro",
            "schema-id": 0,
        },
    ],
    "snapshot-log": [
        {"timestamp-ms": 1637943123188, "snapshot-id": 7681945274687743099},
    ],
    "metadata-log": [
        {
            "timestamp-ms": 1637943123331,
            "metadata-file": "3://foo/bar/baz/00000-907830f8-1a92-4944-965a-ff82c890e912.metadata.json",
        }
    ],
}
EXAMPLE_TABLE_METADATA_V2 = {
    "format-version": 2,
    "table-uuid": "aefee669-d568-4f9c-b732-3c0cfd3bc7b0",
    "location": "s3://foo/bar/baz.metadata.json",
    "last-updated-ms": 1600000000000,
    "last-column-id": 4,
    "last-sequence-number": 1,
    "schemas": [
        {
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "foo", "required": True, "type": "string"},
                {"id": 2, "name": "bar", "required": True, "type": "string"},
                {"id": 3, "name": "baz", "required": True, "type": "string"},
                {"id": 4, "name": "qux", "required": True, "type": "string"},
            ],
            "identifier-field-ids": [],
        }
    ],
    "current-schema-id": 0,
    "default-spec-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "last-partition-id": 999,
    "default-sort-order-id": 0,
    "sort-orders": [{"order-id": 0, "fields": []}],
    "properties": {"owner": "root", "write.format.default": "parquet", "read.split.target.size": 134217728},
    "current-snapshot-id": 7681945274687743099,
    "snapshots": [
        {
            "snapshot-id": 7681945274687743099,
            "timestamp-ms": 1637943123188,
            "summary": {
                "operation": "append",
                "added-data-files": "6",
                "added-records": "237993",
                "added-files-size": "3386901",
                "changed-partition-count": "1",
                "total-records": "237993",
                "total-files-size": "3386901",
                "total-data-files": "6",
                "total-delete-files": "0",
                "total-position-deletes": "0",
                "total-equality-deletes": "0",
            },
            "manifest-list": "s3://foo/bar/baz/snap-2874264644797652805-1-9cb3c3cf-5a04-40c1-bdd9-d8d7e38cd8e3.avro",
            "schema-id": 0,
        },
    ],
    "snapshot-log": [
        {"timestamp-ms": 1637943123188, "snapshot-id": 7681945274687743099},
    ],
    "metadata-log": [
        {
            "timestamp-ms": 1637943123331,
            "metadata-file": "3://foo/bar/baz/00000-907830f8-1a92-4944-965a-ff82c890e912.metadata.json",
        }
    ],
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
    assert table_metadata.table_uuid == UUID("aefee669-d568-4f9c-b732-3c0cfd3bc7b0")
    assert table_metadata.location == "s3://foo/bar/baz.metadata.json"
    assert table_metadata.last_sequence_number == 1
    assert table_metadata.last_updated_ms == 1600000000000
    assert table_metadata.last_column_id == 4
    assert table_metadata.schemas[0].schema_id == 0
    assert table_metadata.current_schema_id == 0
    assert table_metadata.partition_specs[0]["spec-id"] == 0
    assert table_metadata.default_spec_id == 0
    assert table_metadata.last_partition_id == 999
    assert table_metadata.properties["read.split.target.size"] == 134217728
    assert table_metadata.current_snapshot_id == 7681945274687743099
    assert table_metadata.snapshots[0]["snapshot-id"] == 7681945274687743099
    assert table_metadata.snapshot_log[0]["timestamp-ms"] == 1637943123188
    assert table_metadata.metadata_log[0]["timestamp-ms"] == 1637943123331
    assert table_metadata.sort_orders[0]["order-id"] == 0
    assert table_metadata.default_sort_order_id == 0


def test_v1_metadata_parsing_directly():
    """Test retrieving values from a TableMetadata instance of version 1"""
    table_metadata = TableMetadataV1(**EXAMPLE_TABLE_METADATA_V1)

    # The version 1 will automatically be bumped to version 2
    assert table_metadata.format_version == 2
    assert table_metadata.table_uuid == UUID("aefee669-d568-4f9c-b732-3c0cfd3bc7b0")
    assert table_metadata.location == "s3://foo/bar/baz.metadata.json"
    assert table_metadata.last_updated_ms == 1600000000000
    assert table_metadata.last_column_id == 4
    assert table_metadata.schemas[0].schema_id == 0
    assert table_metadata.current_schema_id == 0
    assert table_metadata.partition_specs[0]["spec-id"] == 0
    assert table_metadata.default_spec_id == 0
    assert table_metadata.last_partition_id == 999
    assert table_metadata.current_snapshot_id == 7681945274687743099
    assert table_metadata.snapshots[0]["snapshot-id"] == 7681945274687743099
    assert table_metadata.snapshot_log[0]["timestamp-ms"] == 1637943123188
    assert table_metadata.metadata_log[0]["timestamp-ms"] == 1637943123331
    assert table_metadata.sort_orders[0]["order-id"] == 0
    assert table_metadata.default_sort_order_id == 0


def test_v2_metadata_parsing_directly():
    """Test retrieving values from a TableMetadata instance of version 2"""
    table_metadata = TableMetadataV2(**EXAMPLE_TABLE_METADATA_V2)

    assert table_metadata.format_version == 2
    assert table_metadata.table_uuid == UUID("aefee669-d568-4f9c-b732-3c0cfd3bc7b0")
    assert table_metadata.location == "s3://foo/bar/baz.metadata.json"
    assert table_metadata.last_sequence_number == 1
    assert table_metadata.last_updated_ms == 1600000000000
    assert table_metadata.last_column_id == 4
    assert table_metadata.schemas[0].schema_id == 0
    assert table_metadata.current_schema_id == 0
    assert table_metadata.partition_specs[0]["spec-id"] == 0
    assert table_metadata.default_spec_id == 0
    assert table_metadata.last_partition_id == 999
    assert table_metadata.properties["read.split.target.size"] == 134217728
    assert table_metadata.current_snapshot_id == 7681945274687743099
    assert table_metadata.snapshots[0]["snapshot-id"] == 7681945274687743099
    assert table_metadata.snapshot_log[0]["timestamp-ms"] == 1637943123188
    assert table_metadata.metadata_log[0]["timestamp-ms"] == 1637943123331
    assert table_metadata.sort_orders[0]["order-id"] == 0
    assert table_metadata.default_sort_order_id == 0


def test_parsing_correct_types():
    table_metadata = TableMetadataV2(**EXAMPLE_TABLE_METADATA_V2)
    assert isinstance(table_metadata.schemas[0], Schema)
    assert isinstance(table_metadata.schemas[0].fields[0], NestedField)
    assert isinstance(table_metadata.schemas[0].fields[0].field_type, StringType)


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
