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

import json

from iceberg.core import (PartitionSpecParser,
                          SchemaParser,
                          SnapshotLogEntry,
                          SnapshotParser,
                          TableMetadata,
                          TableMetadataParser)


def test_json_conversion(ops, expected_metadata):
    current_snapshot_id = expected_metadata.current_snapshot_id
    previous_snapshot_id = expected_metadata.current_snapshot().parent_id
    previous_snapshot = expected_metadata.snapshot(previous_snapshot_id)

    as_json = TableMetadataParser.to_json(expected_metadata)
    metadata = TableMetadataParser.from_json(ops, None, as_json)

    assert metadata.location == expected_metadata.location
    assert metadata.last_column_id == expected_metadata.last_column_id
    assert metadata.schema.as_struct() == expected_metadata.schema.as_struct()
    assert str(metadata.spec) == str(expected_metadata.spec)
    assert metadata.default_spec_id == expected_metadata.default_spec_id
    assert metadata.specs == expected_metadata.specs
    assert metadata.properties == expected_metadata.properties
    assert metadata.snapshot_log == expected_metadata.snapshot_log
    assert metadata.current_snapshot().snapshot_id == current_snapshot_id
    assert metadata.current_snapshot().parent_id == previous_snapshot_id
    assert metadata.current_snapshot().manifests == expected_metadata.current_snapshot().manifests
    assert metadata.snapshot(previous_snapshot_id).snapshot_id == previous_snapshot_id
    assert metadata.snapshot(previous_snapshot_id).manifests == previous_snapshot.manifests


def test_from_json_sorts_snapshot_log(ops, expected_metadata_sorting):
    current_snapshot = expected_metadata_sorting.current_snapshot()
    previous_snapshot_id = expected_metadata_sorting.current_snapshot().parent_id
    previous_snapshot = expected_metadata_sorting.snapshot(previous_snapshot_id)

    expected_log = [SnapshotLogEntry(previous_snapshot.timestamp_millis, previous_snapshot.snapshot_id),
                    SnapshotLogEntry(current_snapshot.timestamp_millis, current_snapshot.snapshot_id)]

    as_json = TableMetadataParser.to_json(expected_metadata_sorting)
    metadata = TableMetadataParser.from_json(ops, None, as_json)

    assert expected_log == metadata.snapshot_log


def test_backward_compatibility_missing_part_spec_list(ops, missing_spec_list):
    as_json = to_json_without_spec_list(missing_spec_list)
    metadata = TableMetadataParser.from_json(ops, None, as_json)
    assert metadata.location == missing_spec_list.location
    assert metadata.last_column_id == missing_spec_list.last_column_id
    assert metadata.schema.as_struct() == missing_spec_list.schema.as_struct()
    assert str(metadata.spec) == str(missing_spec_list.spec)
    """
    Assert.assertEquals("Partition spec should be the default",
        expected.spec().toString(), metadata.spec().toString());
    Assert.assertEquals("Default spec ID should default to TableMetadata.INITIAL_SPEC_ID",
        TableMetadata.INITIAL_SPEC_ID, metadata.defaultSpecId());
    Assert.assertEquals("PartitionSpec should contain the spec",
        1, metadata.specs().size());
    Assert.assertTrue("PartitionSpec should contain the spec",
        metadata.specs().get(0).compatibleWith(spec));
    Assert.assertEquals("PartitionSpec should have ID TableMetadata.INITIAL_SPEC_ID",
        TableMetadata.INITIAL_SPEC_ID, metadata.specs().get(0).specId());
    Assert.assertEquals("Properties should match",
        expected.properties(), metadata.properties());
    Assert.assertEquals("Snapshot logs should match",
        expected.snapshotLog(), metadata.snapshotLog());
    Assert.assertEquals("Current snapshot ID should match",
        currentSnapshotId, metadata.currentSnapshot().snapshotId());
    Assert.assertEquals("Parent snapshot ID should match",
        (Long) previousSnapshotId, metadata.currentSnapshot().parentId());
    Assert.assertEquals("Current snapshot files should match",
        currentSnapshot.manifests(), metadata.currentSnapshot().manifests());
    Assert.assertEquals("Previous snapshot ID should match",
        previousSnapshotId, metadata.snapshot(previousSnapshotId).snapshotId());
    Assert.assertEquals("Previous snapshot files should match",
        previousSnapshot.manifests(),
        metadata.snapshot(previousSnapshotId).manifests());"""


def to_json_without_spec_list(metadata):
    return json.dumps({TableMetadataParser.FORMAT_VERSION: TableMetadata.TABLE_FORMAT_VERSION,
                       TableMetadataParser.LOCATION: metadata.location,
                       TableMetadataParser.LAST_UPDATED_MILLIS: metadata.last_updated_millis,
                       TableMetadataParser.LAST_COLUMN_ID: metadata.last_column_id,
                       TableMetadataParser.SCHEMA: SchemaParser.to_dict(metadata.schema),
                       TableMetadataParser.PARTITION_SPEC: PartitionSpecParser.to_json_fields(metadata.spec),
                       TableMetadataParser.PROPERTIES: metadata.properties,
                       TableMetadataParser.CURRENT_SNAPSHOT_ID: (-1 if metadata.current_snapshot() is None
                                                                 else metadata.current_snapshot().snapshot_id),
                       TableMetadataParser.SNAPSHOTS: [SnapshotParser.to_dict(snapshot)
                                                       for snapshot in metadata.snapshots]})
