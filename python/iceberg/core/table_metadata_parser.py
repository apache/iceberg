# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json

from iceberg.core import (PartitionSpecParser,
                          SchemaParser,
                          SnapshotLogEntry,
                          SnapshotParser,
                          TableMetadata)


class TableMetadataParser(object):
    FORMAT_VERSION = "format-version"
    LOCATION = "location"
    LAST_UPDATED_MILLIS = "last-updated-ms"
    LAST_COLUMN_ID = "last-column-id"
    SCHEMA = "schema"
    PARTITION_SPEC = "partition-spec"
    PARTITION_SPECS = "partition-specs"
    DEFAULT_SPEC_ID = "default-spec-id"
    PROPERTIES = "properties"
    CURRENT_SNAPSHOT_ID = "current-snapshot-id"
    SNAPSHOTS = "snapshots"
    SNAPSHOT_ID = "snapshot-id"
    TIMESTAMP_MS = "timestamp-ms"
    LOG = "snapshot-log"

    @staticmethod
    def to_json(metadata):
        return json.dumps({TableMetadataParser.FORMAT_VERSION: TableMetadata.TABLE_FORMAT_VERSION,
                           TableMetadataParser.LOCATION: metadata.location,
                           TableMetadataParser.LAST_UPDATED_MILLIS: metadata.last_updated_millis,
                           TableMetadataParser.LAST_COLUMN_ID: metadata.last_column_id,
                           TableMetadataParser.SCHEMA: SchemaParser.to_json(metadata.schema, dumps=False),
                           TableMetadataParser.PARTITION_SPEC: PartitionSpecParser.to_json_fields(metadata.spec),
                           TableMetadataParser.DEFAULT_SPEC_ID: int(metadata.default_spec_id),
                           TableMetadataParser.PARTITION_SPECS: [PartitionSpecParser.to_json(spec, dumps=False)
                                                                 for spec in metadata.specs],
                           TableMetadataParser.PROPERTIES: metadata.properties,
                           TableMetadataParser.CURRENT_SNAPSHOT_ID: (metadata.current_snapshot_id
                                                                     if metadata.current_snapshot_id is not None
                                                                     else -1),
                           TableMetadataParser.SNAPSHOTS: [SnapshotParser.to_json(snapshot, dumps=False)
                                                           for snapshot in metadata.snapshots],
                           TableMetadataParser.LOG: [{TableMetadataParser.TIMESTAMP_MS: log_entry.timestamp_millis,
                                                      TableMetadataParser.SNAPSHOT_ID: log_entry.snapshot_id}
                                                     for log_entry in metadata.snapshot_log]})

    @staticmethod
    def write(metadata, metadata_location):
        raise RuntimeError("Not Yet implemented")

    @staticmethod
    def read(ops, metadata_location):
        raise RuntimeError("Not Yet implemented")

    @staticmethod
    def from_json(ops, file, json_obj):
        if isinstance(json_obj, str):
            json_obj = json.loads(json_obj)

        if not isinstance(json_obj, dict):
            raise RuntimeError("Cannot parse metadata from non-object: %s" % json_obj)

        format_version = json_obj.get(TableMetadataParser.FORMAT_VERSION)
        if format_version != TableMetadata.TABLE_FORMAT_VERSION:
            raise RuntimeError("Cannot read unsupported version: %s" % format_version)

        location = json_obj.get(TableMetadataParser.LOCATION)
        last_assigned_column = json_obj.get(TableMetadataParser.LAST_COLUMN_ID)
        schema = SchemaParser.from_json(json_obj.get(TableMetadataParser.SCHEMA))

        spec_array = json_obj.get(TableMetadataParser.PARTITION_SPECS)
        if spec_array is not None:
            default_spec_id = json_obj.get(TableMetadataParser.DEFAULT_SPEC_ID)
            specs = [PartitionSpecParser.from_json(schema, spec)
                     for spec in spec_array]
        else:
            default_spec_id = TableMetadata.INITIAL_SPEC_ID
            specs = (PartitionSpecParser.from_json(schema, json_obj.get(TableMetadataParser.PARTITION_SPEC)),)

        props = json_obj.get(TableMetadataParser.PROPERTIES)
        current_version_id = json_obj.get(TableMetadataParser.CURRENT_SNAPSHOT_ID)
        last_updated_millis = json_obj.get(TableMetadataParser.LAST_UPDATED_MILLIS)
        snapshots = [SnapshotParser.from_json(ops, snapshot) for snapshot in json_obj.get(TableMetadataParser.SNAPSHOTS)]
        entries = [SnapshotLogEntry(log_entry.get(TableMetadataParser.TIMESTAMP_MS),
                                    log_entry.get(TableMetadataParser.SNAPSHOT_ID))
                   for log_entry in json_obj.get(TableMetadataParser.LOG)]

        return TableMetadata(ops, file, location,
                             last_updated_millis, last_assigned_column, schema, default_spec_id, specs, props, current_version_id,
                             snapshots, entries)
