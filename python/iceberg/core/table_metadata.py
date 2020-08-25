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

import time
from typing import Optional

from iceberg.api import PartitionSpec, Schema
from iceberg.api.types import assign_fresh_ids
from iceberg.core.table_operations import TableOperations
from iceberg.core.util import AtomicInteger
from iceberg.exceptions import ValidationException


class TableMetadata(object):
    INITIAL_SPEC_ID = 0
    TABLE_FORMAT_VERSION = 1

    @staticmethod
    def new_table_metadata(ops: TableOperations, schema: Schema, spec: PartitionSpec, location: str,
                           properties: dict = None) -> "TableMetadata":
        last_column_id = AtomicInteger(0)
        fresh_schema = assign_fresh_ids(schema, last_column_id.increment_and_get)

        spec_builder = PartitionSpec.builder_for(fresh_schema)
        for field in spec.fields:
            src_name = schema.find_column_name(field.source_id)
            spec_builder.add(field.source_id,
                             fresh_schema.find_field(src_name).field_id,
                             field.name,
                             str(field.transform))

        fresh_spec = spec_builder.build()
        properties = properties if properties is not None else dict()

        return TableMetadata(ops, None, location,
                             int(time.time() * 1000),
                             last_column_id.get(), fresh_schema, TableMetadata.INITIAL_SPEC_ID, [fresh_spec],
                             properties, -1, list(), list())

    def __init__(self, ops, file, location, last_updated_millis,
                 last_column_id, schema, default_spec_id, specs, properties,
                 current_snapshot_id, snapshots, snapshot_log):
        self.ops = ops
        self._file = file
        self._location = location
        self.last_updated_millis = last_updated_millis
        self.last_column_id = last_column_id
        self.schema = schema
        self.default_spec_id = default_spec_id
        self.specs = specs
        self.properties = properties
        self.properties["provider"] = "ICEBERG"
        self.current_snapshot_id = current_snapshot_id
        self.snapshots = snapshots
        self.snapshot_log = snapshot_log

        self.snapshot_by_id = {version.snapshot_id: version for version in self.snapshots}
        self.specs_by_id = {spec.spec_id: spec for spec in self.specs}

        last = None
        for log_entry in snapshot_log:
            if last is not None:
                if not (log_entry.timestamp_millis - last.timestamp_millis > 0):
                    raise RuntimeError("[BUG] Expected sorted snapshot log entries.")
            last = log_entry

        if not (len(self.snapshot_by_id) == 0 or self.current_snapshot_id in self.snapshot_by_id):
            raise RuntimeError("Invalid table metadata: Cannot find current version")

    @property
    def location(self: "TableMetadata") -> str:
        return self._location

    @property
    def metadata_location(self: "TableMetadata") -> Optional[str]:
        return self._file.location() if self._file else None

    @property
    def spec(self):
        return self.specs_by_id[self.default_spec_id]

    def spec_id(self, spec_id):
        if spec_id is None:
            spec_id = self.default_spec_id

        return self.specs_by_id[spec_id]

    def property_as_int(self, property_name, default_value):
        return int(self.properties.get(property_name, default_value))

    def current_snapshot(self):
        return self.snapshot_by_id[self.current_snapshot_id]

    def snapshot(self, snapshot_id):
        return self.snapshot_by_id[snapshot_id]

    def update_metadata_location(self, new_location):
        return TableMetadata(self.ops, None, new_location,
                             int(time.time() * 1000), self.last_column_id, self.schema, self.spec, self.properties,
                             self.current_snapshot_id, self.snapshots, self.snapshot_log)

    def update_schema(self, schema, last_column_id):
        PartitionSpec.check_compatibility(self.spec, schema)
        return TableMetadata(self.ops, None, self.location,
                             int(time.time() * 1000), last_column_id, schema, self.spec, self.properties,
                             self.current_snapshot_id, self.snapshots, self.snapshot_log)

    def add_snapshot(self, snapshot):
        new_snapshots = self.snapshots + snapshot
        new_snapshot_log = self.snapshot_log + [SnapshotLogEntry(snapshot.timestamp_millis, snapshot.snapshot_id)]

        return TableMetadata(self.ops, None, self.location,
                             int(time.time() * 1000), self.last_column_id, self.schema, self.spec, self.properties,
                             self.current_snapshot_id, new_snapshots, new_snapshot_log)

    def add_staged_snapshot(self, snapshot):
        self.snapshots.append(snapshot)
        return TableMetadata(self.ops, None, self.location, snapshot.timestamp_millis,
                             self.last_column_id, self.schema, self.default_spec_id, self.specs,
                             self.current_snapshot_id, self.snapshots, self.snapshot_log)

    def replace_current_snapshot(self, snapshot):
        self.snapshots.append(snapshot)
        self.snapshot_log.append(SnapshotLogEntry(snapshot.timestamp_millis, snapshot.snapshot_id))

        return TableMetadata(self.ops, None, self.location, snapshot.timestamp_millis,
                             self.last_column_id, self.schema, self.default_spec_id, self.specs,
                             self.current_snapshot_id, self.snapshots, self.snapshot_log)

    def remove_snapshots_if(self, remove_if):
        filtered = list()

        for snapshot in self.snapshots:
            if snapshot.snapshot_id == self.current_snapshot_id or not remove_if(snapshot):
                filtered.append(snapshot)

        valid_ids = [snapshot.snapshot_id for snapshot in filtered]
        new_snapshot_log = list()
        for log_entry in self.snapshot_log:
            if log_entry.snapshot_id in valid_ids:
                new_snapshot_log.append(log_entry)
            else:
                new_snapshot_log.clear()

        return TableMetadata(self.ops, None, self.location,
                             int(time.time() * 1000), self.last_column_id, self.schema, self.spec, self.properties,
                             self.current_snapshot_id, filtered, new_snapshot_log)

    def rollback_to(self, snapshot):
        ValidationException.check(snapshot.snapshot_id not in self.snapshot_by_id,
                                  "Cannot set current snapshot to unknown: %s", (snapshot.snapshot_id,))

        now_millis = int(time.time() * 1000)
        new_snapshot_log = self.snapshot_log + [SnapshotLogEntry(now_millis, snapshot.snapshot_id)]

        return TableMetadata(self.ops, None, self.location,
                             now_millis, self.last_column_id, self.schema, self.spec, self.properties,
                             snapshot.snapshot_id, self.snapshots, new_snapshot_log)

    def replace_properties(self, new_properties):
        ValidationException.check(new_properties is not None, "Cannot set properties to null")

        return TableMetadata(self.ops, None, self.location,
                             int(time.time() * 1000), self.last_column_id, self.schema, self.spec, new_properties,
                             self.current_snapshot_id, self.snapshots, self.snapshot_log)

    def remove_snapshot_log_entries(self, snapshot_ids):
        new_snapshot_log = list()

        for entry in self.snapshot_log:
            if entry.snapshot_id not in snapshot_ids:
                new_snapshot_log.append(entry)

        check_snapshot = self.current_snapshot_id < 0 or new_snapshot_log[-1].snapshot_id == self.current_snapshot_id
        ValidationException.check(check_snapshot,
                                  "Cannot set invalid snapshot log: latest entry is not the current snapshot")

        return TableMetadata(self.ops, None, self.location,
                             int(time.time() * 1000), self.last_column_id, self.schema, self.spec, self.properties,
                             self.current_snapshot_id, self.snapshots, new_snapshot_log)


class SnapshotLogEntry(object):

    def __init__(self, timestamp_millis, snapshot_id):
        self.timestamp_millis = timestamp_millis
        self.snapshot_id = snapshot_id

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return SnapshotLogEntry.__class__, self.snapshot_id, self.timestamp_millis

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        if other is None or not isinstance(other, SnapshotLogEntry):
            return False

        return self.snapshot_id == other.snapshot_id and self.timestamp_millis == other.timestamp_millis

    def __ne__(self, other):
        return not self.__eq__()
