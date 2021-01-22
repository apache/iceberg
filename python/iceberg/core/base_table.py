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

from iceberg.api import Table

from .data_table_scan import DataTableScan
from .schema_update import SchemaUpdate


class BaseTable(Table):

    def __init__(self, ops, name):
        self.ops = ops
        self.name = name

    def refresh(self):
        self.ops.refresh()

    def new_scan(self):
        return DataTableScan(self.ops, self)

    def schema(self):
        return self.ops.current().schema

    def spec(self):
        return self.ops.current().spec

    def properties(self):
        return self.ops.current().properties

    def location(self):
        return self.ops.current().location

    def current_snapshot(self):
        return self.ops.current().current_snapshot()

    def snapshots(self):
        return self.ops.current().snapshots

    def snapshots_with_summary_property(self, prop_key, prop_val):
        if prop_key is None:
            raise RuntimeError("Property Key cannot be None: (%s, %s)" % (prop_key, prop_val))

        for snapshot in self.ops.current().snapshots:
            if prop_key in snapshot.summary.keys() and snapshot.summary.get(prop_key) == prop_val:
                yield snapshot

    def update_schema(self):
        return SchemaUpdate(self.ops)

    def update_properties(self):
        # PropertiesUpdate(self.ops)
        raise NotImplementedError()

    def update_location(self):
        # SetLocation(self.ops)
        raise NotImplementedError()

    def new_append(self):
        # MergeAppend(ops)
        raise NotImplementedError()

    def new_fast_append(self):
        # FastAppend(ops)
        raise NotImplementedError()

    def new_rewrite(self):
        # ReplaceFiles(ops)
        raise NotImplementedError()

    def new_overwrite(self):
        # OverwriteData(ops)
        raise NotImplementedError()

    def new_replace_partitions(self):
        # ReplacePartitionsOperation(ops)
        raise NotImplementedError()

    def new_delete(self):
        # StreamingDelete(ops)
        raise NotImplementedError()

    def expire_snapshots(self):
        # RemoveSnapshots(ops)
        raise NotImplementedError()

    def rollback(self):
        # RollbackToSnapshot(ops)
        raise NotImplementedError()

    def new_transaction(self):
        # BaseTransaction.newTransaction(ops)
        raise NotImplementedError()

    def __str__(self):
        return self.name
