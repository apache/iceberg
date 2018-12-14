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

import time

from iceberg.api import (Filterable,
                         FilteredSnapshot,
                         Snapshot,
                         SnapshotIterable)
from iceberg.api.expressions import Expressions
from iceberg.api.io import CloseableGroup

from .manifest_reader import ManifestReader


class BaseSnapshot(Snapshot, SnapshotIterable, CloseableGroup):

    def __init__(self, ops, snapshot_id, parent_id, manifest_files, timestamp_millis=None):
        super(BaseSnapshot, self).__init__()
        if timestamp_millis is None:
            timestamp_millis = int(time.time() * 1000)
        self._ops = ops
        self._snapshot_id = snapshot_id
        self._timestamp_millis = timestamp_millis
        self._manifest_files = manifest_files
        self._adds = None
        self._deletes = None

    @property
    def snapshot_id(self):
        return self._snapshot_id

    @property
    def timestamp_millis(self):
        return self._timestamp_millis

    @property
    def manifests(self):
        return self._manifest_files

    def select(self, columns):
        return FilteredSnapshot(self, Expressions.always_true(), Expressions.always_true(), columns)

    def filter_partitions(self, expr):
        return FilteredSnapshot(self, expr, Expressions.always_true(), Filterable.ALL_COLUMNS)

    def filter_rows(self, expr):
        return FilteredSnapshot(self, Expressions.always_true(), expr, Filterable.ALL_COLUMNS)

    def iterator(self, part_filter=None, row_filter=None, columns=None):
        if part_filter is None and row_filter is None and columns is None:
            return self.iterator(Expressions.always_true(), Expressions.always_true(), Filterable.ALL_COLUMNS)

        return iter([self.get_filtered_manifest(path, part_filter, row_filter, columns)
                     for path in self._manifest_files])

    def added_files(self):
        raise RuntimeError("Not yet implemented")

    def deleted_files(self):
        raise RuntimeError("Not yet implemented")

    def cache_changes(self):
        raise RuntimeError("Not yet implemented")

    def __repr__(self):
        return "BaseSnapshot(id={id},timestamp_ms={ts_ms},manifests={manifests}".format(id=self._snapshot_id,
                                                                                        ts_ms=self._timestamp_millis,
                                                                                        manifests=self._manifest_files)

    def __str__(self):
        return self.__repr__()

    def get_filtered_manifest(self, path, part_filter, row_filter, columns):
        reader = ManifestReader.read(self.ops.new_input_file(path))
        self.add_closeable(reader)
        return reader
