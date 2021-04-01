# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import io
import pickle

from iceberg.api.data_file import DataFile
from iceberg.api.expressions.expressions import ExpressionVisitors
from iceberg.api.expressions.predicate import (BoundPredicate,
                                               UnboundPredicate)
from iceberg.api.struct_like import StructLike
from nose.tools import assert_true


class TestHelpers(object):

    @staticmethod
    def assert_all_references_bound(message, expr):
        ExpressionVisitors.visit(expr, TestHelpers.CheckReferencesBound(message))

    @staticmethod
    def assert_and_unwrap(expr, expected=None):
        if expected is not None:
            assert_true(isinstance(expr, expected))
        else:
            assert_true(isinstance(expr, BoundPredicate))

        return expr

    @staticmethod
    def round_trip_serialize(type_var):
        stream = io.BytesIO()
        pickle.dump(type_var, stream, pickle.HIGHEST_PROTOCOL)
        stream.seek(0)

        return pickle.load(stream)

    class Row(StructLike):

        @staticmethod
        def of(values=None):
            return TestHelpers.Row(values)

        def __init__(self, values):
            self.values = values

        def get(self, pos):
            return self.values[pos]

        def set(self, pos, value):
            raise RuntimeError("Setting values is not supported")

    class CheckReferencesBound(ExpressionVisitors.ExpressionVisitor):

        def __init__(self, message):
            self.message = message

        def predicate(self, pred):
            if isinstance(pred, UnboundPredicate):
                assert_true(False)

            return None


class MockDataFile(DataFile):

    def __init__(self, path, partition, record_count, value_counts=None, null_value_counts=None,
                 lower_bounds=None, upper_bounds=None):
        self.path = path
        self.partition = partition
        self.record_count = record_count
        self.value_counts = value_counts
        self.null_value_counts = null_value_counts
        self.lower_bounds = lower_bounds
        self.upper_bounds = upper_bounds
        self.file_size_in_bytes = 0
        self.block_size_in_bytes = 0
        self.file_ordinal = None
        self.column_size = None

    def copy(self):
        return self


class MockHMSTable(object):
    def __init__(self, params):
        self.parameters = params


class MockManifestEntry(object):
    def __init__(self, path):
        self._path = path

    def path(self):
        return self._path


class MockReader(object):
    def __init__(self, entries):
        self._entries = entries

    def iterator(self):
        return iter(self._entries)


class MockTableOperations(object):
    def __init__(self, metadata, location):
        self.metadata = metadata
        self.current_metadata_location = location
        self.deleted = []

    def current(self):
        return self.metadata

    def delete_file(self, path):
        self.deleted.append(path)


class MockManifest(object):
    def __init__(self, manifest_path):
        self.manifest_path = manifest_path


class MockSnapshot(object):
    def __init__(self, location, manifests, manifest_to_entries):
        self._location = location
        self._manifests = manifests
        self._manifest_to_entries = manifest_to_entries

    @property
    def manifests(self):
        return iter(self._manifests)

    @property
    def manifest_location(self):
        return self._location

    def get_filtered_manifest(self, path):
        return self._manifest_to_entries[path]


class MockMetadata(object):
    def __init__(self, snapshots):
        self.snapshots = snapshots
