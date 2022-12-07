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

import os
import random
import tempfile
import time

from iceberg.api import Files, PartitionSpec, PartitionSpecBuilder, Schema
from iceberg.api.types import BooleanType, IntegerType, LongType, NestedField, StringType
from iceberg.core import (BaseSnapshot,
                          BaseTable,
                          ConfigProperties,
                          GenericManifestFile,
                          SnapshotLogEntry,
                          TableMetadata,
                          TableMetadataParser,
                          TableOperations,
                          TableProperties)
from iceberg.exceptions import AlreadyExistsException, CommitFailedException
import pytest

SCHEMA = Schema([NestedField.optional(1, "b", BooleanType.get())])
METADATA = dict()
VERSIONS = dict()


class LocalTableOperations(TableOperations):

    def current(self):
        raise RuntimeError("Not implemented for tests")

    def refresh(self):
        raise RuntimeError("Not implemented for tests")

    def commit(self, base, metadata):
        raise RuntimeError("Not implemented for tests")

    def new_input_file(self, path):
        return Files.local_input(path)

    def new_metadata_file(self, filename):
        return Files.local_output(tempfile.mkstemp(prefix=filename))

    def delete_file(self, path):
        if os.path.exists(path):
            os.remove(path)

    def new_snapshot_id(self):
        raise RuntimeError("Not implemented for tests")


def create(temp, name, schema, spec):
    ops = TestTableOperations(name, temp)
    if ops.current() is not None:
        raise AlreadyExistsException("Table %s already exists at location: %s" % (name, temp))
    ops.commit(None, TableMetadata.new_table_metadata(ops, schema, spec, str(temp)))
    return TestTable(ops, name)


def begin_create(temp, name, schema, spec):
    raise RuntimeError("Not yet implemented")
    # ops = TestTableOperations(name, temp)
    # if ops.current() is None:
    #     raise AlreadyExistsException("Table %s already exists at location: %s" % (name, temp))
    #
    # metadata = TableMetadata.new_table_metadata(ops, schema, spec, str(temp))
    # return BaseTransaction.create_table_transaction(ops, metadata)


class TestTable(BaseTable):
    def __init__(self, ops, name):
        super(TestTable, self).__init__(ops, name)
        self.ops = ops


class TestTableOperations(TableOperations):

    def __init__(self, table_name, location):
        self.last_snapshot_id = 0
        self._fail_commits = 0
        self.table_name = table_name
        self.metadata = os.path.join(location, "metadata")
        os.makedirs(self.metadata)
        self._current = None
        self.refresh()
        if self._current is not None:
            for snap in self.current().snapshots:
                self.last_snapshot_id = max(self.last_snapshot_id, snap.snapshot_id)

    def current(self):
        return self._current

    def refresh(self):
        self._current = METADATA.get(self.table_name)
        return self._current

    def commit(self, base, metadata):
        if base != self.current():
            raise RuntimeError("Cannot commit changes based on stale metadata")

        self.refresh()
        if base == self.current():
            if self._fail_commits > 0:
                self._fail_commits - 1
                raise RuntimeError("Injected failure")
            version = VERSIONS.get(self.table_name)
            VERSIONS[self.table_name] = 0 if version is None else version + 1
            METADATA[self.table_name] = metadata
            self._current = metadata
        else:
            raise CommitFailedException("Commit failed: table was updated at %s", self.current.last_updated_millis)

    def new_input_file(self, path):
        return Files.local_input(path)

    def new_metadata_file(self, filename):
        return Files.local_output(os.path.join(self.metadata, filename))

    def delete_file(self, path):
        if not os.remove(path):
            raise RuntimeError("Failed to delete file: %s" % path)

    def new_snapshot_id(self):
        next_snapshot_id = self.last_snapshot_id + 1
        self.last_snapshot_id = next_snapshot_id
        return next_snapshot_id


class TestTables(object):
    @staticmethod
    def create(temp, name, schema, spec):
        ops = TestTableOperations(name, temp)

        if ops.current() is not None:
            raise RuntimeError("Table %s already exists at location: %s" % (name, temp))

        ops.commit(None, TableMetadata.new_table_metadata(ops, schema, spec, str(temp)))
        return TestTable(ops, name)


@pytest.fixture(scope="session")
def expected():
    return TableMetadata.new_table_metadata(None, SCHEMA, PartitionSpec.unpartitioned(), "file://tmp/db/table")


@pytest.fixture(scope="session",
                params=[True, False])
def prop(request):
    config = {ConfigProperties.COMPRESS_METADATA: request.param}
    yield request.param

    if os.path.exists(TableMetadataParser.get_file_extension(config)):
        os.remove(TableMetadataParser.get_file_extension(config))


@pytest.fixture(scope="session")
def ops():
    return LocalTableOperations()


@pytest.fixture(scope="session")
def expected_metadata():
    spec_schema = Schema(NestedField.required(1, "x", LongType.get()),
                         NestedField.required(2, "y", LongType.get()),
                         NestedField.required(3, "z", LongType.get()))
    spec = PartitionSpec \
        .builder_for(spec_schema) \
        .with_spec_id(5) \
        .build()

    random.seed(1234)
    previous_snapshot_id = int(time.time()) - random.randint(0, 3600)

    previous_snapshot = BaseSnapshot(None, previous_snapshot_id, None,
                                     timestamp_millis=previous_snapshot_id,
                                     manifests=[GenericManifestFile(file=Files.local_input("file:/tmp/manfiest.1.avro"),
                                                                    spec_id=spec.spec_id)])

    current_snapshot_id = int(time.time())
    current_snapshot = BaseSnapshot(None, current_snapshot_id, previous_snapshot_id,
                                    timestamp_millis=current_snapshot_id,
                                    manifests=[GenericManifestFile(file=Files.local_input("file:/tmp/manfiest.2.avro"),
                                                                   spec_id=spec.spec_id)])

    snapshot_log = [SnapshotLogEntry(previous_snapshot.timestamp_millis, previous_snapshot.snapshot_id),
                    SnapshotLogEntry(current_snapshot.timestamp_millis, current_snapshot.snapshot_id)]

    return TableMetadata(ops, None, "s3://bucket/test/location",
                         int(time.time()), 3, spec_schema, 5, [spec], {"property": "value"}, current_snapshot_id,
                         [previous_snapshot, current_snapshot], snapshot_log)


@pytest.fixture(scope="session")
def expected_metadata_sorting():
    spec_schema = Schema(NestedField.required(1, "x", LongType.get()),
                         NestedField.required(2, "y", LongType.get()),
                         NestedField.required(3, "z", LongType.get()))

    spec = PartitionSpec \
        .builder_for(spec_schema) \
        .with_spec_id(5) \
        .build()

    random.seed(1234)
    previous_snapshot_id = int(time.time()) - random.randint(0, 3600)

    previous_snapshot = BaseSnapshot(ops, previous_snapshot_id, None,
                                     timestamp_millis=previous_snapshot_id,
                                     manifests=[GenericManifestFile(file=Files.local_input("file:/tmp/manfiest.1.avro"),
                                                                    spec_id=spec.spec_id)])

    current_snapshot_id = int(time.time())
    current_snapshot = BaseSnapshot(ops, current_snapshot_id, previous_snapshot_id,
                                    timestamp_millis=current_snapshot_id,
                                    manifests=[GenericManifestFile(file=Files.local_input("file:/tmp/manfiest.2.avro"),
                                                                   spec_id=spec.spec_id)])

    reversed_snapshot_log = list()
    metadata = TableMetadata(ops, None, "s3://bucket/test/location",
                             int(time.time()), 3, spec_schema, 5, [spec], {"property": "value"}, current_snapshot_id,
                             [previous_snapshot, current_snapshot], reversed_snapshot_log)

    reversed_snapshot_log.append(SnapshotLogEntry(current_snapshot.timestamp_millis, current_snapshot.snapshot_id))
    reversed_snapshot_log.append(SnapshotLogEntry(previous_snapshot.timestamp_millis, previous_snapshot.snapshot_id))

    return metadata


@pytest.fixture(scope="session")
def missing_spec_list():
    schema = Schema(NestedField.required(1, "x", LongType.get()),
                    NestedField.required(2, "y", LongType.get()),
                    NestedField.required(3, "z", LongType.get()))

    spec = PartitionSpec.builder_for(schema).identity("x").with_spec_id(6).build()
    random.seed(1234)
    previous_snapshot_id = int(time.time()) - random.randint(0, 3600)

    previous_snapshot = BaseSnapshot(ops, previous_snapshot_id, None,
                                     timestamp_millis=previous_snapshot_id,
                                     manifests=[GenericManifestFile(file=Files.local_input("file:/tmp/manfiest.1.avro"),
                                                                    spec_id=spec.spec_id)])

    current_snapshot_id = int(time.time())
    current_snapshot = BaseSnapshot(ops, current_snapshot_id, previous_snapshot_id,
                                    timestamp_millis=current_snapshot_id,
                                    manifests=[GenericManifestFile(file=Files.local_input("file:/tmp/manfiest.2.avro"),
                                                                   spec_id=spec.spec_id)])
    return TableMetadata(ops, None, "s3://bucket/test/location", int(time.time()), 3, schema, 6,
                         (spec,), {"property": "value"}, current_snapshot_id, [previous_snapshot, current_snapshot],
                         [])


@pytest.fixture(scope="session")
def snapshot_manifests():
    return (GenericManifestFile(file=Files.local_input("file:/tmp/manifest1.avro"), spec_id=0),
            GenericManifestFile(file=Files.local_input("file:/tmp/manifest2.avro"), spec_id=0))


@pytest.fixture(scope="session")
def expected_base_snapshot():
    return BaseSnapshot(LocalTableOperations(), int(time.time()), manifests=["file:/tmp/manfiest.1.avro",
                                                                             "file:/tmp/manfiest.2.avro"])


@pytest.fixture(scope="session")
def base_scan_schema():
    return Schema([NestedField.required(1, "id", IntegerType.get()),
                   NestedField.required(2, "data", StringType.get())])


@pytest.fixture(scope="session", params=["none", "one"])
def base_scan_partition(base_scan_schema, request):
    if request.param == "none":
        spec = PartitionSpec.unpartitioned()
    else:
        spec = PartitionSpecBuilder(base_scan_schema).add(1, 10000, "id", "identity").build()
    return spec


@pytest.fixture(scope="session")
def ts_table(base_scan_schema, base_scan_partition):
    with tempfile.TemporaryDirectory() as td:
        return TestTables.create(td, "test-" + str(len(base_scan_partition.fields)), base_scan_schema,
                                 base_scan_partition)


@pytest.fixture(scope="session")
def split_planning_table(base_scan_schema):
    from iceberg.core.filesystem import FilesystemTables
    tables = FilesystemTables()

    with tempfile.TemporaryDirectory() as td:
        table = tables.create(base_scan_schema, table_identifier=td)
        table.properties().update({TableProperties.SPLIT_SIZE: "{}".format(128 * 1024 * 1024),
                                   TableProperties.SPLIT_OPEN_FILE_COST: "{}".format(4 * 1024 * 1024),
                                   TableProperties.SPLIT_LOOKBACK: "{}".format(2 ** 31 - 1)})

        return table
