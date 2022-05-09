#
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
#
import os

from hmsclient.genthrift.hive_metastore.ttypes import LockResponse, LockState, NoSuchObjectException
from iceberg.exceptions import AlreadyExistsException
from iceberg.hive import HiveTables
import mock
import pytest
from pytest import raises
from tests.api.test_helpers import MockHMSTable, MockManifest, MockManifestEntry, MockMetadata, MockReader, \
    MockSnapshot, MockTableOperations


@mock.patch("iceberg.hive.hive_tables.hmsclient")
def test_get_client(mock_hmsclient):
    conf = {"hive.metastore.uris": 'thrift://hms:123'}
    tables = HiveTables(conf)
    tables.get_client()
    mock_hmsclient.HMSClient.assert_called_with(iprot=None, oprot=None, host="hms", port=123)

    mock_iprot = mock.Mock()
    mock_oprot = mock.Mock()
    conf = {HiveTables.IPROT: mock_iprot, HiveTables.OPROT: mock_oprot}
    tables = HiveTables(conf)
    tables.get_client()
    mock_hmsclient.HMSClient.assert_called_with(iprot=mock_iprot, oprot=mock_oprot, host=None, port=None)


@mock.patch("iceberg.hive.HiveTableOperations.refresh_from_metadata_location")
@mock.patch("iceberg.hive.HiveTableOperations.current")
@mock.patch("iceberg.hive.HiveTables.get_client")
def test_load_tables_check_valid_props(client, current_call, refresh_call):
    parameters = {"table_type": "ICEBERG",
                  "partition_spec": [],
                  "metadata_location": "s3://path/to/iceberg.metadata.json"}

    client.return_value.__enter__.return_value.get_table.return_value = MockHMSTable(parameters)

    conf = {"hive.metastore.uris": 'thrift://hms:port'}
    tables = HiveTables(conf)
    tables.load("test.test_123")


@mock.patch("iceberg.hive.HiveTableOperations.refresh_from_metadata_location")
@mock.patch("iceberg.hive.HiveTableOperations.current")
@mock.patch("iceberg.hive.HiveTables.get_client")
def test_load_tables_check_missing_iceberg_type(client, current_call, refresh_call):
    parameters = {"partition_spec": [],
                  "metadata_location": "s3://path/to/iceberg.metdata.json"}

    client.return_value.__enter__.return_value.get_table.return_value = MockHMSTable(parameters)

    conf = {"hive.metastore.uris": 'thrift://hms:port'}
    tables = HiveTables(conf)
    with raises(RuntimeError):
        tables.load("test.test_123")


@mock.patch("iceberg.hive.HiveTableOperations.refresh_from_metadata_location")
@mock.patch("iceberg.hive.HiveTableOperations.current")
@mock.patch("iceberg.hive.HiveTables.get_client")
def test_load_tables_check_non_iceberg_type(client, current_call, refresh_call):
    parameters = {"table_type": "HIVE",
                  "partition_spec": [],
                  "metadata_location": "s3://path/to/iceberg.metdata.json"}

    client.return_value.__enter__.return_value.get_table.return_value = MockHMSTable(parameters)

    conf = {"hive.metastore.uris": 'thrift://hms:port'}
    tables = HiveTables(conf)
    with raises(RuntimeError):
        tables.load("test.test_123")


@mock.patch("iceberg.hive.HiveTableOperations.refresh_from_metadata_location")
@mock.patch("iceberg.hive.HiveTableOperations.current")
@mock.patch("iceberg.hive.HiveTables.get_client")
def test_load_tables_check_none_table_type(client, current_call, refresh_call):
    parameters = {"table_type": None,
                  "partition_spec": [],
                  "metadata_location": "s3://path/to/iceberg.metdata.json"}

    client.return_value.__enter__.return_value.get_table.return_value = MockHMSTable(parameters)

    conf = {"hive.metastore.uris": 'thrift://hms:port'}
    tables = HiveTables(conf)
    with raises(RuntimeError):
        tables.load("test.test_123")


@mock.patch("iceberg.hive.HiveTableOperations.refresh_from_metadata_location")
@mock.patch("iceberg.hive.HiveTableOperations.current")
@mock.patch("iceberg.hive.HiveTables.get_client")
def test_load_tables_check_no_location(client, current_call, refresh_call):
    parameters = {"table_type": "ICEBERG",
                  "partition_spec": []}

    client.return_value.__enter__.return_value.get_table.return_value = MockHMSTable(parameters)

    conf = {"hive.metastore.uris": 'thrift://hms:port'}
    tables = HiveTables(conf)
    with raises(RuntimeError):
        tables.load("test.test_123")


@mock.patch("iceberg.hive.HiveTableOperations.refresh_from_metadata_location")
@mock.patch("iceberg.hive.HiveTableOperations.current")
@mock.patch("iceberg.hive.HiveTables.get_client")
def test_load_tables_check_none_location(client, current_call, refresh_call):
    parameters = {"table_type": "ICEBERG",
                  "partition_spec": [],
                  "metadata_location": None}

    client.return_value.__enter__.return_value.get_table.return_value = MockHMSTable(parameters)

    conf = {"hive.metastore.uris": 'thrift://hms:port'}
    tables = HiveTables(conf)
    with raises(RuntimeError):
        tables.load("test.test_123")


@mock.patch("iceberg.hive.HiveTableOperations.refresh_from_metadata_location")
@mock.patch("iceberg.hive.HiveTableOperations.current")
@mock.patch("iceberg.hive.HiveTables.get_client")
def test_create_tables_failed(client, current_call, refresh_call, base_scan_schema, base_scan_partition, tmpdir):
    client.return_value.__enter__.return_value.get_table.side_effect = NoSuchObjectException()
    current_call.return_value = None
    conf = {"hive.metastore.uris": 'thrift://hms:port',
            "hive.metastore.warehouse.dir": tmpdir}
    tables = HiveTables(conf)
    with pytest.raises(AlreadyExistsException):
        tables.create(base_scan_schema, "test.test_123", base_scan_partition)
    assert len(os.listdir(os.path.join(tmpdir, "test.db", "test_123", "metadata"))) == 0


@mock.patch("iceberg.hive.HiveTableOperations.current")
@mock.patch("iceberg.hive.HiveTables.get_client")
def test_create_tables(client, current_call, base_scan_schema, base_scan_partition, tmpdir):

    client.return_value.__enter__.return_value.get_table.side_effect = NoSuchObjectException()
    current_call.return_value = None
    client.return_value.__enter__.return_value.lock.return_value = LockResponse("x", LockState.WAITING)
    client.return_value.__enter__.return_value.check_lock.return_value = LockResponse("x", LockState.ACQUIRED)
    tbl = client.return_value.__enter__.return_value.create_table.call_args_list
    conf = {"hive.metastore.uris": 'thrift://hms:port',
            "hive.metastore.warehouse.dir": tmpdir}
    tables = HiveTables(conf)
    tables.create(base_scan_schema, "test.test_123", base_scan_partition)

    client.return_value.__enter__.return_value.get_table.return_value = MockHMSTable(tbl[0].args[0].parameters)
    client.return_value.__enter__.return_value.get_table.side_effect = None
    current_call.return_value = tbl[0].args[0].parameters['metadata_location']

    tables.load("test.test_123")


@mock.patch("iceberg.hive.HiveTableOperations.refresh_from_metadata_location")
@mock.patch("iceberg.hive.HiveTableOperations.current")
@mock.patch("iceberg.hive.HiveTables.get_client")
def test_drop_tables(client, metadata, refresh_call, tmpdir):

    parameters = {"table_type": "ICEBERG",
                  "partition_spec": [],
                  "metadata_location": "s3://path/to/iceberg.metadata.json"}

    client.return_value.__enter__.return_value.get_table.return_value = MockHMSTable(parameters)
    conf = {"hive.metastore.uris": 'thrift://hms:port',
            "hive.metastore.warehouse.dir": tmpdir}
    tables = HiveTables(conf)
    tables.drop("test", "test_123", purge=False)
    client.return_value.__enter__.return_value.drop_table.assert_called_with("test", "test_123", deleteData=False)


@mock.patch("iceberg.hive.HiveTableOperations.refresh_from_metadata_location")
@mock.patch("iceberg.hive.HiveTables.new_table_ops")
@mock.patch("iceberg.hive.HiveTables.get_client")
def test_drop_tables_purge(client, current_ops, refresh_call, tmpdir):
    mock_snapshots = [MockSnapshot(location="snap-a.avro",
                                   manifests=[
                                       MockManifest("a-manifest.avro"),
                                       MockManifest("b-manifest.avro")],
                                   manifest_to_entries={
                                       "a-manifest.avro": MockReader(
                                           [MockManifestEntry("a.parquet"),
                                            MockManifestEntry("b.parquet")]),
                                       "b-manifest.avro": MockReader(
                                           [MockManifestEntry("c.parquet"),
                                            MockManifestEntry("d.parquet")])
                                   }),
                      MockSnapshot(location="snap-b.avro",
                                   manifests=[
                                       MockManifest("b-manifest.avro"),
                                       MockManifest("c-manifest.avro"),
                                       MockManifest("d-manifest.avro")],
                                   manifest_to_entries={
                                       "b-manifest.avro": MockReader(
                                           [MockManifestEntry("c.parquet"),
                                            MockManifestEntry("d.parquet")]),
                                       "c-manifest.avro": MockReader(
                                           [MockManifestEntry("e.parquet"),
                                            MockManifestEntry("f.parquet")]),
                                       "d-manifest.avro": MockReader(
                                           [MockManifestEntry("g.parquet"),
                                            MockManifestEntry("h.parquet")])
                                   })
                      ]
    ops = MockTableOperations(MockMetadata(mock_snapshots), "a.json")
    current_ops.return_value = ops

    parameters = {"table_type": "ICEBERG",
                  "partition_spec": [],
                  "metadata_location": "s3://path/to/iceberg.metadata.json"}
    client.return_value.__enter__.return_value.get_table.return_value = MockHMSTable(parameters)

    conf = {"hive.metastore.uris": 'thrift://hms:port',
            "hive.metastore.warehouse.dir": tmpdir}
    tables = HiveTables(conf)
    tables.drop("test", "test_123", purge=True)

    assert len(ops.deleted) == len(set(ops.deleted)), "Paths should only be deleted once"
    assert "a.json" in ops.deleted
    assert "snap-a.avro" in ops.deleted
    assert "snap-b.avro" in ops.deleted
    assert "a-manifest.avro" in ops.deleted
    assert "b-manifest.avro" in ops.deleted
    assert "c-manifest.avro" in ops.deleted
    assert "d-manifest.avro" in ops.deleted
    assert "a.parquet" in ops.deleted
    assert "b.parquet" in ops.deleted
    assert "c.parquet" in ops.deleted
    assert "d.parquet" in ops.deleted
    assert "e.parquet" in ops.deleted
    assert "f.parquet" in ops.deleted
    assert "g.parquet" in ops.deleted
    assert "h.parquet" in ops.deleted
