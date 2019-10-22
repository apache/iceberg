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

from iceberg.hive import HiveTables
import mock
from pytest import raises


class TestHMSTable(object):
    def __init__(self, params):
        self.parameters = params


@mock.patch("iceberg.hive.HiveTableOperations.refresh_from_metadata_location")
@mock.patch("iceberg.hive.HiveTableOperations.current")
@mock.patch("iceberg.hive.HiveTables.get_client")
def test_load_tables_check_valid_props(client, current_call, refresh_call):
    parameters = {"table_type": "ICEBERG",
                  "partition_spec": [],
                  "metadata_location": "s3://path/to/iceberg.metadata.json"}

    client.return_value.__enter__.return_value.get_table.return_value = TestHMSTable(parameters)

    conf = {"hive.metastore.uris": 'thrift://hms:port'}
    tables = HiveTables(conf)
    tables.load("test.test_123")


@mock.patch("iceberg.hive.HiveTableOperations.refresh_from_metadata_location")
@mock.patch("iceberg.hive.HiveTableOperations.current")
@mock.patch("iceberg.hive.HiveTables.get_client")
def test_load_tables_check_missing_iceberg_type(client, current_call, refresh_call):
    parameters = {"partition_spec": [],
                  "metadata_location": "s3://path/to/iceberg.metdata.json"}

    client.return_value.__enter__.return_value.get_table.return_value = TestHMSTable(parameters)

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

    client.return_value.__enter__.return_value.get_table.return_value = TestHMSTable(parameters)

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

    client.return_value.__enter__.return_value.get_table.return_value = TestHMSTable(parameters)

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

    client.return_value.__enter__.return_value.get_table.return_value = TestHMSTable(parameters)

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

    client.return_value.__enter__.return_value.get_table.return_value = TestHMSTable(parameters)

    conf = {"hive.metastore.uris": 'thrift://hms:port'}
    tables = HiveTables(conf)
    with raises(RuntimeError):
        tables.load("test.test_123")
