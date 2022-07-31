#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
# pylint: disable=protected-access,redefined-outer-name
from unittest.mock import MagicMock

import pytest
from hive_metastore.ttypes import AlreadyExistsException
from hive_metastore.ttypes import Database as HiveDatabase
from hive_metastore.ttypes import (
    FieldSchema,
    InvalidOperationException,
    MetaException,
    NoSuchObjectException,
    SerDeInfo,
    SkewedInfo,
    StorageDescriptor,
)
from hive_metastore.ttypes import Table as HiveTable

from pyiceberg.catalog.base import PropertiesUpdateSummary
from pyiceberg.catalog.hive import HiveCatalog
from pyiceberg.exceptions import (
    AlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
)
from pyiceberg.schema import Schema

HIVE_CATALOG_NAME = "hive"
HIVE_METASTORE_FAKE_URL = "thrift://unknown:9083"


@pytest.fixture
def hive_table() -> HiveTable:
    return HiveTable(
        tableName="new_tabl2e",
        dbName="default",
        owner="fokkodriesprong",
        createTime=1659092339,
        lastAccessTime=1659092,
        retention=0,
        sd=StorageDescriptor(
            cols=[
                FieldSchema(name="foo", type="string", comment=None),
                FieldSchema(name="bar", type="int", comment=None),
                FieldSchema(name="baz", type="boolean", comment=None),
            ],
            location="file:/tmp/new_tabl2e",
            inputFormat="org.apache.hadoop.mapred.FileInputFormat",
            outputFormat="org.apache.hadoop.mapred.FileOutputFormat",
            compressed=False,
            numBuckets=0,
            serdeInfo=SerDeInfo(
                name=None,
                serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                parameters={},
                description=None,
                serializerClass=None,
                deserializerClass=None,
                serdeType=None,
            ),
            bucketCols=[],
            sortCols=[],
            parameters={},
            skewedInfo=SkewedInfo(skewedColNames=[], skewedColValues=[], skewedColValueLocationMaps={}),
            storedAsSubDirectories=False,
        ),
        partitionKeys=[],
        parameters={"EXTERNAL": "TRUE", "transient_lastDdlTime": "1659092339"},
        viewOriginalText=None,
        viewExpandedText=None,
        tableType="EXTERNAL_TABLE",
        privileges=None,
        temporary=False,
        rewriteEnabled=False,
        creationMetadata=None,
        catName="hive",
        ownerType=1,
        writeId=-1,
        isStatsCompliant=None,
        colStats=None,
        accessType=None,
        requiredReadCapabilities=None,
        requiredWriteCapabilities=None,
        id=None,
        fileMetadata=None,
        dictionary=None,
        txnId=None,
    )


@pytest.fixture
def hive_database() -> HiveDatabase:
    return HiveDatabase(
        name="default",
        description=None,
        locationUri="file:/tmp/default2.db",
        parameters={"test": "property"},
        privileges=None,
        ownerName=None,
        ownerType=1,
        catalogName="hive",
        createTime=None,
        managedLocationUri=None,
        type=None,
        connector_name=None,
        remote_dbname=None,
    )


def test_check_number_of_namespaces(table_schema_simple: Schema):
    catalog = HiveCatalog(HIVE_CATALOG_NAME, {}, url=HIVE_METASTORE_FAKE_URL)

    with pytest.raises(ValueError):
        catalog.create_table(("default", "namespace", "table"), schema=table_schema_simple)

    with pytest.raises(ValueError):
        catalog.create_table("default.namespace.table", schema=table_schema_simple)

    with pytest.raises(ValueError):
        catalog.create_table(("table",), schema=table_schema_simple)

    with pytest.raises(ValueError):
        catalog.create_table("table", schema=table_schema_simple)


def test_create_table(table_schema_simple: Schema, hive_table: HiveTable):
    catalog = HiveCatalog(HIVE_CATALOG_NAME, {}, url=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().create_table.return_value = hive_table
    catalog._client.__enter__().get_table.return_value = hive_table

    catalog.create_table(("default", "table"), schema=table_schema_simple)

    catalog._client.__enter__().create_table.assert_called_with(
        HiveTable(
            tableName="table",
            dbName="default",
            owner="fokkodriesprong",
            createTime=1659296,
            lastAccessTime=1659296,
            retention=None,
            sd=StorageDescriptor(
                cols=[
                    FieldSchema(name="foo", type="string", comment=None),
                    FieldSchema(name="bar", type="int", comment=None),
                    FieldSchema(name="baz", type="boolean", comment=None),
                ],
                location=None,
                inputFormat="org.apache.hadoop.mapred.FileInputFormat",
                outputFormat="org.apache.hadoop.mapred.FileOutputFormat",
                compressed=None,
                numBuckets=None,
                serdeInfo=SerDeInfo(
                    name=None,
                    serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                    parameters=None,
                    description=None,
                    serializerClass=None,
                    deserializerClass=None,
                    serdeType=None,
                ),
                bucketCols=None,
                sortCols=None,
                parameters=None,
                skewedInfo=None,
                storedAsSubDirectories=None,
            ),
            partitionKeys=None,
            parameters={"EXTERNAL": "TRUE", "table_type": "ICEBERG", "metadata_location": "s3://"},
            viewOriginalText=None,
            viewExpandedText=None,
            tableType="EXTERNAL_TABLE",
            privileges=None,
            temporary=False,
            rewriteEnabled=None,
            creationMetadata=None,
            catName=None,
            ownerType=1,
            writeId=-1,
            isStatsCompliant=None,
            colStats=None,
            accessType=None,
            requiredReadCapabilities=None,
            requiredWriteCapabilities=None,
            id=None,
            fileMetadata=None,
            dictionary=None,
            txnId=None,
        )
    )


def test_load_table(hive_table: HiveTable):
    catalog = HiveCatalog(HIVE_CATALOG_NAME, {}, url=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_table.return_value = hive_table
    catalog.load_table(("default", "table"))


def test_rename_table_from_does_not_exists():
    catalog = HiveCatalog(HIVE_CATALOG_NAME, {}, url=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().alter_table.side_effect = NoSuchObjectException(
        message="hive.default.does_not_exists table not found"
    )

    with pytest.raises(NoSuchTableError) as exc_info:
        catalog.rename_table(("default", "does_not_exists"), ("default", "new_table"))

    assert "Table does not exist: does_not_exists" in str(exc_info.value)


def test_rename_table_to_namespace_does_not_exists():
    catalog = HiveCatalog(HIVE_CATALOG_NAME, {}, url=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().alter_table.side_effect = InvalidOperationException(
        message="Unable to change partition or table. Database default does not exist Check metastore logs for detailed stack.does_not_exists"
    )

    with pytest.raises(NoSuchNamespaceError) as exc_info:
        catalog.rename_table(("default", "does_exists"), ("default_does_not_exists", "new_table"))

    assert "Database does not exists: default_does_not_exists" in str(exc_info.value)


def test_drop_database_does_not_empty():
    catalog = HiveCatalog(HIVE_CATALOG_NAME, {}, url=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().drop_database.side_effect = InvalidOperationException(
        message="Database not_empty is not empty. One or more tables exist."
    )

    with pytest.raises(NamespaceNotEmptyError) as exc_info:
        catalog.drop_namespace(("not_empty",))

    assert "Database not_empty is not empty" in str(exc_info.value)


def test_drop_database_does_not_exists():
    catalog = HiveCatalog(HIVE_CATALOG_NAME, {}, url=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().drop_database.side_effect = MetaException(message="java.lang.NullPointerException")

    with pytest.raises(NoSuchNamespaceError) as exc_info:
        catalog.drop_namespace(("does_not_exists",))

    assert "Database does not exists: does_not_exists" in str(exc_info.value)


def test_list_tables():
    catalog = HiveCatalog(HIVE_CATALOG_NAME, {}, url=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_all_tables.return_value = ["table1", "table2"]

    assert catalog.list_tables("database") == [
        (
            "database",
            "table1",
        ),
        (
            "database",
            "table2",
        ),
    ]


def test_list_namespaces():
    catalog = HiveCatalog(HIVE_CATALOG_NAME, {}, url=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_all_databases.return_value = ["namespace1", "namespace2"]

    assert catalog.list_namespaces() == [("namespace1",), ("namespace2",)]


def test_drop_table():
    catalog = HiveCatalog(HIVE_CATALOG_NAME, {}, url=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()

    catalog.drop_table(("default", "table"))


def test_drop_table_does_not_exists():
    catalog = HiveCatalog(HIVE_CATALOG_NAME, {}, url=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().drop_table.side_effect = NoSuchObjectException(message="does_not_exists")

    with pytest.raises(NoSuchTableError) as exc_info:
        catalog.drop_table(("default", "does_not_exists"))

    assert "Table does not exists: does_not_exists" in str(exc_info.value)


def test_purge_table():
    catalog = HiveCatalog(HIVE_CATALOG_NAME, {}, url=HIVE_METASTORE_FAKE_URL)

    with pytest.raises(NotImplementedError):
        catalog.purge_table(("default", "does_not_exists"))


def test_create_database():
    catalog = HiveCatalog(HIVE_CATALOG_NAME, {}, url=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog.create_namespace("default", {"property": "true"})


def test_create_database_already_exists():
    catalog = HiveCatalog(HIVE_CATALOG_NAME, {}, url=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().create_database.side_effect = AlreadyExistsException(message="Database default already exists")

    with pytest.raises(AlreadyExistsError) as exc_info:
        catalog.create_namespace("default")

    assert "Database default already exists" in str(exc_info.value)


def test_load_namespace_properties(hive_database: HiveDatabase):
    catalog = HiveCatalog(HIVE_CATALOG_NAME, {}, url=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_database.return_value = hive_database

    assert catalog.load_namespace_properties("default2") == {"location": "file:/tmp/default2.db", "test": "property"}


def test_load_namespace_properties_does_not_exists():
    catalog = HiveCatalog(HIVE_CATALOG_NAME, {}, url=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_database.side_effect = NoSuchObjectException(message="does_not_exists")

    with pytest.raises(NoSuchNamespaceError) as exc_info:
        catalog.load_namespace_properties(("does_not_exists",))

    assert "Database does not exists: does_not_exists" in str(exc_info.value)


def test_update_namespace_properties(hive_database: HiveDatabase):
    catalog = HiveCatalog(HIVE_CATALOG_NAME, {}, url=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_database.return_value = hive_database

    assert catalog.update_namespace_properties(
        namespace="default", removals={"test", "does_not_exists"}, updates={"label": "core"}
    ) == PropertiesUpdateSummary(removed=["test"], updated=["label"], missing=["does_not_exists"])


def test_update_namespace_properties_namespace_does_not_exists():
    catalog = HiveCatalog(HIVE_CATALOG_NAME, {}, url=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_database.side_effect = NoSuchObjectException(message="does_not_exists")

    with pytest.raises(NoSuchNamespaceError) as exc_info:
        catalog.update_namespace_properties(("does_not_exists",), removals=set(), updates={})

    assert "Database does not exists: does_not_exists" in str(exc_info.value)
