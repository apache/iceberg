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
import json
import uuid
from typing import Any, Dict
from unittest.mock import MagicMock, patch

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

from pyiceberg.catalog import PropertiesUpdateSummary
from pyiceberg.catalog.hive import HiveCatalog, _construct_hive_storage_descriptor
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import ToOutputFile
from pyiceberg.table.metadata import TableMetadataUtil, TableMetadataV2
from pyiceberg.table.refs import SnapshotRef, SnapshotRefType
from pyiceberg.table.snapshots import (
    MetadataLogEntry,
    Operation,
    Snapshot,
    SnapshotLogEntry,
    Summary,
)
from pyiceberg.table.sorting import (
    NullOrder,
    SortDirection,
    SortField,
    SortOrder,
)
from pyiceberg.transforms import BucketTransform, IdentityTransform
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
)
from tests.conftest import LocalFileIO

HIVE_CATALOG_NAME = "hive"
HIVE_METASTORE_FAKE_URL = "thrift://unknown:9083"


@pytest.fixture
def hive_table(tmp_path_factory: pytest.TempPathFactory, example_table_metadata_v2: Dict[str, Any]) -> HiveTable:
    metadata_path = str(tmp_path_factory.mktemp("metadata") / f"{uuid.uuid4()}.metadata.json")
    metadata = TableMetadataV2(**example_table_metadata_v2)
    ToOutputFile.table_metadata(metadata, LocalFileIO().new_output(str(metadata_path)), True)

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
        parameters={
            "EXTERNAL": "TRUE",
            "transient_lastDdlTime": "1659092339",
            "table_type": "ICEBERG",
            "metadata_location": metadata_path,
        },
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


@pytest.fixture(scope="session")
def hive_database(tmp_path_factory: pytest.TempPathFactory) -> HiveDatabase:
    # Pre-create the directory, this has to be done because
    # of a local FS. Not needed with an actual object store.
    database_path = tmp_path_factory.mktemp("database")
    manifest_path = database_path / "database" / "table" / "metadata"
    manifest_path.mkdir(parents=True)
    return HiveDatabase(
        name="default",
        description=None,
        locationUri=str(database_path / "database"),
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


def test_no_uri_supplied() -> None:
    with pytest.raises(KeyError):
        HiveCatalog("production")


def test_check_number_of_namespaces(table_schema_simple: Schema) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    with pytest.raises(ValueError):
        catalog.create_table(("default", "namespace", "table"), schema=table_schema_simple)

    with pytest.raises(ValueError):
        catalog.create_table("default.namespace.table", schema=table_schema_simple)

    with pytest.raises(ValueError):
        catalog.create_table(("table",), schema=table_schema_simple)

    with pytest.raises(ValueError):
        catalog.create_table("table", schema=table_schema_simple)


@patch("time.time", MagicMock(return_value=12345))
def test_create_table(table_schema_simple: Schema, hive_database: HiveDatabase, hive_table: HiveTable) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().create_table.return_value = None
    catalog._client.__enter__().get_table.return_value = hive_table
    catalog._client.__enter__().get_database.return_value = hive_database
    catalog.create_table(("default", "table"), schema=table_schema_simple, properties={"owner": "javaberg"})

    called_hive_table: HiveTable = catalog._client.__enter__().create_table.call_args[0][0]
    # This one is generated within the function itself, so we need to extract
    # it to construct the assert_called_with
    metadata_location: str = called_hive_table.parameters["metadata_location"]
    assert metadata_location.endswith(".metadata.json")
    assert "/database/table/metadata/" in metadata_location
    catalog._client.__enter__().create_table.assert_called_with(
        HiveTable(
            tableName="table",
            dbName="default",
            owner="javaberg",
            createTime=12345,
            lastAccessTime=12345,
            retention=None,
            sd=StorageDescriptor(
                cols=[
                    FieldSchema(name="foo", type="string", comment=None),
                    FieldSchema(name="bar", type="int", comment=None),
                    FieldSchema(name="baz", type="boolean", comment=None),
                ],
                location=f"{hive_database.locationUri}/table",
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
            parameters={"EXTERNAL": "TRUE", "table_type": "ICEBERG", "metadata_location": metadata_location},
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

    with open(metadata_location, encoding="utf-8") as f:
        payload = json.load(f)

    metadata = TableMetadataUtil.parse_obj(payload)

    assert "database/table" in metadata.location

    expected = TableMetadataV2(
        location=metadata.location,
        table_uuid=metadata.table_uuid,
        last_updated_ms=metadata.last_updated_ms,
        last_column_id=3,
        schemas=[
            Schema(
                NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
                NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
                schema_id=0,
                identifier_field_ids=[2],
            )
        ],
        current_schema_id=0,
        last_partition_id=1000,
        properties={"owner": "javaberg"},
        partition_specs=[PartitionSpec()],
        default_spec_id=0,
        current_snapshot_id=None,
        snapshots=[],
        snapshot_log=[],
        metadata_log=[],
        sort_orders=[SortOrder(order_id=0)],
        default_sort_order_id=0,
        refs={},
        format_version=2,
        last_sequence_number=0,
    )

    assert metadata.dict() == expected.dict()


def test_load_table(hive_table: HiveTable) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_table.return_value = hive_table
    table = catalog.load_table(("default", "new_tabl2e"))

    catalog._client.__enter__().get_table.assert_called_with(dbname="default", tbl_name="new_tabl2e")

    expected = TableMetadataV2(
        location="s3://bucket/test/location",
        table_uuid=uuid.UUID("9c12d441-03fe-4693-9a96-a0705ddf69c1"),
        last_updated_ms=1602638573590,
        last_column_id=3,
        schemas=[
            Schema(
                NestedField(field_id=1, name="x", field_type=LongType(), required=True),
                schema_id=0,
                identifier_field_ids=[],
            ),
            Schema(
                NestedField(field_id=1, name="x", field_type=LongType(), required=True),
                NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
                NestedField(field_id=3, name="z", field_type=LongType(), required=True),
                schema_id=1,
                identifier_field_ids=[1, 2],
            ),
        ],
        current_schema_id=1,
        partition_specs=[
            PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="x"), spec_id=0)
        ],
        default_spec_id=0,
        last_partition_id=1000,
        properties={"read.split.target.size": "134217728"},
        current_snapshot_id=3055729675574597004,
        snapshots=[
            Snapshot(
                snapshot_id=3051729675574597004,
                parent_snapshot_id=None,
                sequence_number=0,
                timestamp_ms=1515100955770,
                manifest_list="s3://a/b/1.avro",
                summary=Summary(operation=Operation.APPEND),
                schema_id=None,
            ),
            Snapshot(
                snapshot_id=3055729675574597004,
                parent_snapshot_id=3051729675574597004,
                sequence_number=1,
                timestamp_ms=1555100955770,
                manifest_list="s3://a/b/2.avro",
                summary=Summary(operation=Operation.APPEND),
                schema_id=1,
            ),
        ],
        snapshot_log=[
            SnapshotLogEntry(snapshot_id="3051729675574597004", timestamp_ms=1515100955770),
            SnapshotLogEntry(snapshot_id="3055729675574597004", timestamp_ms=1555100955770),
        ],
        metadata_log=[MetadataLogEntry(metadata_file="s3://bucket/.../v1.json", timestamp_ms=1515100)],
        sort_orders=[
            SortOrder(
                SortField(
                    source_id=2, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST
                ),
                SortField(
                    source_id=3,
                    transform=BucketTransform(num_buckets=4),
                    direction=SortDirection.DESC,
                    null_order=NullOrder.NULLS_LAST,
                ),
                order_id=3,
            )
        ],
        default_sort_order_id=3,
        refs={
            "test": SnapshotRef(
                snapshot_id=3051729675574597004,
                snapshot_ref_type=SnapshotRefType.TAG,
                min_snapshots_to_keep=None,
                max_snapshot_age_ms=None,
                max_ref_age_ms=10000000,
            ),
            "main": SnapshotRef(
                snapshot_id=3055729675574597004,
                snapshot_ref_type=SnapshotRefType.BRANCH,
                min_snapshots_to_keep=None,
                max_snapshot_age_ms=None,
                max_ref_age_ms=None,
            ),
        },
        format_version=2,
        last_sequence_number=34,
    )

    assert table.identifier == ("default", "new_tabl2e")
    assert expected == table.metadata


def test_rename_table_from_does_not_exists() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().alter_table.side_effect = NoSuchObjectException(
        message="hive.default.does_not_exists table not found"
    )

    with pytest.raises(NoSuchTableError) as exc_info:
        catalog.rename_table(("default", "does_not_exists"), ("default", "new_table"))

    assert "Table does not exist: does_not_exists" in str(exc_info.value)


def test_rename_table_to_namespace_does_not_exists() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().alter_table.side_effect = InvalidOperationException(
        message="Unable to change partition or table. Database default does not exist Check metastore logs for detailed stack.does_not_exists"
    )

    with pytest.raises(NoSuchNamespaceError) as exc_info:
        catalog.rename_table(("default", "does_exists"), ("default_does_not_exists", "new_table"))

    assert "Database does not exists: default_does_not_exists" in str(exc_info.value)


def test_drop_database_does_not_empty() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().drop_database.side_effect = InvalidOperationException(
        message="Database not_empty is not empty. One or more tables exist."
    )

    with pytest.raises(NamespaceNotEmptyError) as exc_info:
        catalog.drop_namespace(("not_empty",))

    assert "Database not_empty is not empty" in str(exc_info.value)


def test_drop_database_does_not_exists() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().drop_database.side_effect = MetaException(message="java.lang.NullPointerException")

    with pytest.raises(NoSuchNamespaceError) as exc_info:
        catalog.drop_namespace(("does_not_exists",))

    assert "Database does not exists: does_not_exists" in str(exc_info.value)


def test_list_tables() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

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
    catalog._client.__enter__().get_all_tables.assert_called_with(db_name="database")


def test_list_namespaces() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_all_databases.return_value = ["namespace1", "namespace2"]

    assert catalog.list_namespaces() == [("namespace1",), ("namespace2",)]

    catalog._client.__enter__().get_all_databases.assert_called()


def test_drop_table() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_all_databases.return_value = ["namespace1", "namespace2"]

    catalog.drop_table(("default", "table"))

    catalog._client.__enter__().drop_table.assert_called_with(dbname="default", name="table", deleteData=False)


def test_drop_table_does_not_exists() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().drop_table.side_effect = NoSuchObjectException(message="does_not_exists")

    with pytest.raises(NoSuchTableError) as exc_info:
        catalog.drop_table(("default", "does_not_exists"))

    assert "Table does not exists: does_not_exists" in str(exc_info.value)


def test_purge_table() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    with pytest.raises(NotImplementedError):
        catalog.purge_table(("default", "does_not_exists"))


def test_create_database() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().create_database.return_value = None

    catalog.create_namespace("default", {"property": "true"})

    catalog._client.__enter__().create_database.assert_called_with(
        HiveDatabase(
            name="default",
            description=None,
            locationUri=None,
            parameters={"property": "true"},
            privileges=None,
            ownerName=None,
            ownerType=None,
            catalogName=None,
            createTime=None,
            managedLocationUri=None,
            type=None,
            connector_name=None,
            remote_dbname=None,
        )
    )


def test_create_database_already_exists() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().create_database.side_effect = AlreadyExistsException(message="Database default already exists")

    with pytest.raises(NamespaceAlreadyExistsError) as exc_info:
        catalog.create_namespace("default")

    assert "Database default already exists" in str(exc_info.value)


def test_load_namespace_properties(hive_database: HiveDatabase) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_database.return_value = hive_database

    assert catalog.load_namespace_properties("default2") == {"location": hive_database.locationUri, "test": "property"}

    catalog._client.__enter__().get_database.assert_called_with(name="default2")


def test_load_namespace_properties_does_not_exists() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_database.side_effect = NoSuchObjectException(message="does_not_exists")

    with pytest.raises(NoSuchNamespaceError) as exc_info:
        catalog.load_namespace_properties(("does_not_exists",))

    assert "Database does not exists: does_not_exists" in str(exc_info.value)


def test_update_namespace_properties(hive_database: HiveDatabase) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_database.return_value = hive_database
    catalog._client.__enter__().alter_database.return_value = None

    assert catalog.update_namespace_properties(
        namespace="default", removals={"test", "does_not_exists"}, updates={"label": "core"}
    ) == PropertiesUpdateSummary(removed=["test"], updated=["label"], missing=["does_not_exists"])

    catalog._client.__enter__().alter_database.assert_called_with(
        "default",
        HiveDatabase(
            name="default",
            description=None,
            locationUri=hive_database.locationUri,
            parameters={"test": None, "label": "core"},
            privileges=None,
            ownerName=None,
            ownerType=1,
            catalogName="hive",
            createTime=None,
            managedLocationUri=None,
            type=None,
            connector_name=None,
            remote_dbname=None,
        ),
    )


def test_update_namespace_properties_namespace_does_not_exists() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_database.side_effect = NoSuchObjectException(message="does_not_exists")

    with pytest.raises(NoSuchNamespaceError) as exc_info:
        catalog.update_namespace_properties(("does_not_exists",), removals=set(), updates={})

    assert "Database does not exists: does_not_exists" in str(exc_info.value)


def test_update_namespace_properties_overlap() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    with pytest.raises(ValueError) as exc_info:
        catalog.update_namespace_properties(("table",), removals=set("a"), updates={"a": "b"})

    assert "Updates and deletes have an overlap: {'a'}" in str(exc_info.value)


def test_construct_hive_storage_descriptor_simple(table_schema_simple: Schema) -> None:
    descriptor = _construct_hive_storage_descriptor(table_schema_simple, "s3://")
    assert descriptor == StorageDescriptor(
        cols=[
            FieldSchema(name="foo", type="string", comment=None),
            FieldSchema(name="bar", type="int", comment=None),
            FieldSchema(name="baz", type="boolean", comment=None),
        ],
        location="s3://",
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
    )


def test_construct_hive_storage_descriptor_nested(table_schema_nested: Schema) -> None:
    descriptor = _construct_hive_storage_descriptor(table_schema_nested, "s3://")
    assert descriptor == StorageDescriptor(
        cols=[
            FieldSchema(name="foo", type="string", comment=None),
            FieldSchema(name="bar", type="int", comment=None),
            FieldSchema(name="baz", type="boolean", comment=None),
            FieldSchema(name="qux", type="array<string>", comment=None),
            FieldSchema(name="quux", type="map<string,map<string,int>>", comment=None),
            FieldSchema(name="location", type="array<struct<latitude:float,longitude:float>>", comment=None),
            FieldSchema(name="person", type="struct<name:string,age:int>", comment=None),
        ],
        location="s3://",
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
    )


def test_resolve_table_location_warehouse(hive_database: HiveDatabase) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, warehouse="/tmp/warehouse/", uri=HIVE_METASTORE_FAKE_URL)

    # Set this one to None, so we'll fall back to the properties
    hive_database.locationUri = None

    catalog._client = MagicMock()
    catalog._client.__enter__().get_database.return_value = hive_database

    location = catalog._resolve_table_location(None, "database", "table")
    assert location == "/tmp/warehouse/database/table"
