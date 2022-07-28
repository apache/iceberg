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
from getpass import getuser
from time import time
from typing import (
    List,
    Optional,
    Set,
    Union,
)
from urllib.parse import urlparse

from hive_metastore.ThriftHiveMetastore import Client
from hive_metastore.ttypes import AlreadyExistsException
from hive_metastore.ttypes import Database as HiveDatabase
from hive_metastore.ttypes import (
    FieldSchema,
    InvalidOperationException,
    MetaException,
    NoSuchObjectException,
    SerDeInfo,
    StorageDescriptor,
)
from hive_metastore.ttypes import Table as HiveTable
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from pyiceberg.catalog import Identifier, Properties
from pyiceberg.catalog.base import Catalog
from pyiceberg.exceptions import (
    AlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
)
from pyiceberg.schema import Schema
from pyiceberg.table.base import Table
from pyiceberg.table.partitioning import PartitionSpec
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    StringType,
    StructType,
    TimestampType,
    TimeType,
    UUIDType,
)

hive_types = {
    BooleanType: "boolean",
    IntegerType: "int",
    LongType: "bigint",
    FloatType: "float",
    DoubleType: "double",
    DateType: "date",
    TimeType: "string",
    TimestampType: "timestamp",
    StringType: "string",
    UUIDType: "string",
    BinaryType: "binary",
    FixedType: "binary",
    DecimalType: None,
    StructType: None,
    ListType: None,
    MapType: None,
}


class _HiveClient:
    _transport: TTransport
    _client: Client

    def __init__(self, url: str):
        url_parts = urlparse(url)
        transport = TSocket.TSocket(url_parts.hostname, url_parts.port)
        self._transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        self._client = Client(protocol)

    def __enter__(self) -> Client:
        self._transport.open()
        return self._client

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._transport.close()


class HiveCatalog(Catalog):
    _client: _HiveClient

    def __init__(self, name: str, properties: Properties, url: str):
        super().__init__(name, properties)
        self._client = _HiveClient(url)

    def _storage_descriptor(self, schema: Schema, location: Optional[str]) -> StorageDescriptor:
        ser_de_info = SerDeInfo(serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
        return StorageDescriptor(
            self._columns(schema),
            location,
            "org.apache.hadoop.mapred.FileInputFormat",
            "org.apache.hadoop.mapred.FileOutputFormat",
            serdeInfo=ser_de_info,
        )

    def _columns(self, schema: Schema) -> List[FieldSchema]:
        return [FieldSchema(field.name, self._convert_hive_type(field.field_type), field.doc) for field in schema.fields]

    def _convert_hive_type(self, col_type: IcebergType) -> str:
        if hive_type := hive_types.get(type(col_type)):
            return hive_type
        raise NotImplementedError(f"Not yet implemented column type {col_type}")

    def _convert_hive_into_iceberg(self, _: HiveTable) -> Table:
        # Requires reading the manifest, will implement this in another PR
        # Also nice to have the REST catalog in first
        return Table()

    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: Optional[PartitionSpec] = None,
        properties: Optional[Properties] = None,
    ) -> Table:
        database_name, table_name = self.identifier_to_tuple(identifier)
        current_time_millis = int(time())
        tbl = HiveTable(
            dbName=database_name,
            tableName=table_name,
            owner=getuser(),
            createTime=current_time_millis // 1000,
            lastAccessTime=current_time_millis // 1000,
            sd=self._storage_descriptor(schema, location),
            tableType="EXTERNAL_TABLE",
            parameters={"EXTERNAL": "TRUE"},
        )
        try:
            with self._client as open_client:
                open_client.create_table(tbl)
                hive_table = open_client.get_table(dbname=database_name, tbl_name=table_name)
        except AlreadyExistsException as e:
            raise AlreadyExistsError(f"Table {database_name}.{table_name} already exists") from e
        return self._convert_hive_into_iceberg(hive_table)

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        database_name, table_name = self.identifier_to_tuple(identifier)
        try:
            with self._client as open_client:
                hive_table = open_client.get_table(dbname=database_name, tbl_name=table_name)
        except NoSuchObjectException as e:
            raise NoSuchTableError(f"Table {database_name}.{table_name} not found") from e

        return self._convert_hive_into_iceberg(hive_table)

    def _drop_table(self, identifier: Union[str, Identifier], delete_data: bool = False) -> None:
        database_name, table_name = self.identifier_to_tuple(identifier)
        try:
            with self._client as open_client:
                open_client.drop_table(dbname=database_name, name=table_name, deleteData=delete_data)
        except NoSuchObjectException as e:
            # When the namespace doesn't exists, it throws the same error
            raise NoSuchTableError(f"Table {table_name} cannot be found") from e

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        self._drop_table(identifier)

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        self._drop_table(identifier, True)

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        from_database_name, from_table_name = self.identifier_to_tuple(from_identifier)
        to_database_name, to_table_name = self.identifier_to_tuple(to_identifier)
        try:
            with self._client as open_client:
                tbl = open_client.get_table(dbname=from_database_name, tbl_name=from_table_name)
                tbl.dbName = to_database_name
                tbl.tableName = to_table_name
                open_client.alter_table(dbname=from_database_name, tbl_name=from_table_name, new_tbl=tbl)
        except NoSuchObjectException as e:
            raise NoSuchTableError(f"Table {from_database_name}.{from_table_name} not found") from e
        except InvalidOperationException as e:
            raise NoSuchNamespaceError(f"Destination database {to_database_name} does not exists") from e
        return Table()

    def create_namespace(self, namespace: Union[str, Identifier], properties: Optional[Properties] = None) -> None:
        database_name = self.identifier_to_tuple(namespace)[0]

        hive_database = HiveDatabase(name=database_name, parameters=properties)

        try:
            with self._client as open_client:
                open_client.create_database(hive_database)
        except AlreadyExistsException as e:
            raise AlreadyExistsError(f"Database {database_name} already exists") from e

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        database_name = self.identifier_to_tuple(namespace)[0]
        try:
            with self._client as open_client:
                open_client.drop_database(database_name, deleteData=True, cascade=False)
        except InvalidOperationException as e:
            raise NamespaceNotEmptyError(f"Database {database_name} is not empty") from e
        except MetaException as e:
            raise NoSuchNamespaceError(f"Database {database_name} does not exists") from e

    def list_tables(self, namespace: Optional[Union[str, Identifier]] = None) -> List[Identifier]:
        database_name = self.identifier_to_tuple(namespace)[0] if namespace else namespace
        with self._client as open_client:
            return open_client.get_all_tables(db_name=database_name)

    def list_namespaces(self) -> List[Identifier]:
        with self._client as open_client:
            return open_client.get_all_databases()

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        database_name = self.identifier_to_tuple(namespace)[0]
        try:
            with self._client as open_client:
                database = open_client.get_database(database_name)
                return database.parameters
        except NoSuchObjectException as e:
            raise NoSuchNamespaceError(f"Database {database_name} does not exists") from e

    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Optional[Properties] = None
    ) -> None:
        database_name = self.identifier_to_tuple(namespace)[0]
        with self._client as open_client:
            database = open_client.get_database(database_name)
            if updates:
                database.parameters.update(updates)
            if removals:
                for key in removals:
                    if key in database.parameters:
                        del database.parameters[key]
            open_client.alter_database(database_name, database)
