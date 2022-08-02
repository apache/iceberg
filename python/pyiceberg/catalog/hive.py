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
import getpass
import time
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
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
from pyiceberg.catalog.base import Catalog, PropertiesUpdateSummary
from pyiceberg.exceptions import (
    AlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
)
from pyiceberg.schema import Schema
from pyiceberg.table.base import Table
from pyiceberg.table.partitioning import PartitionSpec
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
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

# Replace by visitor
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

OWNER = "owner"


class _HiveClient:
    """Helper class to nicely open and close the transport"""

    _transport: TTransport
    _client: Client

    def __init__(self, uri: str):
        url_parts = urlparse(uri)
        transport = TSocket.TSocket(url_parts.hostname, url_parts.port)
        self._transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        self._client = Client(protocol)

    def __enter__(self) -> Client:
        self._transport.open()
        return self._client

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._transport.close()


def _construct_hive_storage_descriptor(schema: Schema, location: Optional[str]) -> StorageDescriptor:
    ser_de_info = SerDeInfo(serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
    return StorageDescriptor(
        _convert_schema_to_columns(schema),
        location,
        "org.apache.hadoop.mapred.FileInputFormat",
        "org.apache.hadoop.mapred.FileOutputFormat",
        serdeInfo=ser_de_info,
    )


def _convert_schema_to_columns(schema: Schema) -> List[FieldSchema]:
    return [FieldSchema(field.name, _iceberg_type_to_hive_types(field.field_type), field.doc) for field in schema.fields]


def _iceberg_type_to_hive_types(col_type: IcebergType) -> str:
    if hive_type := hive_types.get(type(col_type)):
        return hive_type
    raise NotImplementedError(f"Not yet implemented column type {col_type}")


PROP_EXTERNAL = "EXTERNAL"
PROP_TABLE_TYPE = "table_type"
PROP_METADATA_LOCATION = "metadata_location"
PROP_PREVIOUS_METADATA_LOCATION = "previous_metadata_location"


def _construct_parameters(metadata_location: str, previous_metadata_location: Optional[str] = None) -> Dict[str, Any]:
    properties = {PROP_EXTERNAL: "TRUE", PROP_TABLE_TYPE: "ICEBERG", PROP_METADATA_LOCATION: metadata_location}
    if previous_metadata_location:
        properties[previous_metadata_location] = previous_metadata_location

    return properties


def _annotate_namespace(database: HiveDatabase, properties: Properties) -> HiveDatabase:
    params = {}
    for key, value in properties.items():
        if key == "comment":
            database.description = value
        elif key == "comment":
            database.description = value
        else:
            params[key] = value
    database.parameters = params
    return database


class HiveCatalog(Catalog):
    _client: _HiveClient

    @staticmethod
    def identifier_to_database(
        identifier: Union[str, Identifier], err: Union[Type[ValueError], Type[NoSuchNamespaceError]] = ValueError
    ) -> str:
        tuple_identifier = Catalog.identifier_to_tuple(identifier)
        if len(tuple_identifier) != 1:
            raise err(f"Invalid database, hierarchical namespaces are not supported: {identifier}")

        return tuple_identifier[0]

    @staticmethod
    def identifier_to_database_and_table(
        identifier: Union[str, Identifier],
        err: Union[Type[ValueError], Type[NoSuchTableError], Type[NoSuchNamespaceError]] = ValueError,
    ) -> Tuple[str, str]:
        tuple_identifier = Catalog.identifier_to_tuple(identifier)
        if len(tuple_identifier) != 2:
            raise err(f"Invalid path, hierarchical namespaces are not supported: {identifier}")

        return tuple_identifier[0], tuple_identifier[1]

    def __init__(self, name: str, properties: Properties, uri: str):
        super().__init__(name, properties)
        self._client = _HiveClient(uri)

    def _convert_hive_into_iceberg(self, table: HiveTable) -> Table:
        # Requires reading the manifest, will implement this in another PR
        # Check the table type
        return Table(identifier=(table.dbName, table.tableName))

    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: Optional[PartitionSpec] = None,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Optional[Properties] = None,
    ) -> Table:
        """Create a table

        Args:
            identifier: Table identifier.
            schema: Table's schema.
            location: Location for the table. Optional Argument.
            partition_spec: PartitionSpec for the table.
            sort_order: SortOrder for the table.
            properties: Table properties that can be a string based dictionary. Optional Argument.

        Returns:
            Table: the created table instance

        Raises:
            AlreadyExistsError: If a table with the name already exists
            ValueError: If the identifier is invalid
        """
        database_name, table_name = self.identifier_to_database_and_table(identifier)
        current_time_millis = int(time.time())
        tbl = HiveTable(
            dbName=database_name,
            tableName=table_name,
            owner=properties[OWNER] if properties and OWNER in properties else getpass.getuser(),
            createTime=current_time_millis // 1000,
            lastAccessTime=current_time_millis // 1000,
            sd=_construct_hive_storage_descriptor(schema, location),
            tableType="EXTERNAL_TABLE",
            parameters=_construct_parameters("s3://"),
        )
        try:
            with self._client as open_client:
                open_client.create_table(tbl)
                hive_table = open_client.get_table(dbname=database_name, tbl_name=table_name)
        except AlreadyExistsException as e:
            raise AlreadyExistsError(f"Table {database_name}.{table_name} already exists") from e
        return self._convert_hive_into_iceberg(hive_table)

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        """Loads the table's metadata and returns the table instance.

        You can also use this method to check for table existence using 'try catalog.table() except TableNotFoundError'
        Note: This method doesn't scan data stored in the table.

        Args:
            identifier: Table identifier.

        Returns:
            Table: the table instance with its metadata

        Raises:
            NoSuchTableError: If a table with the name does not exist, or the identifier is invalid
        """
        database_name, table_name = self.identifier_to_database_and_table(identifier, NoSuchTableError)
        try:
            with self._client as open_client:
                hive_table = open_client.get_table(dbname=database_name, tbl_name=table_name)
        except NoSuchObjectException as e:
            raise NoSuchTableError(f"Table does not exists: {table_name}") from e

        return self._convert_hive_into_iceberg(hive_table)

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        """Drop a table.

        Args:
            identifier: Table identifier.

        Raises:
            NoSuchTableError: If a table with the name does not exist, or the identifier is invalid
        """
        database_name, table_name = self.identifier_to_database_and_table(identifier, NoSuchTableError)
        try:
            with self._client as open_client:
                open_client.drop_table(dbname=database_name, name=table_name, deleteData=False)
        except NoSuchObjectException as e:
            # When the namespace doesn't exists, it throws the same error
            raise NoSuchTableError(f"Table does not exists: {table_name}") from e

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        # This requires to traverse the reachability set, and drop all the data files.
        raise NotImplementedError("Not yet implemented")

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        """Rename a fully classified table name

        Args:
            from_identifier: Existing table identifier.
            to_identifier: New table identifier.

        Returns:
            Table: the updated table instance with its metadata

        Raises:
            ValueError: When the from table identifier is invalid
            NoSuchTableError: When a table with the name does not exist
            NoSuchNamespaceError: When the destination namespace doesn't exists
        """
        from_database_name, from_table_name = self.identifier_to_database_and_table(from_identifier, NoSuchTableError)
        to_database_name, to_table_name = self.identifier_to_database_and_table(to_identifier)
        try:
            with self._client as open_client:
                tbl = open_client.get_table(dbname=from_database_name, tbl_name=from_table_name)
                tbl.dbName = to_database_name
                tbl.tableName = to_table_name
                open_client.alter_table(dbname=from_database_name, tbl_name=from_table_name, new_tbl=tbl)
        except NoSuchObjectException as e:
            raise NoSuchTableError(f"Table does not exist: {from_table_name}") from e
        except InvalidOperationException as e:
            raise NoSuchNamespaceError(f"Database does not exists: {to_database_name}") from e
        return Table()

    def create_namespace(self, namespace: Union[str, Identifier], properties: Optional[Properties] = None) -> None:
        """Create a namespace in the catalog.

        Args:
            namespace: Namespace identifier
            properties: A string dictionary of properties for the given namespace

        Raises:
            ValueError: If the identifier is invalid
            AlreadyExistsError: If a namespace with the given name already exists
        """
        database_name = self.identifier_to_database(namespace)
        hive_database = HiveDatabase(name=database_name, parameters=properties)

        try:
            with self._client as open_client:
                open_client.create_database(_annotate_namespace(hive_database, properties or {}))
        except AlreadyExistsException as e:
            raise AlreadyExistsError(f"Database {database_name} already exists") from e

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        """Drop a namespace.

        Args:
            namespace: Namespace identifier

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist, or the identifier is invalid
            NamespaceNotEmptyError: If the namespace is not empty
        """
        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)
        try:
            with self._client as open_client:
                open_client.drop_database(database_name, deleteData=False, cascade=False)
        except InvalidOperationException as e:
            raise NamespaceNotEmptyError(f"Database {database_name} is not empty") from e
        except MetaException as e:
            raise NoSuchNamespaceError(f"Database does not exists: {database_name}") from e

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        """List tables under the given namespace in the catalog (including non-Iceberg tables)

        When the database doesn't exist, it will just return an empty list

        Args:
            namespace: Database to list.

        Returns:
            List[Identifier]: list of table identifiers.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist, or the identifier is invalid
        """
        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)
        with self._client as open_client:
            return [(database_name, table_name) for table_name in open_client.get_all_tables(db_name=database_name)]

    def list_namespaces(self) -> List[Identifier]:
        """List namespaces from the given namespace. If not given, list top-level namespaces from the catalog.

        Returns:
            List[Identifier]: a List of namespace identifiers
        """
        with self._client as open_client:
            return list(map(self.identifier_to_tuple, open_client.get_all_databases()))

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        """Get properties for a namespace.

        Args:
            namespace: Namespace identifier

        Returns:
            Properties: Properties for the given namespace

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist, or identifier is invalid
        """
        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)
        try:
            with self._client as open_client:
                database = open_client.get_database(name=database_name)
                properties = database.parameters
                properties["location"] = database.locationUri
                if comment := database.description:
                    properties["comment"] = comment
                return properties
        except NoSuchObjectException as e:
            raise NoSuchNamespaceError(f"Database does not exists: {database_name}") from e

    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Optional[Properties] = None
    ) -> PropertiesUpdateSummary:
        """Removes provided property keys and updates properties for a namespace.

        Args:
            namespace: Namespace identifier
            removals: Set of property keys that need to be removed. Optional Argument.
            updates: Properties to be updated for the given namespace. Optional Argument.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist
            ValueError: If removals and updates have overlapping keys.
        """
        removed: Set[str] = set()
        updated: Set[str] = set()

        if updates and removals:
            overlap = set(removals) & set(updates.keys())
            if overlap:
                raise ValueError(f"Updates and deletes have an overlap: {overlap}")

        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)
        with self._client as open_client:
            try:
                database = open_client.get_database(database_name)
                parameters = database.parameters
            except NoSuchObjectException as e:
                raise NoSuchNamespaceError(f"Database does not exists: {database_name}") from e
            if removals:
                for key in removals:
                    if key in parameters:
                        parameters[key] = None
                        removed.add(key)
            if updates:
                for key, value in updates.items():
                    parameters[key] = value
                    updated.add(key)
            open_client.alter_database(database_name, _annotate_namespace(database, parameters))

        expected_to_change = (removals or set()).difference(removed)

        return PropertiesUpdateSummary(
            removed=list(removed or []), updated=list(updates.keys() if updates else []), missing=list(expected_to_change)
        )
