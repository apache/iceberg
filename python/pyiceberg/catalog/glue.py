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


import uuid
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

import boto3

from pyiceberg.catalog import (
    ICEBERG,
    MANIFEST,
    MANIFEST_LIST,
    METADATA,
    METADATA_LOCATION,
    PREVIOUS_METADATA,
    TABLE_TYPE,
    WAREHOUSE_LOCATION,
    Catalog,
    Identifier,
    Properties,
    PropertiesUpdateSummary,
    delete_data_files,
    delete_files,
)
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchIcebergTableError,
    NoSuchNamespaceError,
    NoSuchPropertyException,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.io import FileIO, load_file_io
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile, ToOutputFile
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata, new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.typedef import EMPTY_DICT

EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE"
GLUE_CLIENT = "glue"

PROP_GLUE_TABLE = "Table"
PROP_GLUE_TABLE_TYPE = "TableType"
PROP_GLUE_TABLE_DESCRIPTION = "Description"
PROP_GLUE_TABLE_PARAMETERS = "Parameters"
PROP_GLUE_TABLE_DATABASE_NAME = "DatabaseName"
PROP_GLUE_TABLE_NAME = "Name"
PROP_GLUE_TABLE_OWNER = "Owner"
PROP_GLUE_TABLE_STORAGE_DESCRIPTOR = "StorageDescriptor"

PROP_GLUE_TABLELIST = "TableList"

PROP_GLUE_DATABASE = "Database"
PROP_GLUE_DATABASE_LIST = "DatabaseList"
PROP_GLUE_DATABASE_NAME = "Name"
PROP_GLUE_DATABASE_LOCATION = "LocationUri"
PROP_GLUE_DATABASE_DESCRIPTION = "Description"
PROP_GLUE_DATABASE_PARAMETERS = "Parameters"

PROP_GLUE_NEXT_TOKEN = "NextToken"

GLUE_DESCRIPTION_KEY = "comment"
GLUE_DATABASE_LOCATION_KEY = "location"


def _construct_parameters(metadata_location: str) -> Properties:
    return {TABLE_TYPE: ICEBERG.upper(), METADATA_LOCATION: metadata_location}


def _construct_table_input(table_name: str, metadata_location: str, properties: Properties) -> Dict[str, Any]:
    table_input = {
        PROP_GLUE_TABLE_NAME: table_name,
        PROP_GLUE_TABLE_TYPE: EXTERNAL_TABLE_TYPE,
        PROP_GLUE_TABLE_PARAMETERS: _construct_parameters(metadata_location),
    }

    if table_description := properties.get(GLUE_DESCRIPTION_KEY):
        table_input[PROP_GLUE_TABLE_DESCRIPTION] = table_description

    return table_input


def _construct_database_input(database_name: str, properties: Properties) -> Dict[str, Any]:
    database_input: Dict[str, Any] = {PROP_GLUE_DATABASE_NAME: database_name}
    parameters = {}
    for k, v in properties.items():
        if k == GLUE_DESCRIPTION_KEY:
            database_input[PROP_GLUE_DATABASE_DESCRIPTION] = v
        elif k == GLUE_DATABASE_LOCATION_KEY:
            database_input[PROP_GLUE_DATABASE_LOCATION] = v
        else:
            parameters[k] = v
    database_input[PROP_GLUE_DATABASE_PARAMETERS] = parameters
    return database_input


def _write_metadata(metadata: TableMetadata, io: FileIO, metadate_path: str) -> None:
    ToOutputFile.table_metadata(metadata, io.new_output(metadate_path))


class GlueCatalog(Catalog):
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

    def __init__(self, name: str, **properties: str):
        super().__init__(name, **properties)
        self.glue = boto3.client(GLUE_CLIENT)

    def _convert_glue_to_iceberg(self, glue_table: Dict[str, Any]) -> Table:
        properties: Properties = glue_table.get(PROP_GLUE_TABLE_PARAMETERS, {})

        if TABLE_TYPE not in properties:
            raise NoSuchPropertyException(
                f"Property {TABLE_TYPE} missing, could not determine type: "
                f"{glue_table[PROP_GLUE_TABLE_DATABASE_NAME]}.{glue_table[PROP_GLUE_TABLE_NAME]}"
            )
        glue_table_type = properties[TABLE_TYPE]

        if glue_table_type.lower() != ICEBERG:
            raise NoSuchIcebergTableError(
                f"Property table_type is {glue_table_type}, expected {ICEBERG}: "
                f"{glue_table[PROP_GLUE_TABLE_DATABASE_NAME]}.{glue_table[PROP_GLUE_TABLE_NAME]}"
            )

        if METADATA_LOCATION not in properties:
            raise NoSuchPropertyException(
                f"Table property {METADATA_LOCATION} is missing, cannot find metadata for: "
                f"{glue_table[PROP_GLUE_TABLE_DATABASE_NAME]}.{glue_table[PROP_GLUE_TABLE_NAME]}"
            )
        metadata_location = properties[METADATA_LOCATION]

        io = load_file_io(properties=self.properties, location=metadata_location)
        file = io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(file)
        return Table(
            identifier=(glue_table[PROP_GLUE_TABLE_DATABASE_NAME], glue_table[PROP_GLUE_TABLE_NAME]),
            metadata=metadata,
            metadata_location=metadata_location,
            io=self._load_file_io(metadata.properties),
        )

    def _default_warehouse_location(self, database_name: str, table_name: str) -> str:
        database_properties = self.load_namespace_properties(database_name)
        if database_location := database_properties.get(GLUE_DATABASE_LOCATION_KEY):
            database_location = database_location.rstrip("/")
            return f"{database_location}/{table_name}"

        if warehouse_path := self.properties.get(WAREHOUSE_LOCATION):
            warehouse_path = warehouse_path.rstrip("/")
            return f"{warehouse_path}/{database_name}.db/{table_name}"

        raise ValueError("No default path is set, please specify a location when creating a table")

    def _resolve_table_location(self, location: Optional[str], database_name: str, table_name: str) -> str:
        if not location:
            return self._default_warehouse_location(database_name, table_name)
        return location

    def _create_glue_table(self, identifier: Union[str, Identifier], table_input: Dict[str, Any]) -> None:
        database_name, table_name = self.identifier_to_database_and_table(identifier)
        try:
            self.glue.create_table(DatabaseName=database_name, TableInput=table_input)
        except self.glue.exceptions.AlreadyExistsException as e:
            raise TableAlreadyExistsError(f"Table {database_name}.{table_name} already exists") from e
        except self.glue.exceptions.EntityNotFoundException as e:
            raise NoSuchNamespaceError(f"Database {database_name} does not exist") from e

    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        """Create an Iceberg table in Glue catalog

        Args:
            identifier: Table identifier.
            schema: Table's schema.
            location: Location for the table. Optional Argument.
            partition_spec: PartitionSpec for the table.
            sort_order: SortOrder for the table.
            properties: Table properties that can be a string based dictionary.

        Returns:
            Table: the created table instance

        Raises:
            AlreadyExistsError: If a table with the name already exists
            ValueError: If the identifier is invalid, or no path is given to store metadata

        """
        database_name, table_name = self.identifier_to_database_and_table(identifier)

        location = self._resolve_table_location(location, database_name, table_name)
        metadata_location = f"{location}/metadata/00000-{uuid.uuid4()}.metadata.json"
        metadata = new_table_metadata(
            location=location, schema=schema, partition_spec=partition_spec, sort_order=sort_order, properties=properties
        )
        io = load_file_io(properties=self.properties, location=metadata_location)
        _write_metadata(metadata, io, metadata_location)

        self._create_glue_table(
            identifier=identifier, table_input=_construct_table_input(table_name, metadata_location, properties)
        )
        loaded_table = self.load_table(identifier=(database_name, table_name))
        return loaded_table

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
            load_table_response = self.glue.get_table(DatabaseName=database_name, Name=table_name)
        except self.glue.exceptions.EntityNotFoundException as e:
            raise NoSuchTableError(f"Table does not exists: {database_name}.{table_name}") from e

        return self._convert_glue_to_iceberg(load_table_response.get(PROP_GLUE_TABLE, {}))

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        """Drop a table.

        Args:
            identifier: Table identifier.

        Raises:
            NoSuchTableError: If a table with the name does not exist, or the identifier is invalid
        """
        database_name, table_name = self.identifier_to_database_and_table(identifier, NoSuchTableError)
        try:
            self.glue.delete_table(DatabaseName=database_name, Name=table_name)
        except self.glue.exceptions.EntityNotFoundException as e:
            raise NoSuchTableError(f"Table does not exists: {database_name}.{table_name}") from e

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        """Drop a table and purge all data and metadata files.

        Note: This method only logs warning rather than raise exception when encountering file deletion failure

        Args:
            identifier (str | Identifier): Table identifier.

        Raises:
            NoSuchTableError: If a table with the name does not exist, or the identifier is invalid
        """
        table = self.load_table(identifier)
        self.drop_table(identifier)
        io = load_file_io(self.properties, table.metadata_location)
        metadata = table.metadata
        manifest_lists_to_delete = set()
        manifests_to_delete = []
        for snapshot in metadata.snapshots:
            manifests_to_delete += snapshot.manifests(io)
            if snapshot.manifest_list is not None:
                manifest_lists_to_delete.add(snapshot.manifest_list)

        manifest_paths_to_delete = {manifest.manifest_path for manifest in manifests_to_delete}
        prev_metadata_files = {log.metadata_file for log in metadata.metadata_log}

        delete_data_files(io, manifests_to_delete)
        delete_files(io, manifest_paths_to_delete, MANIFEST)
        delete_files(io, manifest_lists_to_delete, MANIFEST_LIST)
        delete_files(io, prev_metadata_files, PREVIOUS_METADATA)
        delete_files(io, {table.metadata_location}, METADATA)

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        """Rename a fully classified table name

        This method can only rename Iceberg tables in AWS Glue

        Args:
            from_identifier: Existing table identifier.
            to_identifier: New table identifier.

        Returns:
            Table: the updated table instance with its metadata

        Raises:
            ValueError: When the from table identifier is invalid
            NoSuchTableError: When a table with the name does not exist
            NoSuchIcebergTableError: When the from table is not a valid iceberg table
            NoSuchPropertyException: When the from table miss some required properties
            NoSuchNamespaceError: When the destination namespace doesn't exist
        """
        from_database_name, from_table_name = self.identifier_to_database_and_table(from_identifier, NoSuchTableError)
        to_database_name, to_table_name = self.identifier_to_database_and_table(to_identifier)
        try:
            get_table_response = self.glue.get_table(DatabaseName=from_database_name, Name=from_table_name)
        except self.glue.exceptions.EntityNotFoundException as e:
            raise NoSuchTableError(f"Table does not exists: {from_database_name}.{from_table_name}") from e

        glue_table = get_table_response[PROP_GLUE_TABLE]

        try:
            # verify that from_identifier is a valid iceberg table
            self._convert_glue_to_iceberg(glue_table)
        except NoSuchPropertyException as e:
            raise NoSuchPropertyException(
                f"Failed to rename table {from_database_name}.{from_table_name} since it miss required properties"
            ) from e
        except NoSuchIcebergTableError as e:
            raise NoSuchIcebergTableError(
                f"Failed to rename table {from_database_name}.{from_table_name} since it is not a valid iceberg table"
            ) from e

        new_table_input = {PROP_GLUE_TABLE_NAME: to_table_name}
        # use the same Glue info to create the new table, pointing to the old metadata
        if table_type := glue_table.get(PROP_GLUE_TABLE_TYPE):
            new_table_input[PROP_GLUE_TABLE_TYPE] = table_type
        if table_parameters := glue_table.get(PROP_GLUE_TABLE_PARAMETERS):
            new_table_input[PROP_GLUE_TABLE_PARAMETERS] = table_parameters
        if table_owner := glue_table.get(PROP_GLUE_TABLE_OWNER):
            new_table_input[PROP_GLUE_TABLE_OWNER] = table_owner
        if table_storage_descriptor := glue_table.get(PROP_GLUE_TABLE_STORAGE_DESCRIPTOR):
            new_table_input[PROP_GLUE_TABLE_STORAGE_DESCRIPTOR] = table_storage_descriptor
        if table_description := glue_table.get(PROP_GLUE_TABLE_DESCRIPTION):
            new_table_input[PROP_GLUE_TABLE_DESCRIPTION] = table_description

        self._create_glue_table(identifier=to_identifier, table_input=new_table_input)
        try:
            self.drop_table(from_identifier)
        except Exception as e:
            self.drop_table(to_identifier)
            raise ValueError(
                f"Fail to drop old table {from_database_name}.{from_table_name}, "
                f"after renaming to {to_database_name}.{to_table_name} roll back to use the old one"
            ) from e
        return self.load_table(to_identifier)

    def create_namespace(self, namespace: Union[str, Identifier], properties: Properties = EMPTY_DICT) -> None:
        """Create a namespace in the catalog.

        Args:
            namespace: Namespace identifier
            properties: A string dictionary of properties for the given namespace

        Raises:
            ValueError: If the identifier is invalid
            AlreadyExistsError: If a namespace with the given name already exists
        """
        database_name = self.identifier_to_database(namespace)
        try:
            self.glue.create_database(DatabaseInput=_construct_database_input(database_name, properties))
        except self.glue.exceptions.AlreadyExistsException as e:
            raise NamespaceAlreadyExistsError(f"Database {database_name} already exists") from e

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        """Drop a namespace.

        A Glue namespace can only be dropped if it is empty

        Args:
            namespace: Namespace identifier

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist, or the identifier is invalid
            NamespaceNotEmptyError: If the namespace is not empty
        """
        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)
        try:
            table_list = self.list_tables(namespace=database_name)
        except NoSuchNamespaceError as e:
            raise NoSuchNamespaceError(f"Database does not exists: {database_name}") from e

        if len(table_list) > 0:
            raise NamespaceNotEmptyError(f"Database {database_name} is not empty")

        self.glue.delete_database(Name=database_name)

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        """List tables under the given namespace in the catalog (including non-Iceberg tables)

        Args:
            namespace (str | Identifier): Namespace identifier to search.

        Returns:
            List[Identifier]: list of table identifiers.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist, or the identifier is invalid
        """

        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)
        table_list = []
        try:
            table_list_response = self.glue.get_tables(DatabaseName=database_name)
            next_token = table_list_response.get(PROP_GLUE_NEXT_TOKEN)
            table_list += table_list_response.get(PROP_GLUE_TABLELIST, [])
            while next_token:
                table_list_response = self.glue.get_tables(DatabaseName=database_name, NextToken=next_token)
                next_token = table_list_response.get(PROP_GLUE_NEXT_TOKEN)
                table_list += table_list_response.get(PROP_GLUE_TABLELIST, [])
        except self.glue.exceptions.EntityNotFoundException as e:
            raise NoSuchNamespaceError(f"Database does not exists: {database_name}") from e
        return [(database_name, table.get(PROP_GLUE_TABLE_NAME)) for table in table_list]

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        """List namespaces from the given namespace. If not given, list top-level namespaces from the catalog.

        Returns:
            List[Identifier]: a List of namespace identifiers
        """
        # Glue does not support hierarchical namespace, therefore return an empty list
        if namespace:
            return []
        database_list = []
        databases_response = self.glue.get_databases()
        next_token = databases_response.get(PROP_GLUE_NEXT_TOKEN)
        database_list += databases_response.get(PROP_GLUE_DATABASE_LIST, [])
        while next_token:
            databases_response = self.glue.get_databases(NextToken=next_token)
            next_token = databases_response.get(PROP_GLUE_NEXT_TOKEN)
            database_list += databases_response.get(PROP_GLUE_DATABASE_LIST, [])
        return [self.identifier_to_tuple(database.get(PROP_GLUE_DATABASE_NAME)) for database in database_list]

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
            database_response = self.glue.get_database(Name=database_name)
        except self.glue.exceptions.EntityNotFoundException as e:
            raise NoSuchNamespaceError(f"Database does not exists: {database_name}") from e
        except self.glue.exceptions.InvalidInputException as e:
            raise NoSuchNamespaceError(f"Invalid input for namespace {database_name}") from e

        database = database_response[PROP_GLUE_DATABASE]
        if PROP_GLUE_DATABASE_PARAMETERS not in database:
            return {}

        properties = dict(database[PROP_GLUE_DATABASE_PARAMETERS])
        if database_location := database.get(PROP_GLUE_DATABASE_LOCATION):
            properties[GLUE_DATABASE_LOCATION_KEY] = database_location
        if database_description := database.get(PROP_GLUE_DATABASE_DESCRIPTION):
            properties[GLUE_DESCRIPTION_KEY] = database_description

        return properties

    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        """Removes provided property keys and updates properties for a namespace.

        Args:
            namespace: Namespace identifier
            removals: Set of property keys that need to be removed. Optional Argument.
            updates: Properties to be updated for the given namespace.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist， or identifier is invalid
            ValueError: If removals and updates have overlapping keys.
        """
        removed: Set[str] = set()
        updated: Set[str] = set()

        if updates and removals:
            overlap = set(removals) & set(updates.keys())
            if overlap:
                raise ValueError(f"Updates and deletes have an overlap: {overlap}")
        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)
        current_properties = self.load_namespace_properties(namespace=database_name)
        new_properties = dict(current_properties)

        if removals:
            for key in removals:
                if key in new_properties:
                    new_properties.pop(key)
                    removed.add(key)
        if updates:
            for key, value in updates.items():
                new_properties[key] = value
                updated.add(key)

        self.glue.update_database(Name=database_name, DatabaseInput=_construct_database_input(database_name, new_properties))

        expected_to_change = (removals or set()).difference(removed)

        return PropertiesUpdateSummary(
            removed=list(removed or []), updated=list(updates.keys() if updates else []), missing=list(expected_to_change)
        )
