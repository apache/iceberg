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
    Union,
)

import boto3

from pyiceberg.catalog import (
    Catalog,
    Identifier,
    Properties,
    PropertiesUpdateSummary,
)
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError, TableAlreadyExistsError
from pyiceberg.io import FileIO, load_file_io
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile, ToOutputFile
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata, new_table_metadata
from pyiceberg.table.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.typedef import EMPTY_DICT

ICEBERG = "ICEBERG"
EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE"

PROP_TABLE_TYPE = "table_type"
PROP_WAREHOUSE = "warehouse"
PROP_METADATA_LOCATION = "metadata_location"

PROP_GLUE_TABLE = "Table"
PROP_GLUE_TABLE_TYPE = "TableType"
PROP_GLUE_TABLE_DESCRIPTION = "description"
PROP_GLUE_TABLE_PARAMETERS = "Parameters"
PROP_GLUE_TABLE_DATABASE_NAME = "DatabaseName"
PROP_GLUE_TABLE_NAME = "Name"

PROP_GLUE_DATABASE = "Database"
PROP_GLUE_DATABASE_LIST = "DatabaseList"
PROP_GLUE_DATABASE_NAME = "Name"
PROP_GLUE_DATABASE_LOCATION = "LocationUri"


def _construct_parameters(metadata_location: str) -> Properties:
    return {PROP_TABLE_TYPE: ICEBERG, PROP_METADATA_LOCATION: metadata_location}


def _construct_table_input(table_name: str, metadata_location: str, properties: Properties) -> Dict[str, Any]:
    table_input = {
        PROP_GLUE_TABLE_NAME: table_name,
        PROP_GLUE_TABLE_TYPE: EXTERNAL_TABLE_TYPE,
        PROP_GLUE_TABLE_PARAMETERS: _construct_parameters(metadata_location),
    }

    if table_description := properties.get(PROP_GLUE_TABLE_DESCRIPTION):
        table_input[PROP_GLUE_TABLE_DESCRIPTION] = table_description

    return table_input


def _convert_glue_to_iceberg(glue_table: Dict[str, Any], io: FileIO) -> Table:
    properties: Properties = glue_table[PROP_GLUE_TABLE_PARAMETERS]

    if PROP_TABLE_TYPE not in properties:
        raise NoSuchTableError(
            f"Property table_type missing, could not determine type: "
            f"{glue_table[PROP_GLUE_TABLE_DATABASE_NAME]}.{glue_table[PROP_GLUE_TABLE_NAME]}"
        )
    glue_table_type = properties.get(PROP_TABLE_TYPE)
    if glue_table_type != ICEBERG:
        raise NoSuchTableError(
            f"Property table_type is {glue_table_type}, expected {ICEBERG}: "
            f"{glue_table[PROP_GLUE_TABLE_DATABASE_NAME]}.{glue_table[PROP_GLUE_TABLE_NAME]}"
        )
    if prop_meta_location := properties.get(PROP_METADATA_LOCATION):
        metadata_location = prop_meta_location
    else:
        raise NoSuchTableError(f"Table property {PROP_METADATA_LOCATION} is missing")

    file = io.new_input(metadata_location)
    metadata = FromInputFile.table_metadata(file)
    return Table(
        identifier=(glue_table[PROP_GLUE_TABLE_DATABASE_NAME], glue_table[PROP_GLUE_TABLE_NAME]),
        metadata=metadata,
        metadata_location=metadata_location,
    )


def _write_metadata(metadata: TableMetadata, io: FileIO, metadate_path: str):
    ToOutputFile.table_metadata(metadata, io.new_output(metadate_path))


class GlueCatalog(Catalog):
    def __init__(self, name: str, **properties: str):
        super().__init__(name, **properties)
        self.glue = boto3.client("glue")

    def _default_warehouse_location(self, database_name: str, table_name: str):
        try:
            response = self.glue.get_database(Name=database_name)
        except self.glue.exceptions.EntityNotFoundException as e:
            raise NoSuchNamespaceError(f"The database: {database_name} does not exist") from e

        if database_location := response.get(PROP_GLUE_DATABASE).get(PROP_GLUE_DATABASE_LOCATION):
            return f"{database_location}/{table_name}"

        if PROP_WAREHOUSE in self.properties:
            return f"{self.properties[PROP_WAREHOUSE]}/{database_name}.db/{table_name}"

        raise ValueError("No default path is set, please specify a location when creating a table")

    def _resolve_table_location(self, location: Optional[str], database_name: str, table_name: str) -> str:
        if not location:
            return self._default_warehouse_location(database_name, table_name)
        return location

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
            ValueError: If the identifier is invalid
        """
        database_name, table_name = self.identifier_to_tuple(identifier)

        location = self._resolve_table_location(location, database_name, table_name)
        metadata_location = f"{location}/metadata/00000-{uuid.uuid4()}.metadata.json"
        metadata = new_table_metadata(
            location=location, schema=schema, partition_spec=partition_spec, sort_order=sort_order, properties=properties
        )
        io = load_file_io({**self.properties, **properties}, location=location)
        _write_metadata(metadata, io, metadata_location)
        try:
            self.glue.create_table(
                DatabaseName=database_name, TableInput=_construct_table_input(table_name, metadata_location, properties)
            )
        except self.glue.exceptions.AlreadyExistsException as e:
            raise TableAlreadyExistsError(f"Table {database_name}.{table_name} already exists") from e
        except self.glue.exceptions.EntityNotFoundException as e:
            raise NoSuchNamespaceError(f"Database {database_name} not found") from e

        try:
            load_table_response = self.glue.get_table(DatabaseName=database_name, Name=table_name)
        except self.glue.exceptions.EntityNotFoundException as e:
            raise NoSuchTableError(f"Table {database_name}.{table_name} fail to be created") from e

        glue_table = load_table_response[PROP_GLUE_TABLE]
        return _convert_glue_to_iceberg(glue_table, io)

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
        database_name, table_name = self.identifier_to_tuple(identifier)
        try:
            load_table_response = self.glue.get_table(DatabaseName=database_name, Name=table_name)
        except self.glue.exceptions.EntityNotFoundException as e:
            raise NoSuchTableError(f"Table does not exists: {table_name}") from e
        loaded_table = load_table_response[PROP_GLUE_TABLE]
        io = load_file_io(self.properties, loaded_table[PROP_GLUE_TABLE_PARAMETERS][PROP_METADATA_LOCATION])
        return _convert_glue_to_iceberg(loaded_table, io)

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        raise NotImplementedError("currently unsupported")

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        self.drop_table(identifier)

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        raise NotImplementedError("currently unsupported")

    def create_namespace(self, namespace: Union[str, Identifier], properties: Properties = EMPTY_DICT) -> None:
        raise NotImplementedError("currently unsupported")

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        raise NotImplementedError("currently unsupported")

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        raise NotImplementedError("currently unsupported")

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        """List namespaces from the given namespace. If not given, list top-level namespaces from the catalog.
        Returns:
            List[Identifier]: a List of namespace identifiers
        """
        # Glue does not support hierarchical namespace, therefore return an empty list
        if namespace:
            return []
        databases_response = self.glue.get_databases()
        return [
            self.identifier_to_tuple(database[PROP_GLUE_DATABASE_NAME])
            for database in databases_response[PROP_GLUE_DATABASE_LIST]
        ]

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        raise NotImplementedError("currently unsupported")

    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        raise NotImplementedError("currently unsupported")