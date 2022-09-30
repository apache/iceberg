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
import uuid

import boto3
from datetime import datetime
from typing import Union, Optional, List, Set, Dict

from pyiceberg.catalog.hive import OWNER

from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)

from pyiceberg.catalog import (
    Catalog,
    Identifier,
    Properties,
    PropertiesUpdateSummary,
)
from pyiceberg.io import FileIO, load_file_io
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile, ToOutputFile
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata, new_table_metadata
from pyiceberg.table.partitioning import PartitionSpec, UNPARTITIONED_PARTITION_SPEC
from pyiceberg.table.sorting import SortOrder, UNSORTED_SORT_ORDER, SortDirection
from pyiceberg.typedef import EMPTY_DICT

from pyiceberg.types import NestedField

COMMENT = "comment"
OWNER = "owner"
TABLE_TYPE = "table_type"
METADATA_LOCATION = "metadata_location"
ICEBERG = "iceberg"
LOCATION = "location"
WAREHOUSE = "warehouse"


PROP_TABLE_TYPE = "table_type"
# TODO: refactor these types

class GlueCatalog(Catalog):

    def __init__(self, name: str, **properties: Properties):
        super().__init__(name, **properties)
        self.glue = boto3.client("glue")


    def _glue_to_iceberg(self, glue_table, io: FileIO) -> Table:
        properties: Dict[str, str] = glue_table["Parameters"]

        if TABLE_TYPE not in properties:
            raise NoSuchTableError(
                f"Property table_type missing, could not determine type: {glue_table['DatabaseName']}.{glue_table['Name']}")
        glue_table_type = properties.get(TABLE_TYPE)
        if glue_table_type.lower() != ICEBERG:
            raise NoSuchTableError(
                f"Property table_type is {glue_table_type}, expected {ICEBERG}: {glue_table['DatabaseName']}.{glue_table['Name']}")
        if prop_meta_location := properties.get(METADATA_LOCATION):
            metadata_location = prop_meta_location
        else:
            raise NoSuchTableError(f"Table property {METADATA_LOCATION} is missing")

        file = io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(file)
        return Table(
            identifier=(glue_table['DatabaseName'], glue_table['Name']),
            metadata=metadata,
            metadata_location=metadata_location
        )

    def _iceberg_to_glue(self, iceberg_table):
        # TODO
        pass

    def _construct_parameters(self, metadata_location: str) -> Dict[str, str]:
        properties = {TABLE_TYPE: "ICEBERG", METADATA_LOCATION: metadata_location}
        return properties

    def _default_warehouse_location(self, database_name: str, table_name: str):
        try:
            response = self.glue.get_database(Name=database_name)
        # TODO: handle response and errors
        except:
            raise NoSuchNamespaceError("Database not found")

        if "LocationUri" in response["Database"]:
            return f"{response['Database']['LocationUri']}/table_name"

        # TODO: should handle potential errors
        return f"{self.properties[WAREHOUSE]}/{database_name}.db/{table_name}"

    def _resolve_table_location(self, location: Optional[str], database_name: str, table_name: str):
        if not location:
            return self._default_warehouse_location(database_name, table_name)
        return location

    def _write_metadata(self, metadata: TableMetadata, io: FileIO, metadate_path: str):
        ToOutputFile.table_metadata(metadata, io.new_output(metadate_path))

    # tested on pre-existing database
    def create_table(
            self,
            identifier: Union[str, Identifier],
            schema: Schema,
            location: Optional[str] = None,
            partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
            sort_order: SortOrder = UNSORTED_SORT_ORDER,
            properties: Properties = EMPTY_DICT,
    ) -> Table:
        """Create a table

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
        # TODO: give it correct path based on java version of glueCatalog
        metadata_location = f"{location}/metadata/00000-{uuid.uuid4()}.metadata.json"
        metadata = new_table_metadata(
            location=location, schema=schema, partition_spec=partition_spec, sort_order=sort_order,
            properties=properties
        )
        io = load_file_io({**self.properties, **properties}, location=location)
        self._write_metadata(metadata, io, metadata_location)
        try:
            create_table_response = self.glue.create_table(
                DatabaseName=database_name,
                TableInput={
                    'Name': table_name,
                    'Description': '',  # TODO: metadata does not have a description field, come back later
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': self._construct_parameters(metadata_location),
                }
            )
            # TODO: check response
            load_table_response = self.glue.get_table(DatabaseName=database_name, Name=table_name)
            glue_table = load_table_response['Table']
        except self.glue.exceptions.AlreadyExistsException as e:
            raise TableAlreadyExistsError(f"Table {database_name}.{table_name} already exists") from e

        return self._glue_to_iceberg(glue_table, io)

    # tested
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
        # TODO: may need to add table properties to the io too
        io = load_file_io(
            {**self.properties},
            load_table_response['Table']['Parameters'][METADATA_LOCATION])
        return self._glue_to_iceberg(load_table_response['Table'], io)

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        raise NotImplementedError("currently unsupported")

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        self.drop_table(identifier)

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        raise NotImplementedError("currently unsupported")

    # tested but cannot see on glueCatalog
    def create_namespace(self, namespace: Union[str, Identifier], properties: Properties = EMPTY_DICT) -> None:
        raise NotImplementedError("currently unsupported")

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        raise NotImplementedError("currently unsupported")

    def list_tables(self, namespace: Union[str, Identifier]) -> list[Identifier]:
        raise NotImplementedError("currently unsupported")

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> list[Identifier]:
        raise NotImplementedError("currently unsupported")

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        raise NotImplementedError("currently unsupported")

    def update_namespace_properties(
            self, namespace: Union[str, Identifier], removals: Optional[set[str]] = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        raise NotImplementedError("currently unsupported")
