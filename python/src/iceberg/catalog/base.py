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

from abc import ABC, abstractmethod
from typing import Optional

from iceberg.schema import Schema
from iceberg.table.base import PartitionSpec, Table


class Catalog(ABC):
    """
    Base Catalog for table operations like - create, drop, load, list and others.

    Attributes:
        name(str): Name of the catalog
        properties(dict): Catalog properties
    """

    def __init__(self, name: str, properties: dict):
        self._name = name
        self._properties = properties

    @property
    def name(self) -> str:
        return self._name

    @property
    def properties(self) -> dict:
        return self._properties

    @abstractmethod
    def list_tables(self) -> list:
        """
        List tables in the catalog.

        :return: list of table names in the catalog.
        """

    @abstractmethod
    def create_table(
        self,
        name: str,
        schema: Schema,
        partition_spec: PartitionSpec,
        *,
        location: Optional[str] = None,
        properties: Optional[dict] = None
    ) -> Table:
        """
        Create a table

        Args:
            name(str): Table's name. Fully classified table name, if it is a namespaced catalog.
            schema(Schema): Table's schema
            partition_spec: A partition spec for the table
            location(str): a location for the table; Optional Keyword Argument
            properties(dict): a string dictionary of table properties; Optional Keyword Argument

        Returns:
            table(Table): the created table instance

        Raises:
            AlreadyExistsError: If a table with the name already exists
        """

    @abstractmethod
    def table(self, name: str) -> Table:
        """
        Loads the table's metadata and returns the table instance. You can also use this method to
        check for table existence using 'try catalog.table() except TableNotFoundError'
        Note: This method does not load table's data in any form.

        :param name: Table's name. Fully classified table name, if it is a namespaced catalog.
        :return: the table instance with its metadata
        :raises TableNotFoundError: If a table with the name does not exist
        """

    @abstractmethod
    def drop_table(self, name: str, purge: bool = True) -> None:
        """
        Drop a table; Optionally purge all data and metadata files.

        :param name: table name
        :param purge: Defaults to true, which deletes all data and metadata files in the table; Optional Argument
        :return: Nothing
        :raises TableNotFoundError: If a table with the name does not exist
        """

    @abstractmethod
    def rename_table(self, from_name: str, to_name: str) -> None:
        """
        Drop a table; Optionally purge all data and metadata files.

        :param from_name: Existing table's name. Fully classified table name, if it is a namespaced catalog.
        :param to_name: New Table name to be assigned. Fully classified table name, if it is a namespaced catalog.
        :return: Nothing
        :raises TableNotFoundError: If a table with the name does not exist
        """

    @abstractmethod
    def replace_table(
        self,
        name: str,
        schema: Schema,
        partition_spec: PartitionSpec,
        *,
        location: Optional[str] = None,
        properties: Optional[dict] = None
    ) -> Table:
        """
        Starts a transaction and replaces the table with the provided spec.

        Args:
            name(str): Table's name. Fully classified table name, if it is a namespaced catalog.
            schema(Schema): Table's schema
            partition_spec: A partition spec for the table
            location(str): a location for the table; Optional Keyword Argument
            properties(dict): a string dictionary of table properties; Optional Keyword Argument

        Returns:
            table(Table): the created table instance

        Raises:
            TableNotFoundError: If a table with the name does not exist
        """


class NamespacedCatalog(Catalog):
    """
    Base catalog for catalogs that support namespaces.
    """

    @abstractmethod
    def create_namespace(self, namespace: str, properties: Optional[dict] = None) -> None:
        """
        Create a namespace in the catalog.

        :param namespace: The namespace to be created.
        :param properties: A string dict of properties for the given namespace
        :return: Nothing
        :raises AlreadyExistsError: If a namespace with the name already exists in the namespace
        """

    @abstractmethod
    def drop_namespace(self, namespace: str) -> None:
        """
        Drop a namespace.

        :param namespace: The namespace to be dropped.
        :return: Nothing
        :raises NamespaceNotFoundError: If a namespace with the name does not exist in the namespace
        :raises NamespaceNotEmptyError: If the namespace is not empty
        """

    @abstractmethod
    def list_tables(self, namespace: Optional[str] = None) -> list:
        """
        List tables under the given namespace in the catalog. If namespace not provided, will list all tables in the
        catalog.

        :param namespace: the namespace to search
        :return: list of table names under this namespace.
        :raises NamespaceNotFoundError: If no such namespace exist
        """

    @abstractmethod
    def list_namespaces(self, namespace: Optional[str] = None) -> list:
        """
        List namespaces from the given namespace. If not given, list top-level namespaces from the catalog.

        :param namespace: given namespace
        :return: a List of namespace names
        """

    @abstractmethod
    def get_namespace_metadata(self, namespace: str) -> dict:
        """
        Get metadata dictionary for a namespace.

        :param namespace: the namespace
        :return: a string dict of properties for the given namespace
        :raises NamespaceNotFoundError: If a namespace with the name does not exist in the namespace
        """

    @abstractmethod
    def set_namespace_metadata(self, namespace: str, metadata: dict) -> None:
        """
        Update or remove metadata for a namespace. Note: Existing metadata is overridden, use get, mutate, and then set.

        :param namespace:
        :param metadata:
        :raises NamespaceNotFoundError: If a namespace with the name does not exist in the namespace
        """
