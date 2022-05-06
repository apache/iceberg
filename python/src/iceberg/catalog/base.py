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
from typing import Dict, List, Optional

from iceberg.table.base import Table, TableSpec


class Catalog(ABC):
    """Base Catalog for table operations like - create, drop, load, list and others.

    Attributes:
        name(str): Name of the catalog
        properties(Dict[str, str]): Catalog properties
    """

    def __init__(self, name: str, properties: Dict[str, str]):
        self._name = name
        self._properties = properties

    @property
    def name(self) -> str:
        return self._name

    @property
    def properties(self) -> Dict[str, str]:
        return self._properties

    @abstractmethod
    def create_table(self, table_spec: TableSpec) -> Table:
        """Create a table

        Args:
            table_spec: A specification to create a table

        Returns:
            Table: the created table instance

        Raises:
            AlreadyExistsError: If a table with the name already exists
        """

    @abstractmethod
    def table(self, name: str) -> Table:
        """Loads the table's metadata and returns the table instance.

        You can also use this method to check for table existence using 'try catalog.table() except TableNotFoundError'
        Note: This method does not load table's data in any form.

        Args:
            name: Table's name. Fully classified table name, if it is a namespaced catalog.

        Returns:
            Table: the table instance with its metadata

        Raises:
            TableNotFoundError: If a table with the name does not exist
        """

    @abstractmethod
    def drop_table(self, name: str, purge: bool = True) -> None:
        """Drop a table; Optionally purge all data and metadata files.

        Args:
            name: table name
            purge: Defaults to true, which deletes all data and metadata files in the table; Optional Argument

        Raises:
            TableNotFoundError: If a table with the name does not exist
        """

    @abstractmethod
    def rename_table(self, from_name: str, to_name: str) -> None:
        """Rename a fully classified table name

        Args:
            from_name: Existing table's name. Fully classified table name, if it is a namespaced catalog.
            to_name: New Table name to be assigned. Fully classified table name, if it is a namespaced catalog.

        Raises:
            TableNotFoundError: If a table with the name does not exist
        """

    @abstractmethod
    def replace_table(self, table_spec: TableSpec) -> Table:
        """Starts a transaction and replaces the table with the provided spec.

        Args:
            table_spec: A specification to replace a table

        Returns:
            Table: the replaced table instance with the updated state

        Raises:
            TableNotFoundError: If a table with the name does not exist
        """

    @abstractmethod
    def create_namespace(self, namespace: str, properties: Optional[Dict[str, str]] = None) -> None:
        """Create a namespace in the catalog.

        Args:
            namespace: The namespace to be created.
            properties: A string dictionary of properties for the given namespace

        Raises:
            AlreadyExistsError: If a namespace with the name already exists in the namespace
        """

    @abstractmethod
    def drop_namespace(self, namespace: str) -> None:
        """Drop a namespace.

        Args:
            namespace: The namespace to be dropped.

        Raises:
            NamespaceNotFoundError: If a namespace with the name does not exist in the namespace
            NamespaceNotEmptyError: If the namespace is not empty
        """

    @abstractmethod
    def list_tables(self, namespace: Optional[str] = None) -> List[Table]:
        """List tables under the given namespace in the catalog.

        If namespace not provided, will list all tables in the catalog.

        Args:
            namespace: the namespace to search

        Returns:
            List[Table]: list of table names under this namespace.

        Raises:
            NamespaceNotFoundError: If a namespace with the name does not exist in the namespace
        """

    @abstractmethod
    def list_namespaces(self) -> List[str]:
        """List namespaces from the given namespace. If not given, list top-level namespaces from the catalog.

        Returns:
            List[str]: a List of namespace string
        """

    @abstractmethod
    def get_namespace_metadata(self, namespace: str) -> Dict[str, str]:
        """Get metadata dictionary for a namespace.

        Args:
            namespace: the namespace

        Returns:
            Dict[str, str]: a string dictionary of properties for the given namespace

        Raises:
            NamespaceNotFoundError: If a namespace with the name does not exist in the namespace
        """

    @abstractmethod
    def set_namespace_metadata(self, namespace: str, metadata: Dict[str, str]) -> None:
        """Update or remove metadata for a namespace.

        Note: Existing metadata is overridden, use get, mutate, and then set.

        Args:
            namespace: the namespace
            metadata: a string dictionary of properties for the given namespace

        Raises:
            NamespaceNotFoundError: If a namespace with the name does not exist in the namespace
        """
