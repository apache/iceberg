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
from typing import Any, Dict, List, Optional, Tuple

from iceberg.schema import Schema
from iceberg.table.base import PartitionSpec, Table


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
    def create_table(
        self,
        *,
        namespace: Tuple[str, ...],
        name: str,
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: Optional[PartitionSpec] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> Table:
        """Create a table

        Args:
            namespace: A tuple of table's namespace levels. Ex: ('com','org','dept')
            name: Table name
            schema: Table's schema.
            location: Location for the table. Optional Argument.
            partition_spec: PartitionSpec for the table. Optional Argument.
            properties: Table metadata that can be a string based dictionary. Optional Argument.

        Returns:
            Table: the created table instance

        Raises:
            AlreadyExistsError: If a table with the name already exists
        """

    @abstractmethod
    def table(self, namespace: Tuple[str, ...], name: str) -> Table:
        """Loads the table's metadata and returns the table instance.

        You can also use this method to check for table existence using 'try catalog.table() except TableNotFoundError'
        Note: This method does not load table's data in any form.

        Args:
            namespace: A tuple of table's namespace levels. Ex: ('com','org','dept')
            name: Table's name.

        Returns:
            Table: the table instance with its metadata

        Raises:
            TableNotFoundError: If a table with the name does not exist
        """

    @abstractmethod
    def drop_table(self, namespace: Tuple[str, ...], name: str, purge: bool = True) -> None:
        """Drop a table; Optionally purge all data and metadata files.

        Args:
            namespace: A tuple of table's namespace levels. Ex: ('com','org','dept')
            name: table name
            purge: Defaults to true, which deletes all data and metadata files in the table; Optional Argument

        Raises:
            TableNotFoundError: If a table with the name does not exist
        """

    @abstractmethod
    def rename_table(self, from_namespace: Tuple[str, ...], from_name: str, to_namespace: Tuple[str, ...], to_name: str) -> Table:
        """Rename a fully classified table name

        Args:
            from_namespace: Existing table's namespace. A tuple of table's namespace levels. Ex: ('com','org','dept')
            from_name: Existing table's name.
            to_namespace: New Table namespace to be assigned. Tuple of namespace levels. Ex: ('com','org','new')
            to_name: New Table name to be assigned.

        Returns:
            Table: the updated table instance with its metadata

        Raises:
            TableNotFoundError: If a table with the name does not exist
        """

    @abstractmethod
    def replace_table(
        self,
        *,
        namespace: Tuple[str, ...],
        name: str,
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: Optional[PartitionSpec] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> Table:
        """Starts a transaction and replaces the table with the provided spec.

        Args:
            namespace: A tuple of table's namespace levels. Ex: ('com','org','dept')
            name: Table name
            schema: Table's schema.
            location: Location for the table. Optional Argument.
            partition_spec: PartitionSpec for the table. Optional Argument.
            properties: Table metadata that can be a string based dictionary. Optional Argument.

        Returns:
            Table: the replaced table instance with the updated state

        Raises:
            TableNotFoundError: If a table with the name does not exist
        """

    @abstractmethod
    def create_namespace(self, namespace: Tuple[str, ...], properties: Optional[Dict[str, str]] = None) -> None:
        """Create a namespace in the catalog.

        Args:
            namespace: The namespace to be created. Tuple of namespace levels. Ex: ('com','org','dept')
            properties: A string dictionary of properties for the given namespace

        Raises:
            AlreadyExistsError: If a namespace with the name already exists in the namespace
        """

    @abstractmethod
    def drop_namespace(self, namespace: Tuple[str, ...]) -> None:
        """Drop a namespace.

        Args:
            namespace: The namespace to be dropped. Tuple of namespace levels. Ex: ('com','org','dept')

        Raises:
            NamespaceNotFoundError: If a namespace with the name does not exist in the namespace
            NamespaceNotEmptyError: If the namespace is not empty
        """

    @abstractmethod
    def list_tables(self, namespace: Optional[Tuple[str, ...]] = None) -> List[Tuple[Tuple[str, ...], str]]:
        """List tables under the given namespace in the catalog.

        If namespace not provided, will list all tables in the catalog.

        Args:
            namespace: the namespace to search. Tuple of namespace levels. Ex: ('com','org','dept')

        Returns:
            List[Tuple[str, str]]: list of tuple of table namespace and their names.

        Raises:
            NamespaceNotFoundError: If a namespace with the name does not exist in the namespace
        """

    @abstractmethod
    def list_namespaces(self) -> List[Tuple[str, ...]]:
        """List namespaces from the given namespace. If not given, list top-level namespaces from the catalog.

        Returns:
            List[Tuple[str, ...]]: a List of namespace, where each element is a Tuple of namespace levels. Ex: ('com','org','dept')
        """

    @abstractmethod
    def load_namespace_metadata(self, namespace: Tuple[str, ...]) -> Dict[str, str]:
        """Get metadata dictionary for a namespace.

        Args:
            namespace: A Tuple of namespace levels. Ex: ('com','org','dept')

        Returns:
            Dict[str, str]: a dictionary of properties for the given namespace

        Raises:
            NamespaceNotFoundError: If a namespace with the name does not exist in the namespace
        """

    @abstractmethod
    def set_namespace_metadata(self, namespace: Tuple[str, ...], metadata: Dict[str, str]) -> None:
        """Update or remove metadata for a namespace.

        Note: Existing metadata is overridden, use get, mutate, and then set.

        Args:
            namespace: A Tuple of namespace levels. Ex: ('com','org','dept')
            metadata: a dictionary of properties for the given namespace

        Raises:
            NamespaceNotFoundError: If a namespace with the name does not exist in the namespace
        """


class TableNotFoundError(Exception):
    """Exception when a table is not found in the catalog"""

    def __init__(self, name: str):
        super().__init__(self, f"Table {name} not found in the catalog")


class NamespaceNotFoundError(Exception):
    """Exception when a Namespace is not found in the catalog"""

    def __init__(self, namespace: Tuple[str, ...]):
        super().__init__(self, f"Namespace {namespace} not found in the catalog")


class NamespaceNotEmptyError(Exception):
    """Exception when a Namespace is not empty"""

    def __init__(self, namespace: Tuple[str, ...]):
        super().__init__(self, f"Namespace {namespace} not empty")


class AlreadyExistsError(Exception):
    """Exception when an entity like table or namespace already exists in the catalog"""

    def __init__(self, name: Any):
        super().__init__(self, f"Table or namespace {name} already exists")
