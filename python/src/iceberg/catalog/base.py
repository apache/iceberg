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
from typing import Dict, List, Optional, Set, Tuple, Union

from iceberg.schema import Schema
from iceberg.table.base import PartitionSpec, Table

Identifier = Tuple[str, ...]
Metadata = Dict[str, str]


class Catalog(ABC):
    """Base Catalog for table operations like - create, drop, load, list and others.

    Attributes:
        name(str): Name of the catalog
        properties(Dict[str, str]): Catalog properties
    """

    def __init__(self, name: str, properties: Metadata):
        self._name = name
        self._properties = properties

    @property
    def name(self) -> str:
        return self._name

    @property
    def properties(self) -> Metadata:
        return self._properties

    @abstractmethod
    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: Optional[PartitionSpec] = None,
        properties: Optional[Metadata] = None,
    ) -> Table:
        """Create a table

        Args:
            identifier: Fully classified table name with its namespace. If the identifier is a string, it is split into a tuple on '.'. If it is a tuple, it is used as-is.
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
    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        """Loads the table's metadata and returns the table instance.

        You can also use this method to check for table existence using 'try catalog.table() except TableNotFoundError'
        Note: This method doesn't scan data stored in the table.

        Args:
            identifier: Fully classified table name with its namespace. If the identifier is a string, it is split into a tuple on '.'. If it is a tuple, it is used as-is.

        Returns:
            Table: the table instance with its metadata

        Raises:
            TableNotFoundError: If a table with the name does not exist
        """

    @abstractmethod
    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        """Drop a table.

        Args:
            identifier: Fully classified table name with its namespace. If the identifier is a string, it is split into a tuple on '.'. If it is a tuple, it is used as-is.

        Raises:
            TableNotFoundError: If a table with the name does not exist
        """

    @abstractmethod
    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        """Drop a table and purge all data and metadata files.

        Args:
            identifier: Fully classified table name with its namespace. If the identifier is a string, it is split into a tuple on '.'. If it is a tuple, it is used as-is.

        Raises:
            TableNotFoundError: If a table with the name does not exist
        """

    @abstractmethod
    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        """Rename a fully classified table name

        Args:
            from_identifier: Fully classified table name with its namespace. If the identifier is a string, it is split into a tuple on '.'. If it is a tuple, it is used as-is.
            to_identifier: New fully classified table name with its namespace. If the identifier is a string, it is split into a tuple on '.'. If it is a tuple, it is used as-is.

        Returns:
            Table: the updated table instance with its metadata

        Raises:
            TableNotFoundError: If a table with the name does not exist
        """

    @abstractmethod
    def create_namespace(self, namespace: Union[str, Identifier], properties: Optional[Metadata] = None) -> None:
        """Create a namespace in the catalog.

        Args:
            namespace: The namespace to be created. If the namespace is a string, it is split into a tuple on '.'. If it is a tuple, it is used as-is.
            properties: A string dictionary of properties for the given namespace

        Raises:
            AlreadyExistsError: If a namespace with the given name already exists
        """

    @abstractmethod
    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        """Drop a namespace.

        Args:
            namespace: The namespace to be dropped. If the namespace is a string, it is split into a tuple on '.'. If it is a tuple, it is used as-is.

        Raises:
            NamespaceNotFoundError: If a namespace with the given name does not exist
            NamespaceNotEmptyError: If the namespace is not empty
        """

    @abstractmethod
    def list_tables(self, namespace: Optional[Union[str, Identifier]] = None) -> List[Identifier]:
        """List tables under the given namespace in the catalog.

        If namespace not provided, will list all tables in the catalog.

        Args:
            namespace: the namespace to search. If the namespace is a string, it is split into a tuple on '.'. If it is a tuple, it is used as-is.

        Returns:
            List[Tuple[str, str]]: list of tuple of table namespace and their names.

        Raises:
            NamespaceNotFoundError: If a namespace with the given name does not exist
        """

    @abstractmethod
    def list_namespaces(self) -> List[Identifier]:
        """List namespaces from the given namespace. If not given, list top-level namespaces from the catalog.

        Returns:
            List[Identifier]: a List of namespace, where each element is a Tuple of namespace levels. Ex: ('com','org','dept')
        """

    @abstractmethod
    def load_namespace(self, namespace: Union[str, Identifier]) -> Metadata:
        """Get metadata dictionary for a namespace.

        Args:
            namespace: If the namespace is a string, it is split into a tuple on '.'. If it is a tuple, it is used as-is.

        Returns:
            Metadata: a dictionary of properties for the given namespace

        Raises:
            NamespaceNotFoundError: If a namespace with the given name does not exist
        """

    @abstractmethod
    def update_namespace_metadata(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Optional[Metadata] = None
    ) -> None:
        """Removes provided metadata keys and then updates metadata for a namespace.

        Note: No errors are raised if a provided removal key is not found.

        Args:
            namespace: If the namespace is a string, it is split into a tuple on '.'. If it is a tuple, it is used as-is.
            removals: a set of metadata keys that need to be removed. Optional Argument.
            updates: a dictionary of properties to be updated for the given namespace. Optional Argument.

        Raises:
            NamespaceNotFoundError: If a namespace with the given name does not exist
        """
