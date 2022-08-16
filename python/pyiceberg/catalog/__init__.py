# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass

from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties
from pyiceberg.utils.config import Config, merge_config

logger = logging.getLogger(__name__)

_env_config = Config()


def load_catalog(name: str, uri: str, **properties: str | None) -> Catalog:
    from pyiceberg.catalog.rest import RestCatalog

    supported_catalogs: dict[str, type[Catalog]] = {"http": RestCatalog}

    try:
        # In case Thrift isn't installed
        from pyiceberg.catalog.hive import HiveCatalog

        supported_catalogs["thrift"] = HiveCatalog
    except ImportError:
        logger.warning("Apache Hive not supported, to enable: pip install 'pyiceberg[hive]'")

    if name:
        env = _env_config.get_catalog_config(name)
        conf = merge_config(env or {}, properties)
    else:
        conf = properties

    catalog: Catalog | None = None
    for scheme, catalog_type in supported_catalogs.items():
        if uri.startswith(scheme):
            catalog = catalog_type(name, **conf)

    if not catalog:
        raise ValueError(f"Could not initialize a catalog for URI: {uri}")

    return catalog


@dataclass
class PropertiesUpdateSummary:
    removed: list[str]
    updated: list[str]
    missing: list[str]


class Catalog(ABC):
    """Base Catalog for table operations like - create, drop, load, list and others.

    The catalog table APIs accept a table identifier, which is fully classified table name. The identifier can be a string or
    tuple of strings. If the identifier is a string, it is split into a tuple on '.'. If it is a tuple, it is used as-is.

    The catalog namespace APIs follow a similar convention wherein they also accept a namespace identifier that can be a string
    or tuple of strings.

    Attributes:
        name (str | None): Name of the catalog
        properties (Properties): Catalog properties
    """

    name: str
    properties: Properties

    def __init__(self, name: str, **properties: str):
        self.name = name
        self.properties = properties

    def property(self, key: str) -> str:
        if key not in self.properties:
            raise ValueError(
                f"{type(self).__name__} expects an {key} property. Please set in config or using environment variable PYICEBERG_CATALOG__{self.name.upper()}__{key.upper()}"
            )
        return self.properties[key]

    @abstractmethod
    def create_table(
        self,
        identifier: str | Identifier,
        schema: Schema,
        location: str | None = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        """Create a table

        Args:
            identifier (str | Identifier): Table identifier.
            schema (Schema): Table's schema.
            location (str | None): Location for the table. Optional Argument.
            partition_spec (PartitionSpec): PartitionSpec for the table.
            sort_order (SortOrder): SortOrder for the table.
            properties (Properties): Table properties that can be a string based dictionary.

        Returns:
            Table: the created table instance

        Raises:
            TableAlreadyExistsError: If a table with the name already exists
        """

    @abstractmethod
    def load_table(self, identifier: str | Identifier) -> Table:
        """Loads the table's metadata and returns the table instance.

        You can also use this method to check for table existence using 'try catalog.table() except NoSuchTableError'
        Note: This method doesn't scan data stored in the table.

        Args:
            identifier (str | Identifier): Table identifier.

        Returns:
            Table: the table instance with its metadata

        Raises:
            NoSuchTableError: If a table with the name does not exist
        """

    @abstractmethod
    def drop_table(self, identifier: str | Identifier) -> None:
        """Drop a table.

        Args:
            identifier (str | Identifier): Table identifier.

        Raises:
            NoSuchTableError: If a table with the name does not exist
        """

    @abstractmethod
    def purge_table(self, identifier: str | Identifier) -> None:
        """Drop a table and purge all data and metadata files.

        Args:
            identifier (str | Identifier): Table identifier.

        Raises:
            NoSuchTableError: If a table with the name does not exist
        """

    @abstractmethod
    def rename_table(self, from_identifier: str | Identifier, to_identifier: str | Identifier) -> Table:
        """Rename a fully classified table name

        Args:
            from_identifier (str | Identifier): Existing table identifier.
            to_identifier (str | Identifier): New table identifier.

        Returns:
            Table: the updated table instance with its metadata

        Raises:
            NoSuchTableError: If a table with the name does not exist
        """

    @abstractmethod
    def create_namespace(self, namespace: str | Identifier, properties: Properties = EMPTY_DICT) -> None:
        """Create a namespace in the catalog.

        Args:
            namespace (str | Identifier): Namespace identifier
            properties (Properties): A string dictionary of properties for the given namespace

        Raises:
            NamespaceAlreadyExistsError: If a namespace with the given name already exists
        """

    @abstractmethod
    def drop_namespace(self, namespace: str | Identifier) -> None:
        """Drop a namespace.

        Args:
            namespace (str | Identifier): Namespace identifier

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist
            NamespaceNotEmptyError: If the namespace is not empty
        """

    @abstractmethod
    def list_tables(self, namespace: str | Identifier) -> list[Identifier]:
        """List tables under the given namespace in the catalog.

        If namespace not provided, will list all tables in the catalog.

        Args:
            namespace (str | Identifier): Namespace identifier to search.

        Returns:
            List[Identifier]: list of table identifiers.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist
        """

    @abstractmethod
    def list_namespaces(self, namespace: str | Identifier = ()) -> list[Identifier]:
        """List namespaces from the given namespace. If not given, list top-level namespaces from the catalog.

        Args:
            namespace (str | Identifier): Namespace identifier to search.

        Returns:
            List[Identifier]: a List of namespace identifiers

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist
        """

    @abstractmethod
    def load_namespace_properties(self, namespace: str | Identifier) -> Properties:
        """Get properties for a namespace.

        Args:
            namespace (str | Identifier): Namespace identifier

        Returns:
            Properties: Properties for the given namespace

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist
        """

    @abstractmethod
    def update_namespace_properties(
        self, namespace: str | Identifier, removals: set[str] | None = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        """Removes provided property keys and updates properties for a namespace.

        Args:
            namespace (str | Identifier): Namespace identifier
            removals (Set[str]): Set of property keys that need to be removed. Optional Argument.
            updates (Properties): Properties to be updated for the given namespace.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist
            ValueError: If removals and updates have overlapping keys.
        """

    @staticmethod
    def identifier_to_tuple(identifier: str | Identifier) -> Identifier:
        """Parses an identifier to a tuple.

        If the identifier is a string, it is split into a tuple on '.'. If it is a tuple, it is used as-is.

        Args:
            identifier (str | Identifier: an identifier, either a string or tuple of strings

        Returns:
            Identifier: a tuple of strings
        """
        return identifier if isinstance(identifier, tuple) else tuple(str.split(identifier, "."))

    @staticmethod
    def table_name_from(identifier: str | Identifier) -> str:
        """Extracts table name from a table identifier

        Args:
            identifier (str | Identifier: a table identifier

        Returns:
            str: Table name
        """
        return Catalog.identifier_to_tuple(identifier)[-1]

    @staticmethod
    def namespace_from(identifier: str | Identifier) -> Identifier:
        """Extracts table namespace from a table identifier

        Args:
            identifier (str | Identifier: a table identifier

        Returns:
            Identifier: Namespace identifier
        """
        return Catalog.identifier_to_tuple(identifier)[:-1]
