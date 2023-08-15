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
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import (
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError, NotInstalledError
from pyiceberg.io import FileIO, load_file_io
from pyiceberg.manifest import ManifestFile
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import ToOutputFile
from pyiceberg.table import (
    CommitTableRequest,
    CommitTableResponse,
    Table,
    TableMetadata,
)
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.typedef import (
    EMPTY_DICT,
    Identifier,
    Properties,
    RecursiveDict,
)
from pyiceberg.utils.config import Config, merge_config

logger = logging.getLogger(__name__)

_ENV_CONFIG = Config()

TOKEN = "token"
TYPE = "type"
ICEBERG = "iceberg"
TABLE_TYPE = "table_type"
WAREHOUSE_LOCATION = "warehouse"
METADATA_LOCATION = "metadata_location"
PREVIOUS_METADATA_LOCATION = "previous_metadata_location"
MANIFEST = "manifest"
MANIFEST_LIST = "manifest list"
PREVIOUS_METADATA = "previous metadata"
METADATA = "metadata"
URI = "uri"
LOCATION = "location"
EXTERNAL_TABLE = "EXTERNAL_TABLE"


class CatalogType(Enum):
    REST = "rest"
    HIVE = "hive"
    GLUE = "glue"
    DYNAMODB = "dynamodb"
    SQL = "sql"


def load_rest(name: str, conf: Properties) -> Catalog:
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(name, **conf)


def load_hive(name: str, conf: Properties) -> Catalog:
    try:
        from pyiceberg.catalog.hive import HiveCatalog

        return HiveCatalog(name, **conf)
    except ImportError as exc:
        raise NotInstalledError("Apache Hive support not installed: pip install 'pyiceberg[hive]'") from exc


def load_glue(name: str, conf: Properties) -> Catalog:
    try:
        from pyiceberg.catalog.glue import GlueCatalog

        return GlueCatalog(name, **conf)
    except ImportError as exc:
        raise NotInstalledError("AWS glue support not installed: pip install 'pyiceberg[glue]'") from exc


def load_dynamodb(name: str, conf: Properties) -> Catalog:
    try:
        from pyiceberg.catalog.dynamodb import DynamoDbCatalog

        return DynamoDbCatalog(name, **conf)
    except ImportError as exc:
        raise NotInstalledError("AWS DynamoDB support not installed: pip install 'pyiceberg[dynamodb]'") from exc


def load_sql(name: str, conf: Properties) -> Catalog:
    try:
        from pyiceberg.catalog.sql import SqlCatalog

        return SqlCatalog(name, **conf)
    except ImportError as exc:
        raise NotInstalledError("SQLAlchemy support not installed: pip install 'pyiceberg[sql-postgres]'") from exc


AVAILABLE_CATALOGS: dict[CatalogType, Callable[[str, Properties], Catalog]] = {
    CatalogType.REST: load_rest,
    CatalogType.HIVE: load_hive,
    CatalogType.GLUE: load_glue,
    CatalogType.DYNAMODB: load_dynamodb,
    CatalogType.SQL: load_sql,
}


def infer_catalog_type(name: str, catalog_properties: RecursiveDict) -> Optional[CatalogType]:
    """Tries to infer the type based on the dict.

    Args:
        name: Name of the catalog.
        catalog_properties: Catalog properties.

    Returns:
        The inferred type based on the provided properties.

    Raises:
        ValueError: Raises a ValueError in case properties are missing, or the wrong type.
    """
    if uri := catalog_properties.get("uri"):
        if isinstance(uri, str):
            if uri.startswith("http"):
                return CatalogType.REST
            elif uri.startswith("thrift"):
                return CatalogType.HIVE
            elif uri.startswith("postgresql"):
                return CatalogType.SQL
            else:
                raise ValueError(f"Could not infer the catalog type from the uri: {uri}")
        else:
            raise ValueError(f"Expects the URI to be a string, got: {type(uri)}")
    raise ValueError(
        f"URI missing, please provide using --uri, the config or environment variable PYICEBERG_CATALOG__{name.upper()}__URI"
    )


def load_catalog(name: Optional[str], **properties: Optional[str]) -> Catalog:
    """Load the catalog based on the properties.

    Will look up the properties from the config, based on the name.

    Args:
        name: The name of the catalog.
        properties: The properties that are used next to the configuration.

    Returns:
        An initialized Catalog.

    Raises:
        ValueError: Raises a ValueError in case properties are missing or malformed,
            or if it could not determine the catalog based on the properties.
    """
    if name is None:
        name = _ENV_CONFIG.get_default_catalog_name()

    env = _ENV_CONFIG.get_catalog_config(name)
    conf: RecursiveDict = merge_config(env or {}, cast(RecursiveDict, properties))

    catalog_type: Optional[CatalogType]
    provided_catalog_type = conf.get(TYPE)

    catalog_type = None
    if provided_catalog_type and isinstance(provided_catalog_type, str):
        catalog_type = CatalogType[provided_catalog_type.upper()]
    elif not provided_catalog_type:
        catalog_type = infer_catalog_type(name, conf)

    if catalog_type:
        return AVAILABLE_CATALOGS[catalog_type](name, cast(Dict[str, str], conf))

    raise ValueError(f"Could not initialize catalog with the following properties: {properties}")


def delete_files(io: FileIO, files_to_delete: Set[str], file_type: str) -> None:
    """Helper to delete files.

    Log warnings if failing to delete any file.

    Args:
        io: The FileIO used to delete the object.
        files_to_delete: A set of file paths to be deleted.
        file_type: The type of the file.
    """
    for file in files_to_delete:
        try:
            io.delete(file)
        except OSError as exc:
            logger.warning(msg=f"Failed to delete {file_type} file {file}", exc_info=exc)


def delete_data_files(io: FileIO, manifests_to_delete: List[ManifestFile]) -> None:
    """Helper to delete data files linked to given manifests.

    Log warnings if failing to delete any file.

    Args:
        io: The FileIO used to delete the object.
        manifests_to_delete: A list of manifest contains paths of data files to be deleted.
    """
    deleted_files: dict[str, bool] = {}
    for manifest_file in manifests_to_delete:
        for entry in manifest_file.fetch_manifest_entry(io, discard_deleted=False):
            path = entry.data_file.file_path
            if not deleted_files.get(path, False):
                try:
                    io.delete(path)
                except OSError as exc:
                    logger.warning(msg=f"Failed to delete data file {path}", exc_info=exc)
                deleted_files[path] = True


@dataclass
class PropertiesUpdateSummary:
    removed: List[str]
    updated: List[str]
    missing: List[str]


class Catalog(ABC):
    """Base Catalog for table operations like - create, drop, load, list and others.

    The catalog table APIs accept a table identifier, which is fully classified table name. The identifier can be a string or
    tuple of strings. If the identifier is a string, it is split into a tuple on '.'. If it is a tuple, it is used as-is.

    The catalog namespace APIs follow a similar convention wherein they also accept a namespace identifier that can be a string
    or tuple of strings.

    Attributes:
        name (str): Name of the catalog.
        properties (Properties): Catalog properties.
    """

    name: str
    properties: Properties

    def __init__(self, name: str, **properties: str):
        self.name = name
        self.properties = properties

    def _load_file_io(self, properties: Properties = EMPTY_DICT, location: Optional[str] = None) -> FileIO:
        return load_file_io({**self.properties, **properties}, location)

    @abstractmethod
    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        """Create a table.

        Args:
            identifier (str | Identifier): Table identifier.
            schema (Schema): Table's schema.
            location (str | None): Location for the table. Optional Argument.
            partition_spec (PartitionSpec): PartitionSpec for the table.
            sort_order (SortOrder): SortOrder for the table.
            properties (Properties): Table properties that can be a string based dictionary.

        Returns:
            Table: the created table instance.

        Raises:
            TableAlreadyExistsError: If a table with the name already exists.
        """

    @abstractmethod
    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        """Loads the table's metadata and returns the table instance.

        You can also use this method to check for table existence using 'try catalog.table() except NoSuchTableError'.
        Note: This method doesn't scan data stored in the table.

        Args:
            identifier (str | Identifier): Table identifier.

        Returns:
            Table: the table instance with its metadata.

        Raises:
            NoSuchTableError: If a table with the name does not exist.
        """

    @abstractmethod
    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        """Drop a table.

        Args:
            identifier (str | Identifier): Table identifier.

        Raises:
            NoSuchTableError: If a table with the name does not exist.
        """

    @abstractmethod
    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        """Rename a fully classified table name.

        Args:
            from_identifier (str | Identifier): Existing table identifier.
            to_identifier (str | Identifier): New table identifier.

        Returns:
            Table: the updated table instance with its metadata.

        Raises:
            NoSuchTableError: If a table with the name does not exist.
        """

    @abstractmethod
    def _commit_table(self, table_request: CommitTableRequest) -> CommitTableResponse:
        """Updates one or more tables.

        Args:
            table_request (CommitTableRequest): The table requests to be carried out.

        Returns:
            CommitTableResponse: The updated metadata.

        Raises:
            NoSuchTableError: If a table with the given identifier does not exist.
        """

    @abstractmethod
    def create_namespace(self, namespace: Union[str, Identifier], properties: Properties = EMPTY_DICT) -> None:
        """Create a namespace in the catalog.

        Args:
            namespace (str | Identifier): Namespace identifier.
            properties (Properties): A string dictionary of properties for the given namespace.

        Raises:
            NamespaceAlreadyExistsError: If a namespace with the given name already exists.
        """

    @abstractmethod
    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        """Drop a namespace.

        Args:
            namespace (str | Identifier): Namespace identifier.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist.
            NamespaceNotEmptyError: If the namespace is not empty.
        """

    @abstractmethod
    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        """List tables under the given namespace in the catalog.

        If namespace not provided, will list all tables in the catalog.

        Args:
            namespace (str | Identifier): Namespace identifier to search.

        Returns:
            List[Identifier]: list of table identifiers.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist.
        """

    @abstractmethod
    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        """List namespaces from the given namespace. If not given, list top-level namespaces from the catalog.

        Args:
            namespace (str | Identifier): Namespace identifier to search.

        Returns:
            List[Identifier]: a List of namespace identifiers.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist.
        """

    @abstractmethod
    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        """Get properties for a namespace.

        Args:
            namespace (str | Identifier): Namespace identifier.

        Returns:
            Properties: Properties for the given namespace.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist.
        """

    @abstractmethod
    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        """Removes provided property keys and updates properties for a namespace.

        Args:
            namespace (str | Identifier): Namespace identifier.
            removals (Set[str]): Set of property keys that need to be removed. Optional Argument.
            updates (Properties): Properties to be updated for the given namespace.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist.
            ValueError: If removals and updates have overlapping keys.
        """

    @staticmethod
    def identifier_to_tuple(identifier: Union[str, Identifier]) -> Identifier:
        """Parses an identifier to a tuple.

        If the identifier is a string, it is split into a tuple on '.'. If it is a tuple, it is used as-is.

        Args:
            identifier (str | Identifier: an identifier, either a string or tuple of strings.

        Returns:
            Identifier: a tuple of strings.
        """
        return identifier if isinstance(identifier, tuple) else tuple(str.split(identifier, "."))

    @staticmethod
    def table_name_from(identifier: Union[str, Identifier]) -> str:
        """Extracts table name from a table identifier.

        Args:
            identifier (str | Identifier: a table identifier.

        Returns:
            str: Table name.
        """
        return Catalog.identifier_to_tuple(identifier)[-1]

    @staticmethod
    def namespace_from(identifier: Union[str, Identifier]) -> Identifier:
        """Extracts table namespace from a table identifier.

        Args:
            identifier (Union[str, Identifier]): a table identifier.

        Returns:
            Identifier: Namespace identifier.
        """
        return Catalog.identifier_to_tuple(identifier)[:-1]

    @staticmethod
    def _check_for_overlap(removals: Optional[Set[str]], updates: Properties) -> None:
        if updates and removals:
            overlap = set(removals) & set(updates.keys())
            if overlap:
                raise ValueError(f"Updates and deletes have an overlap: {overlap}")

    def _resolve_table_location(self, location: Optional[str], database_name: str, table_name: str) -> str:
        if not location:
            return self._get_default_warehouse_location(database_name, table_name)
        return location

    def _get_default_warehouse_location(self, database_name: str, table_name: str) -> str:
        database_properties = self.load_namespace_properties(database_name)
        if database_location := database_properties.get(LOCATION):
            database_location = database_location.rstrip("/")
            return f"{database_location}/{table_name}"

        if warehouse_path := self.properties.get(WAREHOUSE_LOCATION):
            warehouse_path = warehouse_path.rstrip("/")
            return f"{warehouse_path}/{database_name}.db/{table_name}"

        raise ValueError("No default path is set, please specify a location when creating a table")

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

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        """Drop a table and purge all data and metadata files.

        Note: This method only logs warning rather than raise exception when encountering file deletion failure.

        Args:
            identifier (str | Identifier): Table identifier.

        Raises:
            NoSuchTableError: If a table with the name does not exist, or the identifier is invalid.
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

    @staticmethod
    def _write_metadata(metadata: TableMetadata, io: FileIO, metadata_path: str) -> None:
        ToOutputFile.table_metadata(metadata, io.new_output(metadata_path))

    @staticmethod
    def _get_metadata_location(location: str) -> str:
        return f"{location}/metadata/00000-{uuid.uuid4()}.metadata.json"

    def _get_updated_props_and_update_summary(
        self, current_properties: Properties, removals: Optional[Set[str]], updates: Properties
    ) -> Tuple[PropertiesUpdateSummary, Properties]:
        self._check_for_overlap(updates=updates, removals=removals)
        updated_properties = dict(current_properties)

        removed: Set[str] = set()
        updated: Set[str] = set()

        if removals:
            for key in removals:
                if key in updated_properties:
                    updated_properties.pop(key)
                    removed.add(key)
        if updates:
            for key, value in updates.items():
                updated_properties[key] = value
                updated.add(key)

        expected_to_change = (removals or set()).difference(removed)
        properties_update_summary = PropertiesUpdateSummary(
            removed=list(removed or []), updated=list(updated or []), missing=list(expected_to_change)
        )

        return properties_update_summary, updated_properties
