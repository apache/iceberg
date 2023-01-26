import uuid
from abc import ABC, abstractmethod
from typing import (
    Optional,
    Set,
    Tuple,
    Union,
)

from pyiceberg.catalog import (
    MANIFEST,
    MANIFEST_LIST,
    METADATA,
    PREVIOUS_METADATA,
    Catalog,
    Identifier,
    Properties,
    PropertiesUpdateSummary,
    delete_data_files,
    delete_files,
)
from pyiceberg.io import FileIO, load_file_io
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import ToOutputFile
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata, new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.typedef import EMPTY_DICT

EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE"


class BaseAwsCatalog(Catalog, ABC):
    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        """
        Create an Iceberg table

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

        self._create_table(
            identifier=identifier, table_name=table_name, metadata_location=metadata_location, properties=properties
        )

        loaded_table = self.load_table(identifier=identifier)
        return loaded_table

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

    @abstractmethod
    def _create_table(
        self, identifier: Union[str, Identifier], table_name: str, metadata_location: str, properties: Properties = EMPTY_DICT
    ) -> None:
        pass

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


def _write_metadata(metadata: TableMetadata, io: FileIO, metadate_path: str) -> None:
    ToOutputFile.table_metadata(metadata, io.new_output(metadate_path))
