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

import itertools
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from pydantic import Field, SerializeAsAny
from sortedcontainers import SortedList

from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    BooleanExpression,
    EqualTo,
    parser,
    visitors,
)
from pyiceberg.expressions.visitors import _InclusiveMetricsEvaluator, inclusive_projection
from pyiceberg.io import FileIO, load_file_io
from pyiceberg.manifest import (
    POSITIONAL_DELETE_SCHEMA,
    DataFile,
    DataFileContent,
    ManifestContent,
    ManifestEntry,
    ManifestFile,
)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import (
    Schema,
    SchemaVisitor,
    assign_fresh_schema_ids,
    index_by_name,
    visit,
)
from pyiceberg.table.metadata import INITIAL_SEQUENCE_NUMBER, TableMetadata
from pyiceberg.table.snapshots import Snapshot, SnapshotLogEntry
from pyiceberg.table.sorting import SortOrder
from pyiceberg.typedef import (
    EMPTY_DICT,
    IcebergBaseModel,
    Identifier,
    KeyDefaultDict,
    Properties,
)
from pyiceberg.types import (
    IcebergType,
    ListType,
    MapType,
    NestedField,
    PrimitiveType,
    StructType,
)
from pyiceberg.utils.concurrent import ExecutorFactory

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa
    import ray
    from duckdb import DuckDBPyConnection

    from pyiceberg.catalog import Catalog


ALWAYS_TRUE = AlwaysTrue()
TABLE_ROOT_ID = -1


class Transaction:
    _table: Table
    _updates: Tuple[TableUpdate, ...]
    _requirements: Tuple[TableRequirement, ...]

    def __init__(
        self,
        table: Table,
        actions: Optional[Tuple[TableUpdate, ...]] = None,
        requirements: Optional[Tuple[TableRequirement, ...]] = None,
    ):
        self._table = table
        self._updates = actions or ()
        self._requirements = requirements or ()

    def __enter__(self) -> Transaction:
        """Start a transaction to update the table."""
        return self

    def __exit__(self, _: Any, value: Any, traceback: Any) -> None:
        """Close and commit the transaction."""
        fresh_table = self.commit_transaction()
        # Update the new data in place
        self._table.metadata = fresh_table.metadata
        self._table.metadata_location = fresh_table.metadata_location

    def _append_updates(self, *new_updates: TableUpdate) -> Transaction:
        """Append updates to the set of staged updates.

        Args:
            *new_updates: Any new updates.

        Raises:
            ValueError: When the type of update is not unique.

        Returns:
            Transaction object with the new updates appended.
        """
        for new_update in new_updates:
            type_new_update = type(new_update)
            if any(type(update) == type_new_update for update in self._updates):
                raise ValueError(f"Updates in a single commit need to be unique, duplicate: {type_new_update}")
        self._updates = self._updates + new_updates
        return self

    def _append_requirements(self, *new_requirements: TableRequirement) -> Transaction:
        """Append requirements to the set of staged requirements.

        Args:
            *new_requirements: Any new requirements.

        Raises:
            ValueError: When the type of requirement is not unique.

        Returns:
            Transaction object with the new requirements appended.
        """
        for requirement in new_requirements:
            type_new_requirement = type(requirement)
            if any(type(update) == type_new_requirement for update in self._updates):
                raise ValueError(f"Requirements in a single commit need to be unique, duplicate: {type_new_requirement}")
        self._requirements = self._requirements + new_requirements
        return self

    def set_table_version(self, format_version: Literal[1, 2]) -> Transaction:
        """Set the table to a certain version.

        Args:
            format_version: The newly set version.

        Returns:
            The alter table builder.
        """
        raise NotImplementedError("Not yet implemented")

    def set_properties(self, **updates: str) -> Transaction:
        """Set properties.

        When a property is already set, it will be overwritten.

        Args:
            updates: The properties set on the table.

        Returns:
            The alter table builder.
        """
        return self._append_updates(SetPropertiesUpdate(updates=updates))

    def update_schema(self) -> UpdateSchema:
        """Create a new UpdateSchema to alter the columns of this table.

        Returns:
            A new UpdateSchema.
        """
        return UpdateSchema(self._table.schema(), self._table, self)

    def remove_properties(self, *removals: str) -> Transaction:
        """Remove properties.

        Args:
            removals: Properties to be removed.

        Returns:
            The alter table builder.
        """
        return self._append_updates(RemovePropertiesUpdate(removals=removals))

    def update_location(self, location: str) -> Transaction:
        """Set the new table location.

        Args:
            location: The new location of the table.

        Returns:
            The alter table builder.
        """
        raise NotImplementedError("Not yet implemented")

    def commit_transaction(self) -> Table:
        """Commit the changes to the catalog.

        Returns:
            The table with the updates applied.
        """
        # Strip the catalog name
        if len(self._updates) > 0:
            response = self._table.catalog._commit_table(  # pylint: disable=W0212
                CommitTableRequest(
                    identifier=self._table.identifier[1:],
                    requirements=self._requirements,
                    updates=self._updates,
                )
            )
            # Update the metadata with the new one
            self._table.metadata = response.metadata
            self._table.metadata_location = response.metadata_location

            return self._table
        else:
            return self._table


class TableUpdateAction(Enum):
    upgrade_format_version = "upgrade-format-version"
    add_schema = "add-schema"
    set_current_schema = "set-current-schema"
    add_spec = "add-spec"
    set_default_spec = "set-default-spec"
    add_sort_order = "add-sort-order"
    set_default_sort_order = "set-default-sort-order"
    add_snapshot = "add-snapshot"
    set_snapshot_ref = "set-snapshot-ref"
    remove_snapshots = "remove-snapshots"
    remove_snapshot_ref = "remove-snapshot-ref"
    set_location = "set-location"
    set_properties = "set-properties"
    remove_properties = "remove-properties"


class TableUpdate(IcebergBaseModel):
    action: TableUpdateAction


class UpgradeFormatVersionUpdate(TableUpdate):
    action: TableUpdateAction = TableUpdateAction.upgrade_format_version
    format_version: int = Field(alias="format-version")


class AddSchemaUpdate(TableUpdate):
    action: TableUpdateAction = TableUpdateAction.add_schema
    schema_: Schema = Field(alias="schema")
    # This field is required: https://github.com/apache/iceberg/pull/7445
    last_column_id: int = Field(alias="last-column-id")


class SetCurrentSchemaUpdate(TableUpdate):
    action: TableUpdateAction = TableUpdateAction.set_current_schema
    schema_id: int = Field(
        alias="schema-id", description="Schema ID to set as current, or -1 to set last added schema", default=-1
    )


class AddPartitionSpecUpdate(TableUpdate):
    action: TableUpdateAction = TableUpdateAction.add_spec
    spec: PartitionSpec


class SetDefaultSpecUpdate(TableUpdate):
    action: TableUpdateAction = TableUpdateAction.set_default_spec
    spec_id: int = Field(
        alias="spec-id", description="Partition spec ID to set as the default, or -1 to set last added spec", default=-1
    )


class AddSortOrderUpdate(TableUpdate):
    action: TableUpdateAction = TableUpdateAction.add_sort_order
    sort_order: SortOrder = Field(alias="sort-order")


class SetDefaultSortOrderUpdate(TableUpdate):
    action: TableUpdateAction = TableUpdateAction.set_default_sort_order
    sort_order_id: int = Field(
        alias="sort-order-id", description="Sort order ID to set as the default, or -1 to set last added sort order", default=-1
    )


class AddSnapshotUpdate(TableUpdate):
    action: TableUpdateAction = TableUpdateAction.add_snapshot
    snapshot: Snapshot


class SetSnapshotRefUpdate(TableUpdate):
    action: TableUpdateAction = TableUpdateAction.set_snapshot_ref
    ref_name: str = Field(alias="ref-name")
    type: Literal["tag", "branch"]
    snapshot_id: int = Field(alias="snapshot-id")
    max_age_ref_ms: int = Field(alias="max-ref-age-ms")
    max_snapshot_age_ms: int = Field(alias="max-snapshot-age-ms")
    min_snapshots_to_keep: int = Field(alias="min-snapshots-to-keep")


class RemoveSnapshotsUpdate(TableUpdate):
    action: TableUpdateAction = TableUpdateAction.remove_snapshots
    snapshot_ids: List[int] = Field(alias="snapshot-ids")


class RemoveSnapshotRefUpdate(TableUpdate):
    action: TableUpdateAction = TableUpdateAction.remove_snapshot_ref
    ref_name: str = Field(alias="ref-name")


class SetLocationUpdate(TableUpdate):
    action: TableUpdateAction = TableUpdateAction.set_location
    location: str


class SetPropertiesUpdate(TableUpdate):
    action: TableUpdateAction = TableUpdateAction.set_properties
    updates: Dict[str, str]


class RemovePropertiesUpdate(TableUpdate):
    action: TableUpdateAction = TableUpdateAction.remove_properties
    removals: List[str]


class TableRequirement(IcebergBaseModel):
    type: str


class AssertCreate(TableRequirement):
    """The table must not already exist; used for create transactions."""

    type: Literal["assert-create"] = Field(default="assert-create")


class AssertTableUUID(TableRequirement):
    """The table UUID must match the requirement's `uuid`."""

    type: Literal["assert-table-uuid"] = Field(default="assert-table-uuid")
    uuid: str


class AssertRefSnapshotId(TableRequirement):
    """The table branch or tag identified by the requirement's `ref` must reference the requirement's `snapshot-id`.

    if `snapshot-id` is `null` or missing, the ref must not already exist.
    """

    type: Literal["assert-ref-snapshot-id"] = Field(default="assert-ref-snapshot-id")
    ref: str
    snapshot_id: int = Field(..., alias="snapshot-id")


class AssertLastAssignedFieldId(TableRequirement):
    """The table's last assigned column id must match the requirement's `last-assigned-field-id`."""

    type: Literal["assert-last-assigned-field-id"] = Field(default="assert-last-assigned-field-id")
    last_assigned_field_id: int = Field(..., alias="last-assigned-field-id")


class AssertCurrentSchemaId(TableRequirement):
    """The table's current schema id must match the requirement's `current-schema-id`."""

    type: Literal["assert-current-schema-id"] = Field(default="assert-current-schema-id")
    current_schema_id: int = Field(..., alias="current-schema-id")


class AssertLastAssignedPartitionId(TableRequirement):
    """The table's last assigned partition id must match the requirement's `last-assigned-partition-id`."""

    type: Literal["assert-last-assigned-partition-id"] = Field(default="assert-last-assigned-partition-id")
    last_assigned_partition_id: int = Field(..., alias="last-assigned-partition-id")


class AssertDefaultSpecId(TableRequirement):
    """The table's default spec id must match the requirement's `default-spec-id`."""

    type: Literal["assert-default-spec-id"] = Field(default="assert-default-spec-id")
    default_spec_id: int = Field(..., alias="default-spec-id")


class AssertDefaultSortOrderId(TableRequirement):
    """The table's default sort order id must match the requirement's `default-sort-order-id`."""

    type: Literal["assert-default-sort-order-id"] = Field(default="assert-default-sort-order-id")
    default_sort_order_id: int = Field(..., alias="default-sort-order-id")


class CommitTableRequest(IcebergBaseModel):
    identifier: Identifier = Field()
    requirements: List[SerializeAsAny[TableRequirement]] = Field(default_factory=list)
    updates: List[SerializeAsAny[TableUpdate]] = Field(default_factory=list)


class CommitTableResponse(IcebergBaseModel):
    metadata: TableMetadata
    metadata_location: str = Field(alias="metadata-location")


class Table:
    identifier: Identifier = Field()
    metadata: TableMetadata
    metadata_location: str = Field()
    io: FileIO
    catalog: Catalog

    def __init__(
        self, identifier: Identifier, metadata: TableMetadata, metadata_location: str, io: FileIO, catalog: Catalog
    ) -> None:
        self.identifier = identifier
        self.metadata = metadata
        self.metadata_location = metadata_location
        self.io = io
        self.catalog = catalog

    def transaction(self) -> Transaction:
        return Transaction(self)

    def refresh(self) -> Table:
        """Refresh the current table metadata."""
        fresh = self.catalog.load_table(self.identifier[1:])
        self.metadata = fresh.metadata
        self.io = fresh.io
        self.metadata_location = fresh.metadata_location
        return self

    def name(self) -> Identifier:
        """Return the identifier of this table."""
        return self.identifier

    def scan(
        self,
        row_filter: Union[str, BooleanExpression] = ALWAYS_TRUE,
        selected_fields: Tuple[str, ...] = ("*",),
        case_sensitive: bool = True,
        snapshot_id: Optional[int] = None,
        options: Properties = EMPTY_DICT,
        limit: Optional[int] = None,
    ) -> DataScan:
        return DataScan(
            table=self,
            row_filter=row_filter,
            selected_fields=selected_fields,
            case_sensitive=case_sensitive,
            snapshot_id=snapshot_id,
            options=options,
            limit=limit,
        )

    def schema(self) -> Schema:
        """Return the schema for this table."""
        return next(schema for schema in self.metadata.schemas if schema.schema_id == self.metadata.current_schema_id)

    def schemas(self) -> Dict[int, Schema]:
        """Return a dict of the schema of this table."""
        return {schema.schema_id: schema for schema in self.metadata.schemas}

    def spec(self) -> PartitionSpec:
        """Return the partition spec of this table."""
        return next(spec for spec in self.metadata.partition_specs if spec.spec_id == self.metadata.default_spec_id)

    def specs(self) -> Dict[int, PartitionSpec]:
        """Return a dict the partition specs this table."""
        return {spec.spec_id: spec for spec in self.metadata.partition_specs}

    def sort_order(self) -> SortOrder:
        """Return the sort order of this table."""
        return next(
            sort_order for sort_order in self.metadata.sort_orders if sort_order.order_id == self.metadata.default_sort_order_id
        )

    def sort_orders(self) -> Dict[int, SortOrder]:
        """Return a dict of the sort orders of this table."""
        return {sort_order.order_id: sort_order for sort_order in self.metadata.sort_orders}

    @property
    def properties(self) -> Dict[str, str]:
        """Properties of the table."""
        return self.metadata.properties

    def location(self) -> str:
        """Return the table's base location."""
        return self.metadata.location

    def current_snapshot(self) -> Optional[Snapshot]:
        """Get the current snapshot for this table, or None if there is no current snapshot."""
        if snapshot_id := self.metadata.current_snapshot_id:
            return self.snapshot_by_id(snapshot_id)
        return None

    def snapshot_by_id(self, snapshot_id: int) -> Optional[Snapshot]:
        """Get the snapshot of this table with the given id, or None if there is no matching snapshot."""
        try:
            return next(snapshot for snapshot in self.metadata.snapshots if snapshot.snapshot_id == snapshot_id)
        except StopIteration:
            return None

    def snapshot_by_name(self, name: str) -> Optional[Snapshot]:
        """Return the snapshot referenced by the given name or null if no such reference exists."""
        if ref := self.metadata.refs.get(name):
            return self.snapshot_by_id(ref.snapshot_id)
        return None

    def history(self) -> List[SnapshotLogEntry]:
        """Get the snapshot history of this table."""
        return self.metadata.snapshot_log

    def update_schema(self) -> UpdateSchema:
        return UpdateSchema(self.schema(), self)

    def __eq__(self, other: Any) -> bool:
        """Return the equality of two instances of the Table class."""
        return (
            self.identifier == other.identifier
            and self.metadata == other.metadata
            and self.metadata_location == other.metadata_location
            if isinstance(other, Table)
            else False
        )


class StaticTable(Table):
    """Load a table directly from a metadata file (i.e., without using a catalog)."""

    def refresh(self) -> Table:
        """Refresh the current table metadata."""
        raise NotImplementedError("To be implemented")

    @classmethod
    def from_metadata(cls, metadata_location: str, properties: Properties = EMPTY_DICT) -> StaticTable:
        io = load_file_io(properties=properties, location=metadata_location)
        file = io.new_input(metadata_location)

        from pyiceberg.serializers import FromInputFile

        metadata = FromInputFile.table_metadata(file)

        from pyiceberg.catalog.noop import NoopCatalog

        return cls(
            identifier=("static-table", metadata_location),
            metadata_location=metadata_location,
            metadata=metadata,
            io=load_file_io({**properties, **metadata.properties}),
            catalog=NoopCatalog("static-table"),
        )


def _parse_row_filter(expr: Union[str, BooleanExpression]) -> BooleanExpression:
    """Accept an expression in the form of a BooleanExpression or a string.

    In the case of a string, it will be converted into a unbound BooleanExpression.

    Args:
        expr: Expression as a BooleanExpression or a string.

    Returns: An unbound BooleanExpression.
    """
    return parser.parse(expr) if isinstance(expr, str) else expr


S = TypeVar("S", bound="TableScan", covariant=True)


class TableScan(ABC):
    table: Table
    row_filter: BooleanExpression
    selected_fields: Tuple[str, ...]
    case_sensitive: bool
    snapshot_id: Optional[int]
    options: Properties
    limit: Optional[int]

    def __init__(
        self,
        table: Table,
        row_filter: Union[str, BooleanExpression] = ALWAYS_TRUE,
        selected_fields: Tuple[str, ...] = ("*",),
        case_sensitive: bool = True,
        snapshot_id: Optional[int] = None,
        options: Properties = EMPTY_DICT,
        limit: Optional[int] = None,
    ):
        self.table = table
        self.row_filter = _parse_row_filter(row_filter)
        self.selected_fields = selected_fields
        self.case_sensitive = case_sensitive
        self.snapshot_id = snapshot_id
        self.options = options
        self.limit = limit

    def snapshot(self) -> Optional[Snapshot]:
        if self.snapshot_id:
            return self.table.snapshot_by_id(self.snapshot_id)
        return self.table.current_snapshot()

    def projection(self) -> Schema:
        snapshot_schema = self.table.schema()
        if snapshot := self.snapshot():
            if snapshot_schema_id := snapshot.schema_id:
                snapshot_schema = self.table.schemas()[snapshot_schema_id]

        if "*" in self.selected_fields:
            return snapshot_schema

        return snapshot_schema.select(*self.selected_fields, case_sensitive=self.case_sensitive)

    @abstractmethod
    def plan_files(self) -> Iterable[ScanTask]:
        ...

    @abstractmethod
    def to_arrow(self) -> pa.Table:
        ...

    @abstractmethod
    def to_pandas(self, **kwargs: Any) -> pd.DataFrame:
        ...

    def update(self: S, **overrides: Any) -> S:
        """Create a copy of this table scan with updated fields."""
        return type(self)(**{**self.__dict__, **overrides})

    def use_ref(self: S, name: str) -> S:
        if self.snapshot_id:
            raise ValueError(f"Cannot override ref, already set snapshot id={self.snapshot_id}")
        if snapshot := self.table.snapshot_by_name(name):
            return self.update(snapshot_id=snapshot.snapshot_id)

        raise ValueError(f"Cannot scan unknown ref={name}")

    def select(self: S, *field_names: str) -> S:
        if "*" in self.selected_fields:
            return self.update(selected_fields=field_names)
        return self.update(selected_fields=tuple(set(self.selected_fields).intersection(set(field_names))))

    def filter(self: S, expr: Union[str, BooleanExpression]) -> S:
        return self.update(row_filter=And(self.row_filter, _parse_row_filter(expr)))

    def with_case_sensitive(self: S, case_sensitive: bool = True) -> S:
        return self.update(case_sensitive=case_sensitive)


class ScanTask(ABC):
    pass


@dataclass(init=False)
class FileScanTask(ScanTask):
    file: DataFile
    delete_files: Set[DataFile]
    start: int
    length: int

    def __init__(
        self,
        data_file: DataFile,
        delete_files: Optional[Set[DataFile]] = None,
        start: Optional[int] = None,
        length: Optional[int] = None,
    ) -> None:
        self.file = data_file
        self.delete_files = delete_files or set()
        self.start = start or 0
        self.length = length or data_file.file_size_in_bytes


def _open_manifest(
    io: FileIO,
    manifest: ManifestFile,
    partition_filter: Callable[[DataFile], bool],
    metrics_evaluator: Callable[[DataFile], bool],
) -> List[ManifestEntry]:
    return [
        manifest_entry
        for manifest_entry in manifest.fetch_manifest_entry(io, discard_deleted=True)
        if partition_filter(manifest_entry.data_file) and metrics_evaluator(manifest_entry.data_file)
    ]


def _min_data_file_sequence_number(manifests: List[ManifestFile]) -> int:
    try:
        return min(
            manifest.min_sequence_number or INITIAL_SEQUENCE_NUMBER
            for manifest in manifests
            if manifest.content == ManifestContent.DATA
        )
    except ValueError:
        # In case of an empty iterator
        return INITIAL_SEQUENCE_NUMBER


def _match_deletes_to_datafile(data_entry: ManifestEntry, positional_delete_entries: SortedList[ManifestEntry]) -> Set[DataFile]:
    """Check if the delete file is relevant for the data file.

    Using the column metrics to see if the filename is in the lower and upper bound.

    Args:
        data_entry (ManifestEntry): The manifest entry path of the datafile.
        positional_delete_entries (List[ManifestEntry]): All the candidate positional deletes manifest entries.

    Returns:
        A set of files that are relevant for the data file.
    """
    relevant_entries = positional_delete_entries[positional_delete_entries.bisect_right(data_entry) :]

    if len(relevant_entries) > 0:
        evaluator = _InclusiveMetricsEvaluator(POSITIONAL_DELETE_SCHEMA, EqualTo("file_path", data_entry.data_file.file_path))
        return {
            positional_delete_entry.data_file
            for positional_delete_entry in relevant_entries
            if evaluator.eval(positional_delete_entry.data_file)
        }
    else:
        return set()


class DataScan(TableScan):
    def __init__(
        self,
        table: Table,
        row_filter: Union[str, BooleanExpression] = ALWAYS_TRUE,
        selected_fields: Tuple[str, ...] = ("*",),
        case_sensitive: bool = True,
        snapshot_id: Optional[int] = None,
        options: Properties = EMPTY_DICT,
        limit: Optional[int] = None,
    ):
        super().__init__(table, row_filter, selected_fields, case_sensitive, snapshot_id, options, limit)

    def _build_partition_projection(self, spec_id: int) -> BooleanExpression:
        project = inclusive_projection(self.table.schema(), self.table.specs()[spec_id])
        return project(self.row_filter)

    @cached_property
    def partition_filters(self) -> KeyDefaultDict[int, BooleanExpression]:
        return KeyDefaultDict(self._build_partition_projection)

    def _build_manifest_evaluator(self, spec_id: int) -> Callable[[ManifestFile], bool]:
        spec = self.table.specs()[spec_id]
        return visitors.manifest_evaluator(spec, self.table.schema(), self.partition_filters[spec_id], self.case_sensitive)

    def _build_partition_evaluator(self, spec_id: int) -> Callable[[DataFile], bool]:
        spec = self.table.specs()[spec_id]
        partition_type = spec.partition_type(self.table.schema())
        partition_schema = Schema(*partition_type.fields)
        partition_expr = self.partition_filters[spec_id]

        evaluator = visitors.expression_evaluator(partition_schema, partition_expr, self.case_sensitive)
        return lambda data_file: evaluator(data_file.partition)

    def _check_sequence_number(self, min_data_sequence_number: int, manifest: ManifestFile) -> bool:
        """Ensure that no manifests are loaded that contain deletes that are older than the data.

        Args:
            min_data_sequence_number (int): The minimal sequence number.
            manifest (ManifestFile): A ManifestFile that can be either data or deletes.

        Returns:
            Boolean indicating if it is either a data file, or a relevant delete file.
        """
        return manifest.content == ManifestContent.DATA or (
            # Not interested in deletes that are older than the data
            manifest.content == ManifestContent.DELETES
            and (manifest.sequence_number or INITIAL_SEQUENCE_NUMBER) >= min_data_sequence_number
        )

    def plan_files(self) -> Iterable[FileScanTask]:
        """Plans the relevant files by filtering on the PartitionSpecs.

        Returns:
            List of FileScanTasks that contain both data and delete files.
        """
        snapshot = self.snapshot()
        if not snapshot:
            return iter([])

        io = self.table.io

        # step 1: filter manifests using partition summaries
        # the filter depends on the partition spec used to write the manifest file, so create a cache of filters for each spec id

        manifest_evaluators: Dict[int, Callable[[ManifestFile], bool]] = KeyDefaultDict(self._build_manifest_evaluator)

        manifests = [
            manifest_file
            for manifest_file in snapshot.manifests(io)
            if manifest_evaluators[manifest_file.partition_spec_id](manifest_file)
        ]

        # step 2: filter the data files in each manifest
        # this filter depends on the partition spec used to write the manifest file

        partition_evaluators: Dict[int, Callable[[DataFile], bool]] = KeyDefaultDict(self._build_partition_evaluator)
        metrics_evaluator = _InclusiveMetricsEvaluator(
            self.table.schema(), self.row_filter, self.case_sensitive, self.options.get("include_empty_files") == "true"
        ).eval

        min_data_sequence_number = _min_data_file_sequence_number(manifests)

        data_entries: List[ManifestEntry] = []
        positional_delete_entries = SortedList(key=lambda entry: entry.data_sequence_number or INITIAL_SEQUENCE_NUMBER)

        executor = ExecutorFactory.get_or_create()
        for manifest_entry in chain(
            *executor.map(
                lambda args: _open_manifest(*args),
                [
                    (
                        io,
                        manifest,
                        partition_evaluators[manifest.partition_spec_id],
                        metrics_evaluator,
                    )
                    for manifest in manifests
                    if self._check_sequence_number(min_data_sequence_number, manifest)
                ],
            )
        ):
            data_file = manifest_entry.data_file
            if data_file.content == DataFileContent.DATA:
                data_entries.append(manifest_entry)
            elif data_file.content == DataFileContent.POSITION_DELETES:
                positional_delete_entries.add(manifest_entry)
            elif data_file.content == DataFileContent.EQUALITY_DELETES:
                raise ValueError("PyIceberg does not yet support equality deletes: https://github.com/apache/iceberg/issues/6568")
            else:
                raise ValueError(f"Unknown DataFileContent ({data_file.content}): {manifest_entry}")

        return [
            FileScanTask(
                data_entry.data_file,
                delete_files=_match_deletes_to_datafile(
                    data_entry,
                    positional_delete_entries,
                ),
            )
            for data_entry in data_entries
        ]

    def to_arrow(self) -> pa.Table:
        from pyiceberg.io.pyarrow import project_table

        return project_table(
            self.plan_files(),
            self.table,
            self.row_filter,
            self.projection(),
            case_sensitive=self.case_sensitive,
            limit=self.limit,
        )

    def to_pandas(self, **kwargs: Any) -> pd.DataFrame:
        return self.to_arrow().to_pandas(**kwargs)

    def to_duckdb(self, table_name: str, connection: Optional[DuckDBPyConnection] = None) -> DuckDBPyConnection:
        import duckdb

        con = connection or duckdb.connect(database=":memory:")
        con.register(table_name, self.to_arrow())

        return con

    def to_ray(self) -> ray.data.dataset.Dataset:
        import ray

        return ray.data.from_arrow(self.to_arrow())


class UpdateSchema:
    _table: Table
    _schema: Schema
    _last_column_id: itertools.count[int]
    _identifier_field_names: List[str]
    _adds: Dict[int, List[NestedField]]
    _added_name_to_id: Dict[str, int]
    _id_to_parent: Dict[int, str]
    _allow_incompatible_changes: bool
    _case_sensitive: bool
    _transaction: Optional[Transaction]

    def __init__(self, schema: Schema, table: Table, transaction: Optional[Transaction] = None):
        self._table = table
        self._schema = schema
        self._last_column_id = itertools.count(schema.highest_field_id + 1)
        self._identifier_field_names = schema.column_names
        self._adds = {}
        self._added_name_to_id = {}
        self._id_to_parent = {}
        self._allow_incompatible_changes = False
        self._case_sensitive = True
        self._transaction = transaction

    def __exit__(self, _: Any, value: Any, traceback: Any) -> None:
        """Close and commit the change."""
        return self.commit()

    def __enter__(self) -> UpdateSchema:
        """Update the table."""
        return self

    def case_sensitive(self, case_sensitive: bool) -> UpdateSchema:
        """Determine if the case of schema needs to be considered when comparing column names.

        Args:
            case_sensitive: When false case is not considered in column name comparisons.

        Returns:
            This for method chaining
        """
        self._case_sensitive = case_sensitive
        return self

    def add_column(
        self, name: str, type_var: IcebergType, doc: Optional[str] = None, parent: Optional[str] = None, required: bool = False
    ) -> UpdateSchema:
        """Add a new column to a nested struct or Add a new top-level column.

        Args:
            name: Name for the new column.
            type_var: Type for the new column.
            doc: Documentation string for the new column.
            parent: Name of the parent struct to the column will be added to.
            required: Whether the new column is required.

        Returns:
            This for method chaining
        """
        if "." in name:
            raise ValueError(f"Cannot add column with ambiguous name: {name}")

        if required and not self._allow_incompatible_changes:
            # Table format version 1 and 2 cannot add required column because there is no initial value
            raise ValueError(f"Incompatible change: cannot add required column: {name}")

        self._internal_add_column(parent, name, not required, type_var, doc)
        return self

    def allow_incompatible_changes(self) -> UpdateSchema:
        """Allow incompatible changes to the schema.

        Returns:
            This for method chaining
        """
        self._allow_incompatible_changes = True
        return self

    def commit(self) -> None:
        """Apply the pending changes and commit."""
        new_schema = self._apply()
        updates = [
            AddSchemaUpdate(schema=new_schema, last_column_id=new_schema.highest_field_id),
            SetCurrentSchemaUpdate(schema_id=-1),
        ]
        requirements = [AssertCurrentSchemaId(current_schema_id=self._schema.schema_id)]

        if self._transaction is not None:
            self._transaction._append_updates(*updates)  # pylint: disable=W0212
            self._transaction._append_requirements(*requirements)  # pylint: disable=W0212
        else:
            table_update_response = self._table.catalog._commit_table(  # pylint: disable=W0212
                CommitTableRequest(identifier=self._table.identifier[1:], updates=updates, requirements=requirements)
            )
            self._table.metadata = table_update_response.metadata
            self._table.metadata_location = table_update_response.metadata_location

    def _apply(self) -> Schema:
        """Apply the pending changes to the original schema and returns the result.

        Returns:
            the result Schema when all pending updates are applied
        """
        return _apply_changes(self._schema, self._adds, self._identifier_field_names)

    def _internal_add_column(
        self, parent: Optional[str], name: str, is_optional: bool, type_var: IcebergType, doc: Optional[str]
    ) -> None:
        full_name: str = name
        parent_id: int = TABLE_ROOT_ID

        exist_field: Optional[NestedField] = None
        if parent:
            parent_field = self._schema.find_field(parent, self._case_sensitive)
            parent_type = parent_field.field_type
            if isinstance(parent_type, MapType):
                parent_field = parent_type.value_field
            elif isinstance(parent_type, ListType):
                parent_field = parent_type.element_field

            if not parent_field.field_type.is_struct:
                raise ValueError(f"Cannot add column to non-struct type: {parent}")

            parent_id = parent_field.field_id

            try:
                exist_field = self._schema.find_field(parent + "." + name, self._case_sensitive)
            except ValueError:
                pass

            if exist_field:
                raise ValueError(f"Cannot add column, name already exists: {parent}.{name}")

            full_name = parent_field.name + "." + name

        else:
            try:
                exist_field = self._schema.find_field(name, self._case_sensitive)
            except ValueError:
                pass

            if exist_field:
                raise ValueError(f"Cannot add column, name already exists: {name}")

        # assign new IDs in order
        new_id = self.assign_new_column_id()

        # update tracking for moves
        self._added_name_to_id[full_name] = new_id

        new_type = assign_fresh_schema_ids(type_var, self.assign_new_column_id)
        field = NestedField(new_id, name, new_type, not is_optional, doc)

        self._adds.setdefault(parent_id, []).append(field)

    def assign_new_column_id(self) -> int:
        return next(self._last_column_id)


def _apply_changes(schema_: Schema, adds: Dict[int, List[NestedField]], identifier_field_names: List[str]) -> Schema:
    struct = visit(schema_, _ApplyChanges(adds))
    name_to_id: Dict[str, int] = index_by_name(struct)
    for name in identifier_field_names:
        if name not in name_to_id:
            raise ValueError(f"Cannot add field {name} as an identifier field: not found in current schema or added columns")

    return Schema(*struct.fields)


class _ApplyChanges(SchemaVisitor[IcebergType]):
    def __init__(self, adds: Dict[int, List[NestedField]]):
        self.adds = adds

    def schema(self, schema: Schema, struct_result: IcebergType) -> IcebergType:
        fields = _ApplyChanges.add_fields(schema.as_struct().fields, self.adds.get(TABLE_ROOT_ID))
        if len(fields) > 0:
            return StructType(*fields)

        return struct_result

    def struct(self, struct: StructType, field_results: List[IcebergType]) -> IcebergType:
        has_change = False
        new_fields: List[NestedField] = []
        for i in range(len(field_results)):
            type_: Optional[IcebergType] = field_results[i]
            if type_ is None:
                has_change = True
                continue

            field: NestedField = struct.fields[i]
            new_fields.append(field)

        if has_change:
            return StructType(*new_fields)

        return struct

    def field(self, field: NestedField, field_result: IcebergType) -> IcebergType:
        field_id: int = field.field_id
        if field_id in self.adds:
            new_fields = self.adds[field_id]
            if len(new_fields) > 0:
                fields = _ApplyChanges.add_fields(field_result.fields, new_fields)
                if len(fields) > 0:
                    return StructType(*fields)

        return field_result

    def list(self, list_type: ListType, element_result: IcebergType) -> IcebergType:
        element_field: NestedField = list_type.element_field
        element_type = self.field(element_field, element_result)
        if element_type is None:
            raise ValueError(f"Cannot delete element type from list: {element_field}")

        is_element_optional = not list_type.element_required

        if is_element_optional == element_field.required and list_type.element_type == element_type:
            return list_type

        return ListType(list_type.element_id, element_type, is_element_optional)

    def map(self, map_type: MapType, key_result: IcebergType, value_result: IcebergType) -> IcebergType:
        key_id: int = map_type.key_field.field_id
        if key_id in self.adds:
            raise ValueError(f"Cannot add fields to map keys: {map_type}")

        value_field: NestedField = map_type.value_field
        value_type = self.field(value_field, value_result)
        if value_type is None:
            raise ValueError(f"Cannot delete value type from map: {value_field}")

        is_value_optional = not map_type.value_required

        if is_value_optional != value_field.required and map_type.value_type == value_type:
            return map_type

        return MapType(map_type.key_id, map_type.key_field, map_type.value_id, value_type, not is_value_optional)

    def primitive(self, primitive: PrimitiveType) -> IcebergType:
        return primitive

    @staticmethod
    def add_fields(fields: Tuple[NestedField, ...], adds: Optional[List[NestedField]]) -> List[NestedField]:
        new_fields: List[NestedField] = []
        new_fields.extend(fields)
        if adds:
            new_fields.extend(adds)
        return new_fields
