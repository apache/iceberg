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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from itertools import chain
from multiprocessing.pool import ThreadPool
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from pydantic import Field
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
    live_entries,
)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import INITIAL_SEQUENCE_NUMBER, TableMetadata
from pyiceberg.table.snapshots import Snapshot, SnapshotLogEntry
from pyiceberg.table.sorting import SortOrder
from pyiceberg.typedef import (
    EMPTY_DICT,
    Identifier,
    KeyDefaultDict,
    Properties,
)

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa
    import ray
    from duckdb import DuckDBPyConnection

ALWAYS_TRUE = AlwaysTrue()


class Table:
    identifier: Identifier = Field()
    metadata: TableMetadata = Field()
    metadata_location: str = Field()
    io: FileIO

    def __init__(self, identifier: Identifier, metadata: TableMetadata, metadata_location: str, io: FileIO) -> None:
        self.identifier = identifier
        self.metadata = metadata
        self.metadata_location = metadata_location
        self.io = io

    def refresh(self) -> Table:
        """Refresh the current table metadata"""
        raise NotImplementedError("To be implemented")

    def name(self) -> Identifier:
        """Return the identifier of this table"""
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
        """Return the schema for this table"""
        return next(schema for schema in self.metadata.schemas if schema.schema_id == self.metadata.current_schema_id)

    def schemas(self) -> Dict[int, Schema]:
        """Return a dict of the schema of this table"""
        return {schema.schema_id: schema for schema in self.metadata.schemas}

    def spec(self) -> PartitionSpec:
        """Return the partition spec of this table"""
        return next(spec for spec in self.metadata.partition_specs if spec.spec_id == self.metadata.default_spec_id)

    def specs(self) -> Dict[int, PartitionSpec]:
        """Return a dict the partition specs this table"""
        return {spec.spec_id: spec for spec in self.metadata.partition_specs}

    def sort_order(self) -> SortOrder:
        """Return the sort order of this table"""
        return next(
            sort_order for sort_order in self.metadata.sort_orders if sort_order.order_id == self.metadata.default_sort_order_id
        )

    def sort_orders(self) -> Dict[int, SortOrder]:
        """Return a dict of the sort orders of this table"""
        return {sort_order.order_id: sort_order for sort_order in self.metadata.sort_orders}

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
        """Returns the snapshot referenced by the given name or null if no such reference exists."""
        if ref := self.metadata.refs.get(name):
            return self.snapshot_by_id(ref.snapshot_id)
        return None

    def history(self) -> List[SnapshotLogEntry]:
        """Get the snapshot history of this table."""
        return self.metadata.snapshot_log

    def __eq__(self, other: Any) -> bool:
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
        """Refresh the current table metadata"""
        raise NotImplementedError("To be implemented")

    @classmethod
    def from_metadata(cls, metadata_location: str, properties: Properties = EMPTY_DICT) -> StaticTable:
        io = load_file_io(properties=properties, location=metadata_location)
        file = io.new_input(metadata_location)

        from pyiceberg.serializers import FromInputFile

        metadata = FromInputFile.table_metadata(file)

        return cls(
            identifier=("static-table", metadata_location),
            metadata_location=metadata_location,
            metadata=metadata,
            io=load_file_io({**properties, **metadata.properties}),
        )


def _parse_row_filter(expr: Union[str, BooleanExpression]) -> BooleanExpression:
    """Accepts an expression in the form of a BooleanExpression or a string

    In the case of a string, it will be converted into a unbound BooleanExpression

    Args:
        expr: Expression as a BooleanExpression or a string

    Returns: An unbound BooleanExpression
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
        """Creates a copy of this table scan with updated fields."""
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
        for manifest_entry in live_entries(io.new_input(manifest.manifest_path))
        if partition_filter(manifest_entry.data_file) and metrics_evaluator(manifest_entry.data_file)
    ]


def _min_data_file_sequence_number(manifests: List[ManifestFile]) -> int:
    try:
        return min(
            manifest.min_sequence_number or INITIAL_SEQUENCE_NUMBER
            for manifest in manifests
            if manifest.content is None or manifest.content == ManifestContent.DATA
        )
    except ValueError:
        # In case of an empty iterator
        return INITIAL_SEQUENCE_NUMBER


def _match_deletes_to_datafile(data_entry: ManifestEntry, positional_delete_entries: SortedList[ManifestEntry]) -> Set[DataFile]:
    """This method will check if the delete file is relevant for the data file
     by using the column metrics to see if the filename is in the lower and upper bound

    Args:
        data_entry (ManifestEntry): The manifest entry path of the datafile
        positional_delete_entries (List[ManifestEntry]): All the candidate positional deletes manifest entries

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
        """A helper function to make sure that no manifests are loaded that contain deletes
        that are older than the data

        Args:
            min_data_sequence_number (int): The minimal
            manifest (ManifestFile): A ManifestFile that can be either data or deletes

        Returns:
            Boolean indicating if it is either a data file, or a relevant delete file
        """
        return (manifest.content is None or manifest.content == ManifestContent.DATA) or (
            # Not interested in deletes that are older than the data
            manifest.content == ManifestContent.DELETES
            and (manifest.sequence_number or INITIAL_SEQUENCE_NUMBER) >= min_data_sequence_number
        )

    def plan_files(self) -> Iterable[FileScanTask]:
        """Plans the relevant files by filtering on the PartitionSpecs

        Returns:
            List of FileScanTasks that contain both data and delete files
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
        metrics_evaluator = _InclusiveMetricsEvaluator(self.table.schema(), self.row_filter, self.case_sensitive).eval

        min_data_sequence_number = _min_data_file_sequence_number(manifests)

        data_entries: List[ManifestEntry] = []
        positional_delete_entries = SortedList(key=lambda entry: entry.sequence_number or INITIAL_SEQUENCE_NUMBER)

        with ThreadPool() as pool:
            for manifest_entry in chain(
                *pool.starmap(
                    func=_open_manifest,
                    iterable=[
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
                if data_file.content is None or data_file.content == DataFileContent.DATA:
                    data_entries.append(manifest_entry)
                elif data_file.content == DataFileContent.POSITION_DELETES:
                    positional_delete_entries.add(manifest_entry)
                elif data_file.content == DataFileContent.EQUALITY_DELETES:
                    raise ValueError(
                        "PyIceberg does not yet support equality deletes: https://github.com/apache/iceberg/issues/6568"
                    )
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
