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
from typing import (
    ClassVar,
    Dict,
    List,
    Optional,
)

from pyiceberg.expressions import AlwaysTrue, And, BooleanExpression
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.table.partitioning import PartitionSpec
from pyiceberg.table.snapshots import Snapshot, SnapshotLogEntry
from pyiceberg.table.sorting import SortOrder
from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties


class Table:
    identifier: Identifier
    metadata: TableMetadata
    metadata_location: Optional[str]

    def __init__(self, identifier: Identifier, metadata: TableMetadata, metadata_location: Optional[str]):
        self.identifier = identifier
        self.metadata = metadata
        self.metadata_location = metadata_location

    def refresh(self):
        """Refresh the current table metadata"""
        raise NotImplementedError("To be implemented")

    def name(self) -> Identifier:
        """Return the identifer of this table"""
        return self.identifier

    def scan(self, **kwargs):
        return TableScan(**{**kwargs, "table": self})

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


class TableScan:
    _always_true: ClassVar[BooleanExpression] = AlwaysTrue()
    table: Table
    row_filter: BooleanExpression
    partition_filter: BooleanExpression
    selected_fields: tuple[str]
    case_sensitive: bool
    snapshot_id: Optional[int]
    options: Properties

    def __init__(
        self,
        *,
        table: Table,
        row_filter: BooleanExpression = _always_true,
        partition_filter: BooleanExpression = _always_true,
        selected_fields: tuple[str] = ("*",),
        case_sensitive: bool = True,
        snapshot_id: Optional[int] = None,
        options: Properties = EMPTY_DICT,
    ):
        self.table = table
        self.row_filter = row_filter
        self.partition_filter = partition_filter
        self.selected_fields = selected_fields
        self.case_sensitive = case_sensitive
        self.snapshot_id = snapshot_id
        self.options = options

    def update(self, **overrides):
        """Creates a copy of this table scan with updated fields."""
        return TableScan(**{**self.__dict__, **overrides})

    def snapshot(self):
        if self.snapshot_id:
            return self.table.snapshot_by_id(self.snapshot_id)

        return self.table.current_snapshot()

    def projection(self):
        snapshot_schema = self.table.schemas().get(self.snapshot().schema_id) or self.table.schema()

        if "*" in self.selected_fields:
            return snapshot_schema

        return snapshot_schema.select(*self.selected_fields, case_sensitive=self.case_sensitive)

    def use_snapshot(self, snapshot_id: int):
        if self.snapshot_id:
            raise ValueError(f"Cannot override snapshot, already set snapshot id={self.snapshot_id}")
        if self.table.snapshot_by_id(snapshot_id):
            return self.update(snapshot_id=snapshot_id)

        raise ValueError(f"Cannot scan unknown snapshot id={snapshot_id}")

    def use_ref(self, name: str):
        if self.snapshot_id:
            raise ValueError(f"Cannot override ref, already set snapshot id={self.snapshot_id}")
        if snapshot := self.table.snapshot_by_name(name):
            return self.update(snapshot_id=snapshot.snapshot_id)

        raise ValueError(f"Cannot scan unknown ref={name}")

    def select(self, *field_names: str) -> "TableScan":
        if "*" in self.selected_fields:
            return self.update(selected_fields=field_names)
        return self.update(selected_fields=tuple(set(self.selected_fields).intersection(field_names)))

    def filter_rows(self, new_row_filter: BooleanExpression) -> "TableScan":
        return self.update(row_filter=And(self.row_filter, new_row_filter))

    def filter_partitions(self, new_partition_filter: BooleanExpression) -> "TableScan":
        return self.update(partition_filter=And(self.partition_filter, new_partition_filter))

    def with_case_sensitive(self, case_sensitive: bool = True) -> "TableScan":
        return self.update(case_sensitive=case_sensitive)

    def set(self, **options: str):
        return self.update(options={**self.options, **options})

    def plan_files(self):
        raise NotImplementedError("Not yet implemented")

    def to_arrow(self):
        raise NotImplementedError("Not yet implemented")
