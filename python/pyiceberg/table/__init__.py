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
from functools import cached_property
from typing import (
    Dict,
    List,
    Optional,
    Union,
)

from pydantic import Field

from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadataV1, TableMetadataV2
from pyiceberg.table.partitioning import PartitionSpec
from pyiceberg.table.snapshots import Snapshot, SnapshotLogEntry
from pyiceberg.table.sorting import SortOrder
from pyiceberg.typedef import Identifier
from pyiceberg.utils.iceberg_base_model import IcebergBaseModel


class Table(IcebergBaseModel):
    identifier: Identifier = Field()
    metadata_location: str = Field()
    metadata: Union[TableMetadataV1, TableMetadataV2] = Field()

    def refresh(self, catalog) -> "Table":
        """Refresh the current table metadata"""
        table_catalog_name = self.identifier[0]
        if catalog.name != table_catalog_name:
            raise ValueError(f"Catalog mismatch: Table catalog={table_catalog_name}, catalog={catalog.name}")

        fresh_table = catalog.load_table(self.identifier[1:])

        # If the metadata has changed, we can assume that the table has been updated
        if fresh_table.metadata_location != self.metadata_location:
            return fresh_table
        else:
            return self

    @cached_property
    def schema(self) -> Schema:
        """Return the schema for this table"""
        return next(schema for schema in self.metadata.schemas if schema.schema_id == self.metadata.current_schema_id)

    @property
    def schemas(self) -> Dict[int, Schema]:
        """Return a dict of the schema of this table"""
        return {schema.schema_id: schema for schema in self.metadata.schemas}

    @cached_property
    def spec(self) -> PartitionSpec:
        """Return the partition spec of this table"""
        return next(spec for spec in self.metadata.partition_specs if spec.spec_id == self.metadata.default_spec_id)

    @property
    def specs(self) -> Dict[int, PartitionSpec]:
        """Return a dict the partition specs this table"""
        return {spec.spec_id: spec for spec in self.metadata.partition_specs}

    @cached_property
    def sort_order(self) -> SortOrder:
        """Return the sort order of this table"""
        return next(
            sort_order for sort_order in self.metadata.sort_orders if sort_order.order_id == self.metadata.default_sort_order_id
        )

    @property
    def sort_orders(self) -> Dict[int, SortOrder]:
        """Return a dict of the sort orders of this table"""
        return {sort_order.order_id: sort_order for sort_order in self.metadata.sort_orders}

    @property
    def location(self) -> str:
        """Return the table's base location."""
        return self.metadata.location

    @cached_property
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

    @property
    def history(self) -> List[SnapshotLogEntry]:
        """Get the snapshot history of this table."""
        return self.metadata.snapshot_log
