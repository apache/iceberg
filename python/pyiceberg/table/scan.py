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
import itertools
from abc import ABC, abstractmethod
from typing import Iterable, List, Optional

from pydantic import Field

from pyiceberg.expressions import AlwaysTrue, BooleanExpression
from pyiceberg.expressions.visitors import manifest_evaluator
from pyiceberg.io import FileIO
from pyiceberg.manifest import DataFile, ManifestFile
from pyiceberg.table import PartitionSpec, Snapshot, Table
from pyiceberg.utils.iceberg_base_model import IcebergBaseModel

ALWAYS_TRUE = AlwaysTrue()


class FileScanTask(IcebergBaseModel):
    """A scan task over a range of bytes in a single data file."""

    manifest: ManifestFile = Field()
    data_file: DataFile = Field()
    _residual: BooleanExpression = Field()
    spec: PartitionSpec = Field()
    start: int = Field(default=0)

    @property
    def length(self) -> int:
        return self.data_file.file_size_in_bytes


class TableScan(ABC):
    """API for configuring a table scan."""

    table: Table
    snapshot: Snapshot
    expression: BooleanExpression

    def __init__(self, table: Table, snapshot: Optional[Snapshot] = None, expression: Optional[BooleanExpression] = ALWAYS_TRUE):
        self.table = table
        self.expression = expression or ALWAYS_TRUE
        if resolved_snapshot := snapshot or table.current_snapshot():
            self.snapshot = resolved_snapshot
        else:
            raise ValueError("Unable to resolve to a Snapshot to use for the table scan.")

    @abstractmethod
    def plan_files(self) -> Iterable[FileScanTask]:
        """Plan tasks for this scan where each task reads a single file.

        Returns:
            Table: a tuple of tasks scanning entire files required by this scan
        """


class DataTableScan(TableScan):
    """API for configuring a table scan."""

    io: FileIO

    def __init__(
        self, io: FileIO, table: Table, snapshot: Optional[Snapshot] = None, expression: Optional[BooleanExpression] = ALWAYS_TRUE
    ):
        self.io = io
        super().__init__(table, snapshot, expression)

    def plan_files(self) -> Iterable[FileScanTask]:
        matching_manifests = [
            manifest
            for manifest in self.snapshot.fetch_manifest_list(self.io)
            if manifest_evaluator(
                self.table.specs()[manifest.partition_spec_id],
                self.table.schemas()[self.snapshot.schema_id] if self.snapshot.schema_id is not None else self.table.schema(),
                self.expression,
            )(manifest)
        ]

        return itertools.chain.from_iterable(
            [self._fetch_file_scan_tasks_for_manifest(manifest) for manifest in matching_manifests]
        )

    def _fetch_file_scan_tasks_for_manifest(self, manifest: ManifestFile) -> List[FileScanTask]:
        manifest_entries = manifest.fetch_manifest_entry(self.io)
        data_files = [entry.data_file for entry in manifest_entries]

        spec = self.table.specs().get(manifest.partition_spec_id)
        # Row level filters to be implemented. Need projections for evaluating residual. Skipping for the time being.
        return [FileScanTask(manifest=manifest, data_file=file, spec=spec, residual=None) for file in data_files]
