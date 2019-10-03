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

from datetime import datetime
import logging

from iceberg.api import Filterable
from iceberg.api import TableScan
from iceberg.api.expressions import (Binder,
                                     Expressions)
from iceberg.api.io import CloseableGroup
from iceberg.api.types import get_projected_ids, select

from .base_combined_scan_task import BaseCombinedScanTask
from .table_properties import TableProperties
from .util import PackingIterator

_logger = logging.getLogger(__name__)


class BaseTableScan(CloseableGroup, TableScan):
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
    SNAPSHOT_COLUMNS = ["snapshot_id", "file_path", "file_ordinal", "file_format", "block_size_in_bytes",
                        "file_size_in_bytes", "record_count", "partition", "value_counts", "null_value_counts",
                        "lower_bounds", "upper_bounds"]

    def new_refined_scan(self, ops, table, schema, snapshot_id, row_filter,
                         case_sensitive, selected_columns, options, minused_cols):
        raise NotImplementedError()

    def target_split_size(self, ops):
        raise NotImplementedError()

    def __init__(self, ops, table, schema, snapshot_id=None, columns=None,
                 row_filter=None, case_sensitive=True, selected_columns=None, options=None,
                 minused_cols=None):
        self.ops = ops
        self.table = table
        self._schema = schema
        self.snapshot_id = snapshot_id
        self.columns = columns
        self._row_filter = row_filter
        self._case_sensitive = case_sensitive
        self.selected_columns = selected_columns
        self.minused_cols = minused_cols or list()
        self.options = options if options is not None else dict()

        if self.columns is None and self._row_filter is None:
            self.columns = Filterable.ALL_COLUMNS
            self._row_filter = Expressions.always_true()

        self._stats = dict()

    def is_case_sensitive(self):
        return self.case_sensitive

    def use_snapshot(self, snapshot_id):
        if self.snapshot_id is not None:
            raise RuntimeError("Cannot override snapshot, already set to id=%s" % self.snapshot_id)
        if self.ops.current().snapshot(snapshot_id) is None:
            raise RuntimeError("Cannot find snapshot with ID %s" % self.snapshot_id)

        return self.new_refined_scan(self.ops, self.table, self._schema, snapshot_id=snapshot_id,
                                     row_filter=self._row_filter, case_sensitive=self._case_sensitive,
                                     selected_columns=self.selected_columns, options=self.options,
                                     minused_cols=self.minused_cols)

    def as_of_time(self, timestamp_millis):
        raise NotImplementedError()

    def project(self, schema):
        return self.new_refined_scan(self.ops, self.table, schema, snapshot_id=self.snapshot_id,
                                     row_filter=self._row_filter, case_sensitive=self._case_sensitive,
                                     selected_columns=self.selected_columns, options=self.options,
                                     minused_cols=self.minused_cols)

    def case_sensitive(self, case_sensitive):
        return self.new_refined_scan(self.ops, self.table, self._schema, snapshot_id=self.snapshot_id,
                                     row_filter=self._row_filter, case_sensitive=case_sensitive,
                                     selected_columns=self.selected_columns, options=self.options,
                                     minused_cols=self.minused_cols)

    def select(self, columns):
        return self.new_refined_scan(self.ops, self.table, self._schema, snapshot_id=self.snapshot_id,
                                     row_filter=self._row_filter, case_sensitive=self._case_sensitive,
                                     selected_columns=columns, options=self.options,
                                     minused_cols=self.minused_cols)

    def select_except(self, columns):
        return self.new_refined_scan(self.ops, self.table, self._schema, snapshot_id=self.snapshot_id,
                                     row_filter=self._row_filter, case_sensitive=self._case_sensitive,
                                     selected_columns=self.selected_columns, options=self.options,
                                     minused_cols=columns)

    @property
    def row_filter(self):
        return self._row_filter

    def filter(self, expr):
        return self.new_refined_scan(self.ops, self.table, self._schema, snapshot_id=self.snapshot_id,
                                     row_filter=Expressions.and_(self._row_filter, expr),
                                     case_sensitive=self._case_sensitive, selected_columns=self.selected_columns,
                                     options=self.options, minused_cols=self.minused_cols)

    def option(self, property, value):
        builder = dict()
        builder.update(self.options)
        builder[property] = value
        return self.new_refined_scan(self.ops, self.table, self._schema, snapshot_id=self.snapshot_id,
                                     row_filter=self._row_filter, case_sensitive=self._case_sensitive,
                                     selected_columns=self.selected_columns, options=builder,
                                     minused_cols=self.minused_cols)

    def plan_files(self, ops=None, snapshot=None, row_filter=None):

        if not all(i is None for i in [ops, snapshot, row_filter]):
            raise NotImplementedError()

        snapshot = self.ops.current().snapshot(self.snapshot_id) \
            if self.snapshot_id is not None else self.ops.current().current_snapshot()

        if snapshot is not None:
            _logger.info("Scanning table {} snapshot {} created at {} with filter {}"
                         .format(self.table,
                                 snapshot.snapshot_id,
                                 datetime.fromtimestamp(snapshot.timestamp_millis / 1000.0)
                                 .strftime(BaseTableScan.DATE_FORMAT),
                                 self._row_filter))

            return self.plan_files(ops, snapshot, row_filter)
        else:
            _logger.info("Scanning empty table {}" % self.table)

    def plan_tasks(self):
        split_size = self.target_split_size(self.ops)
        lookback = int(self.ops.current().properties.get(TableProperties.SPLIT_LOOKBACK,
                                                         TableProperties.SPLIT_LOOKBACK_DEFAULT))
        open_file_cost = int(self.ops.current().properties.get(TableProperties.SPLIT_OPEN_FILE_COST,
                                                               TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT))

        if not self.ops.conf.get("iceberg.scan.split-file-tasks", True):
            split_files = list(self.plan_files())
        else:
            split_files = self.split_files(split_size)

        def weight_func(file):
            return max(file.length, open_file_cost)

        return (BaseCombinedScanTask(scan_tasks)
                for scan_tasks in PackingIterator(split_files, split_size, lookback, weight_func))

    def split_files(self, split_size):
        file_scan_tasks = list(self.plan_files())
        split_tasks = [task for split_tasks in [scan_task.split(split_size) for scan_task in file_scan_tasks]
                       for task in split_tasks]

        return split_tasks

    @property
    def schema(self):
        return self._lazy_column_projection()

    def to_arrow_table(self):
        raise NotImplementedError()

    def to_pandas(self):
        raise NotImplementedError()

    def _lazy_column_projection(self):
        if "*" in self.selected_columns:
            if len(self.minused_cols) == 0:
                return self._schema
            self.selected_columns = [field.name for field in self._schema.as_struct().fields]
            final_selected_cols = [column for column in self.selected_columns if column not in self.minused_cols]
        else:
            final_selected_cols = self.selected_columns

        required_field_ids = set()
        required_field_ids.update(Binder.bound_references(self.table.schema().as_struct(),
                                                          [self._row_filter],
                                                          self._case_sensitive))

        if self._case_sensitive:
            selected_ids = get_projected_ids(self.table.schema().select(final_selected_cols))
        else:
            selected_ids = get_projected_ids(self.table.schema().case_insensitive_select(final_selected_cols))

        required_field_ids.update(selected_ids)

        return select(self.table.schema(), required_field_ids)

    def __repr__(self):
        return "BaseTableScan(table={}, projection={}, filter={}, case_sensitive={}".format(self.table,
                                                                                            self._schema.as_struct(),
                                                                                            self._row_filter,
                                                                                            self._case_sensitive)

    def __str__(self):
        return self.__repr__()
