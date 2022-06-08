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
import logging
from multiprocessing import cpu_count
from multiprocessing import Pool

from iceberg.api.expressions import (InclusiveManifestEvaluator,
                                     ResidualEvaluator)

from .base_file_scan_task import BaseFileScanTask
from .base_table_scan import BaseTableScan
from .manifest_reader import ManifestReader
from .partition_spec_parser import PartitionSpecParser
from .schema_parser import SchemaParser
from .table_properties import TableProperties
from .util import SCAN_THREAD_POOL_ENABLED, WORKER_THREAD_POOL_SIZE_PROP


_logger = logging.getLogger(__name__)


class DataTableScan(BaseTableScan):

    SNAPSHOT_COLUMNS = ("snapshot_id", "file_path", "file_ordinal", "file_format", "block_size_in_bytes",
                        "file_size_in_bytes", "record_count", "partition", "value_counts", "null_value_counts",
                        "lower_bounds", "upper_bounds")

    def __init__(self, ops, table, schema=None, snapshot_id=None, row_filter=None,
                 case_sensitive=True, selected_columns=None, options=None, minused_cols=None):
        super(DataTableScan, self).__init__(ops, table, schema if schema is not None else table.schema(),
                                            snapshot_id=snapshot_id, row_filter=row_filter,
                                            case_sensitive=case_sensitive, selected_columns=selected_columns,
                                            options=options, minused_cols=minused_cols)
        self._cached_evaluators = dict()

    def new_refined_scan(self, ops, table, schema, snapshot_id=None, row_filter=None, case_sensitive=None,
                         selected_columns=None, options=None, minused_cols=None):
        return DataTableScan(ops, table, schema,
                             snapshot_id=snapshot_id, row_filter=row_filter, case_sensitive=case_sensitive,
                             selected_columns=selected_columns, options=options, minused_cols=minused_cols)

    def plan_files(self, ops=None, snapshot=None, row_filter=None):
        if all(i is None for i in [ops, snapshot, row_filter]):
            return super(DataTableScan, self).plan_files()

        matching_manifests = [manifest for manifest in snapshot.manifests
                              if self.cache_loader(manifest.spec_id).eval(manifest)]

        if self.ops.conf.get(SCAN_THREAD_POOL_ENABLED):
            with Pool(self.ops.conf.get(WORKER_THREAD_POOL_SIZE_PROP,
                                        cpu_count())) as reader_scan_pool:
                return itertools.chain.from_iterable([scan for scan
                                                      in reader_scan_pool.map(self.get_scans_for_manifest,
                                                                              matching_manifests)])
        else:
            return itertools.chain.from_iterable([self.get_scans_for_manifest(manifest)
                                                  for manifest in matching_manifests])

    def cache_loader(self, spec_id):
        spec = self.ops.current().spec_id(spec_id)
        return InclusiveManifestEvaluator(spec, self.row_filter)

    def get_scans_for_manifest(self, manifest):
        from .filesystem import FileSystemInputFile
        input_file = FileSystemInputFile.from_location(manifest.manifest_path, self.ops.conf)
        reader = ManifestReader.read(input_file)
        schema_str = SchemaParser.to_json(reader.spec.schema)
        spec_str = PartitionSpecParser.to_json(reader.spec)
        residuals = ResidualEvaluator(reader.spec, self.row_filter)
        return [BaseFileScanTask(file, schema_str, spec_str, residuals)
                for file in reader.filter_rows(self.row_filter).select(BaseTableScan.SNAPSHOT_COLUMNS).iterator()]

    def target_split_size(self, ops):
        scan_split_size_str = self.options.get(TableProperties.SPLIT_SIZE)

        if scan_split_size_str is not None:
            try:
                return int(scan_split_size_str)
            except ValueError:
                _logger.warning("Invalid %s option: %s" % (TableProperties.SPLIT_SIZE, scan_split_size_str))

        return int(self.ops.current().properties.get(TableProperties.SPLIT_SIZE, TableProperties.SPLIT_SIZE_DEFAULT))
