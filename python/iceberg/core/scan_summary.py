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

from copy import deepcopy
import functools

from iceberg.api import DataOperations
from iceberg.api.expressions import Expressions, Literal, Operation, UnboundPredicate
from iceberg.api.types import TimestampType

from .manifest_group import ManifestGroup
from .util import str_as_bool

TIMESTAMP_RANGE_MAP = {Operation.LT: lambda min_val, max_val, val: (min_val, val - 1 if val - 1 < max_val else max_val),
                       Operation.LT_EQ: lambda min_val, max_val, val: (min_val, val if val < max_val else max_val),
                       Operation.GT: lambda min_val, max_val, val: (val - 1 if val - 1 > min_val else min_val, max_val),
                       Operation.GT_EQ: lambda min_val, max_val, val: (val if val > min_val else min_val, max_val),
                       Operation.EQ: lambda min_val, max_val, val: (val if val > min_val else min_val,
                                                                    val if val < max_val else max_val)}


class ScanSummary(object):

    IGNORED_OPERATIONS = {DataOperations.DELETE, DataOperations.REPLACE}
    SCAN_SUMMARY_COLUMNS = ["partition", "record_count", "file_size_in_bytes"]


class ScanSummaryBuilder(object):

    TIMESTAMP_NAMES = {"dateCreated", "lastUpdated"}

    def __init__(self, scan):
        self.scan = scan
        self.table = scan.table
        self.ops = self.table.ops
        self.snapshot_timestamps = {snap.snapshot_id: snap.timestamp_millis
                                    for snap in self.table.snapshots()}
        self._throw_if_limited = False
        self.force_use_manifests = False
        self._limit = 2**31 - 1

        self.time_filters = list()

    def add_timestamp_filter(self, filter):
        self.throw_if_limited()
        self.time_filters.append(filter)
        return self

    def after(self, timestamp):
        self.add_timestamp_expression(timestamp, Expressions.greater_than_or_equal)
        return self

    def before(self, timestamp):
        self.add_timestamp_expression(timestamp, Expressions.less_than_or_equal)
        return self

    def add_timestamp_expression(self, timestamp, expr_func):
        if isinstance(timestamp, str):
            timestamp = Literal.of(timestamp).to(TimestampType.without_timezone()).value / 1000

        self.add_timestamp_filter(expr_func("timestamp_ms", timestamp))
        return self

    def throw_if_limited(self):
        self._throw_if_limited = True
        return self

    def limit(self, num_partitions):
        self._limit = num_partitions
        return self

    def use_manifests(self):
        self.force_use_manifests = True
        return self

    def remove_time_filters(self, expressions, expression):
        if expression.op == Operation.AND:
            self.remove_time_filters(expressions, expression.left)
            self.remove_time_filters(expressions, expression.right)
            return
        elif isinstance(expression, UnboundPredicate):
            pred = expression
            ref = pred.ref
            lit = pred.lit

            if ref.name in ScanSummaryBuilder.TIMESTAMP_NAMES:
                ts_literal = lit.to(TimestampType.without_timezone())
                millis = ScanSummaryBuilder.to_millis(ts_literal.value)
                self.add_timestamp_filter(Expressions.predicate(pred.op, "timestamp_ms", millis))
                return

        expressions.append(expression)

    def build(self):
        if self.table.current_snapshot() is None:
            return dict()

        filters = list()
        self.remove_time_filters(filters, Expressions.rewrite_not(self.scan.row_filter))
        row_filter = self.join_filters(filters)

        if len(self.time_filters) == 0:
            return self.from_manifest_scan(self.table.current_snapshot().manifests, row_filter)

        min_timestamp, max_timestamp = self.timestamp_range(self.time_filters)
        oldest_snapshot = self.table.current_snapshot()
        for key, val in self.snapshot_timestamps.items():
            if val < oldest_snapshot.timestamp_millis:
                oldest_snapshot = self.ops.current().snapshot(key)

        # if oldest known snapshot is in the range, then there may be an expired snapshot that has
        # been removed that matched the range. because the timestamp of that snapshot is unknown,
        # it can't be included in the results and the results are not reliable."""
        if oldest_snapshot.timestamp_millis >= min_timestamp and oldest_snapshot <= max_timestamp:
            raise RuntimeError("Cannot satisfy time filters: time range may include expired snapshots")

        snapshots = [snapshot for snapshot in ScanSummaryBuilder.snapshots_in_time_range(self.ops.current(),
                                                                                         min_timestamp,
                                                                                         max_timestamp)
                     if snapshot.operation not in ScanSummary.IGNORED_OPERATIONS]

        result = self.from_partition_summaries(snapshots)
        if result is not None and not self.force_use_manifests:
            return result

        # filter down to the the set of manifest files that were created in the time range, ignoring
        # the snapshots created by delete or replace operations. this is complete because it finds
        # files in the snapshot where they were added to the dataset in either an append or an
        # overwrite. if those files are later compacted with a replace or deleted, those changes are
        # ignored.
        manifests_to_scan = list()
        snapshot_ids = set()

        for snap in snapshots:
            snapshot_ids.add(snap)
            for manifest in snap.manifests:
                if manifest.snapshot_id is not None or manifest.snapshot_id == snap.snapshot_id:
                    manifests_to_scan.append(manifest)

        return self.from_manifest_scan(manifests_to_scan, row_filter, True)

    def from_manifest_scan(self, manifests, row_filter, ignore_existing=False):
        top_n = TopN(self._limit, self._throw_if_limited, lambda x, y: 0 if x == y else -1 if x < y else 1)

        entries = (ManifestGroup(self.ops, manifests)
                   .filter_data(row_filter)
                   .ignore_deleted()
                   .ignore_existing(ignore_existing)
                   .select(ScanSummary.SCAN_SUMMARY_COLUMNS)
                   .entries())

        spec = self.table.spec()
        for entry in entries:
            timestamp = self.snapshot_timestamps.get(entry.snapshot_id)
            partition = spec.partition_to_path(entry.file.partition())
            top_n.update(partition,
                         lambda metrics: ((metrics if metrics is not None else PartitionMetrics())
                                          .update_from_file(entry.file, timestamp)))

        return top_n.get()

    def from_partition_summaries(self, snapshots):
        # try to build the result from snapshot metadata, but fall back if:
        # any snapshot has no summary
        # any snapshot has
        top_n = TopN(self._limit, self._throw_if_limited, lambda x, y: 0 if x == y else -1 if x < y else 1)

        for snap in snapshots:
            if snap.operation is None or snap.summary is None \
                    or str_as_bool(snap.summary.get(SnapshotSummary.PARTITION_SUMMARY_PROP, "false")):
                return None

            for key, _ in snap.summary.items():
                if key.startswith(SnapshotSummary.CHANGED_PARTITION_PREFIX):
                    part_key = key[len(SnapshotSummary.CHANGED_PARTITION_PREFIX):]
                    # part = dict(entry.split("=") for entry in val.split(","))
                    # UPDATE THIS BEFORE FINISHING
                    added_files = 0
                    added_records = 0
                    added_size = 0
                    top_n.update(part_key,
                                 lambda metrics: ((PartitionMetrics() if metrics is None else metrics)
                                                  .update_from_counts(added_files,
                                                                      added_records,
                                                                      added_size,
                                                                      snap.timestamp_millis)))

        return top_n.get()

    @staticmethod
    def snapshots_in_time_range(meta, min_ts, max_ts):
        snapshots = []
        current = meta.current_snapshot()
        while current is not None and current.timestamp_millis >= min_ts:
            current = meta.snapshot(current.parent_id)

        if current.timestamp_millis <= max_ts:
            snapshots.add(current)

        snapshots.reverse()
        return snapshots

    @staticmethod
    def timestamp_range(time_filters):
        min_timestamp = float('-inf')
        max_timestamp = float('inf')

        for pred in time_filters:
            value = pred.lit.val
            try:
                min_timestamp, max_timestamp = TIMESTAMP_RANGE_MAP[pred.op](min_timestamp, max_timestamp, value)
            except KeyError:
                raise RuntimeError("Cannot filter timestamps using predicate: %s" % pred)

        if max_timestamp < min_timestamp:
            raise RuntimeError("No timestamps can match filters: %s" % ", ".join([str(pred)
                                                                                  for pred in time_filters]))

        return min_timestamp, max_timestamp

    @staticmethod
    def join_filters(expressions):
        result = Expressions.always_true()
        for expression in expressions:
            result = Expressions.and_(result, expression)

        return result

    @staticmethod
    def to_millis(timestamp):
        if timestamp < 10000000000:
            # in seconds
            return timestamp * 1000
        elif timestamp < 10000000000000:
            # in millis
            return timestamp

        # in micros
        return timestamp / 1000


class TopN(object):

    def __init__(self, N, throw_if_limited, key_comparator):
        self.max_size = N
        self.throw_if_limited = throw_if_limited
        self.map = dict()
        self.key_comparator = key_comparator
        self.cut = None

    def update(self, key, update_func):
        if self.cut is not None and self.key_comparator(self.cut, key) <= 0:
            return

        self.map[key] = update_func(self.map.get(key))

        while len(map.keys()) > self.max_size:
            if self.throw_if_limited:
                raise RuntimeError("Too many matching keys: more than %s" % self.max_size)

            self.cut = sorted(self.map, key=functools.cmp_to_key(self.key_comparator))[-1]
            del self.map[self.cut]

    def get(self):
        return deepcopy(self.map)


class PartitionMetrics(object):

    def __init__(self):
        self.file_count = 0
        self.record_count = 0
        self.total_size = 0
        self.data_timestamp_millis = None

    def update_from_counts(self, file_count, record_count, files_size, timestamp_millis):
        self.file_count += file_count
        self.record_count += record_count
        self.total_size += files_size

        if self.data_timestamp_millis is None or self.data_timestamp_millis < timestamp_millis:
            self.data_timestamp_millis = timestamp_millis

        return self

    def update_from_file(self, file, timestamp_millis):
        self.file_count += 1
        self.record_count += file.record_count()
        self.total_size += file.files_size_in_bytes()

        if self.data_timestamp_millis is None or self.data_timestamp_millis < timestamp_millis:
            self.data_timestamp_millis = timestamp_millis

        return self

    def __repr__(self):
        items = ("%s=%r" % (k, v) for k, v in self.__dict__.items())
        return "%s(%s)" % (self.__class__.__name__, ','.join(items))

    def __str__(self):
        return self.__repr__()


class SnapshotSummary(object):
    GENIE_ID_PROP = "genie-id"
    ADDED_FILES_PROP = "added-data-files"
    DELETED_FILES_PROP = "deleted-data-files"
    TOTAL_FILES_PROP = "total-data-files"
    ADDED_RECORDS_PROP = "added-records"
    DELETED_RECORDS_PROP = "deleted-records"
    TOTAL_RECORDS_PROP = "total-records"
    ADDED_FILE_SIZE_PROP = "added-files-size"
    DELETED_DUPLICATE_FILES = "deleted-duplicate-files"
    CHANGED_PARTITION_COUNT_PROP = "changed-partition-count"
    CHANGED_PARTITION_PREFIX = "partitions."
    PARTITION_SUMMARY_PROP = "partition-summaries-included"

    def __init__(self):
        pass


class SnapshotSummaryBuilder(object):

    def __init__(self):
        self.changed_partitions = dict()
        self.added_files = 0
        self.deleted_files = 0
        self.deleted_dupicate_files = 0
        self.added_records = 0
        self.deleted_records = 0
        self.properties = dict()

    def clear(self):
        self.changed_partitions = dict()
        self.added_files = 0
        self.deleted_files = 0
        self.deleted_dupicate_files = 0
        self.added_records = 0
        self.deleted_records = 0

    def increment_duplicate_deletes(self):
        self.deleted_dupicate_files += 1

    def deleted_file(self, spec, data_file):
        self.update_partitiomns(spec, data_file, False)
        self.deleted_files += 1
        self.deleted_records += data_file.record_count

    def added_file(self, spec, data_file):
        self.update_partitiomns(spec, data_file, True)
        self.added_files += 1
        self.added_records += data_file.record_count

    def update_partitions(self, spec, file, is_addition):
        key = spec.partition_to_path(file.partition())
        metrics = self.changed_partitions.get(key, PartitionMetrics())

        if is_addition:
            self.changed_partitions[key] = metrics.update_from_file(file, None)

    def build(self):
        builder = dict()
        builder.update(self.properties)

        SnapshotSummaryBuilder.set_if(self.added_files > 0, builder,
                                      SnapshotSummary.ADDED_FILES_PROP, self.added_files)
        SnapshotSummaryBuilder.set_if(self.deleted_files > 0,
                                      builder, SnapshotSummary.DELETED_FILES_PROP, self.deleted_files)
        SnapshotSummaryBuilder.set_if(self.deleted_dupicate_files > 0,
                                      builder, SnapshotSummary.DELETED_DUPLICATE_FILES, self.deleted_dupicate_files)
        SnapshotSummaryBuilder.set_if(self.added_records > 0,
                                      builder, SnapshotSummary.ADDED_RECORDS_PROP, self.added_records)
        SnapshotSummaryBuilder.set_if(self.deleted_records > 0,
                                      builder, SnapshotSummary.DELETED_RECORDS_PROP, self.deleted_records)
        builder[SnapshotSummary.CHANGED_PARTITION_COUNT_PROP] = len(self.changed_partitions.items())

        if len(self.changed_partitions.items()) < 100:
            builder[SnapshotSummary.PARTITION_SUMMARY_PROP] = "true"
            for key, metrics in self.changed_partitions:
                metric_dict = {SnapshotSummary.ADDED_FILES_PROP: metrics.file_count,
                               SnapshotSummary.ADDED_RECORDS_PROP: metrics.record_count,
                               SnapshotSummary.ADDED_FILE_SIZE_PROP: metrics.total_size}
                builder[SnapshotSummary.CHANGED_PARTITION_PREFIX + key] = ",".join(["{}={}".format(key, val)
                                                                                    for inner_key, val
                                                                                    in metric_dict.items()])

        return builder

    def set(self, prop, value):
        self.properties[prop] = value

    @staticmethod
    def set_if(expression, builder, prop, value):
        if expression:
            builder[prop] = value
