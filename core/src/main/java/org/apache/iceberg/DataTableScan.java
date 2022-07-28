/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.SnapshotUtil;

public class DataTableScan extends BaseTableScan {
  static final ImmutableList<String> SCAN_COLUMNS =
      ImmutableList.of(
          "snapshot_id",
          "file_path",
          "file_ordinal",
          "file_format",
          "block_size_in_bytes",
          "file_size_in_bytes",
          "record_count",
          "partition",
          "key_metadata",
          "split_offsets");
  static final ImmutableList<String> SCAN_WITH_STATS_COLUMNS =
      ImmutableList.<String>builder()
          .addAll(SCAN_COLUMNS)
          .add(
              "value_counts",
              "null_value_counts",
              "nan_value_counts",
              "lower_bounds",
              "upper_bounds",
              "column_sizes")
          .build();
  static final boolean PLAN_SCANS_WITH_WORKER_POOL =
      SystemProperties.getBoolean(SystemProperties.SCAN_THREAD_POOL_ENABLED, true);

  public DataTableScan(TableOperations ops, Table table) {
    super(ops, table, table.schema());
  }

  protected DataTableScan(
      TableOperations ops, Table table, Schema schema, TableScanContext context) {
    super(ops, table, schema, context);
  }

  @Override
  public TableScan appendsBetween(long fromSnapshotId, long toSnapshotId) {
    Preconditions.checkState(
        snapshotId() == null,
        "Cannot enable incremental scan, scan-snapshot set to id=%s",
        snapshotId());
    return new IncrementalDataTableScan(
        tableOps(),
        table(),
        schema(),
        context().fromSnapshotIdExclusive(fromSnapshotId).toSnapshotId(toSnapshotId));
  }

  @Override
  public TableScan appendsAfter(long fromSnapshotId) {
    Snapshot currentSnapshot = table().currentSnapshot();
    Preconditions.checkState(
        currentSnapshot != null,
        "Cannot scan appends after %s, there is no current snapshot",
        fromSnapshotId);
    return appendsBetween(fromSnapshotId, currentSnapshot.snapshotId());
  }

  @Override
  public TableScan useSnapshot(long scanSnapshotId) {
    // call method in superclass just for the side effect of argument validation;
    // we do not use its return value
    super.useSnapshot(scanSnapshotId);
    Schema snapshotSchema = SnapshotUtil.schemaFor(table(), scanSnapshotId);
    return newRefinedScan(
        tableOps(), table(), snapshotSchema, context().useSnapshotId(scanSnapshotId));
  }

  @Override
  protected TableScan newRefinedScan(
      TableOperations ops, Table table, Schema schema, TableScanContext context) {
    return new DataTableScan(ops, table, schema, context);
  }

  @Override
  public CloseableIterable<FileScanTask> doPlanFiles() {
    Snapshot snapshot = snapshot();

    FileIO io = table().io();
    ManifestGroup manifestGroup =
        new ManifestGroup(io, snapshot.dataManifests(io), snapshot.deleteManifests(io))
            .caseSensitive(isCaseSensitive())
            .select(colStats() ? SCAN_WITH_STATS_COLUMNS : SCAN_COLUMNS)
            .filterData(filter())
            .specsById(table().specs())
            .ignoreDeleted();

    if (shouldIgnoreResiduals()) {
      manifestGroup = manifestGroup.ignoreResiduals();
    }

    if (snapshot.dataManifests(io).size() > 1
        && (PLAN_SCANS_WITH_WORKER_POOL || context().planWithCustomizedExecutor())) {
      manifestGroup = manifestGroup.planWith(planExecutor());
    }

    return manifestGroup.planFiles();
  }
}
