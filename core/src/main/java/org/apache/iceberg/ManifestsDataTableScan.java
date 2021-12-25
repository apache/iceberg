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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.ThreadPools;

public class ManifestsDataTableScan extends DataTableScan {

  ManifestsDataTableScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    super(ops, table, schema, context);
    Preconditions.checkState(context.manifests() != null && !Iterables.isEmpty(context.manifests()),
        "Scanned manifests cannot be null or empty");
  }

  @Override
  public TableScan asOfTime(long timestampMillis) {
    throw new UnsupportedOperationException(String.format(
        "Cannot scan table as of time %s: configured for specific manifests %s",
        timestampMillis, context().manifests()));
  }

  @Override
  public TableScan useSnapshot(long scanSnapshotId) {
    throw new UnsupportedOperationException(String.format(
        "Cannot scan table using scan snapshot id %s: configured for specific manifests %s",
        scanSnapshotId, context().manifests()));
  }

  @Override
  public TableScan appendsBetween(long fromSnapshotId, long toSnapshotId) {
    throw new UnsupportedOperationException(String.format(
        "Cannot scan table in snapshots (%s, %s]: configured for specific manifests %s",
        fromSnapshotId, toSnapshotId, context().manifests()));
  }

  @Override
  public TableScan appendsAfter(long fromSnapshotId) {
    throw new UnsupportedOperationException(String.format(
        "Cannot scan appends after %s: configured for specific manifests %s",
        fromSnapshotId, context().manifests()));
  }

  @Override
  public TableScan useManifests(Iterable<ManifestFile> manifests) {
    return new ManifestsDataTableScan(tableOps(), table(), schema(), context().useManifests(manifests));
  }

  @Override
  protected TableScan newRefinedScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    return new ManifestsDataTableScan(ops, table, schema, context);
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    ManifestGroup manifestGroup = new ManifestGroup(tableOps().io(), context().manifests())
        .caseSensitive(isCaseSensitive())
        .select(colStats() ? SCAN_WITH_STATS_COLUMNS : SCAN_COLUMNS)
        .filterData(filter())
        .specsById(tableOps().current().specsById())
        .ignoreDeleted();

    if (shouldIgnoreResiduals()) {
      manifestGroup = manifestGroup.ignoreResiduals();
    }

    if (PLAN_SCANS_WITH_WORKER_POOL && Iterables.size(context().manifests()) > 1) {
      manifestGroup = manifestGroup.planWith(ThreadPools.getWorkerPool());
    }

    return manifestGroup.planFiles();
  }
}
