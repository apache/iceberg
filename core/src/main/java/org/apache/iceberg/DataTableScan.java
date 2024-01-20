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

import java.util.List;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class DataTableScan extends BaseTableScan {

  private volatile List<FileScanTask> files = null;

  protected DataTableScan(Table table, Schema schema, TableScanContext context) {
    super(table, schema, context);
  }

  protected void setFiles(List<FileScanTask> files) {
    this.files = files;
  }

  @Override
  public TableScan appendsBetween(long fromSnapshotId, long toSnapshotId) {
    Preconditions.checkState(
        snapshotId() == null,
        "Cannot enable incremental scan, scan-snapshot set to id=%s",
        snapshotId());
    return new IncrementalDataTableScan(
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
  protected boolean useSnapshotSchema() {
    return true;
  }

  @Override
  protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
    return new DataTableScan(table, schema, context);
  }

  @Override
  public CloseableIterable<FileScanTask> doPlanFiles() {
    Snapshot snapshot = snapshot();

    FileIO io = table().io();
    List<ManifestFile> dataManifests = snapshot.dataManifests(io);
    List<ManifestFile> deleteManifests = snapshot.deleteManifests(io);
    scanMetrics().totalDataManifests().increment((long) dataManifests.size());
    scanMetrics().totalDeleteManifests().increment((long) deleteManifests.size());
    ManifestGroup manifestGroup =
        new ManifestGroup(io, dataManifests, deleteManifests)
            .caseSensitive(isCaseSensitive())
            .select(scanColumns())
            .filterData(filter())
            .specsById(table().specs())
            .scanMetrics(scanMetrics())
            .ignoreDeleted()
            .columnsToKeepStats(columnsToKeepStats());

    if (shouldIgnoreResiduals()) {
      manifestGroup = manifestGroup.ignoreResiduals();
    }

    if (shouldPlanWithExecutor() && (dataManifests.size() > 1 || deleteManifests.size() > 1)) {
      manifestGroup = manifestGroup.planWith(planExecutor());
    }
    CloseableIterable<FileScanTask> temp = manifestGroup.planFiles();
    this.setFiles(Lists.newArrayList(temp));
    return temp;
  }
}
