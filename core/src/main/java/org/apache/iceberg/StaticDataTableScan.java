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
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticDataTableScan extends DataTableScan {
  private static final Logger LOG = LoggerFactory.getLogger(StaticDataTableScan.class);

  private static final long DUMMY_SNAPSHOT_ID = 0L;

  private final List<ManifestFile> dataManifests;
  private final List<ManifestFile> deleteManifests;

  private StaticDataTableScan(Table table,
                              TableOperations ops) {
    super(ops, table);
    this.dataManifests = Lists.newArrayList();
    this.deleteManifests = Lists.newArrayList();
  }

  @Override
  public TableScan useSnapshot(long scanSnapshotId) {
    throw new UnsupportedOperationException(String.format(
        "Scan table using scan snapshot id %s is not supported", scanSnapshotId));
  }

  @Override
  public TableScan asOfTime(long timestampMillis) {
    throw new UnsupportedOperationException(String.format(
        "Scan table as of time %s is not supported", timestampMillis));
  }

  @Override
  public TableScan appendsBetween(long fromSnapshotId, long toSnapshotId) {
    throw new UnsupportedOperationException("Incremental scan is not supported");
  }

  @Override
  public TableScan appendsAfter(long fromSnapshotId) {
    throw new UnsupportedOperationException("Incremental scan is not supported");
  }

  @Override
  public Snapshot snapshot() {
    return null;
  }

  public TableScan scan(Iterable<ManifestFile> newManifests) {
    for (ManifestFile manifestFile : newManifests) {
      Preconditions.checkArgument(manifestFile != null, "Cannot scan a null manifest file");
      switch (manifestFile.content()) {
        case DATA:
          dataManifests.add(manifestFile);
          break;
        case DELETES:
          deleteManifests.add(manifestFile);
          break;
        default: throw new IllegalArgumentException("Unknown ManifestContent type: " + manifestFile.content());
      }
    }
    return this;
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    LOG.info("Scanning table {} partially with filter {}", table(), context().rowFilter());

    Listeners.notifyAll(new ScanEvent(table().name(), DUMMY_SNAPSHOT_ID, context().rowFilter(), schema()));

    return planFiles(tableOps(), null,
        context().rowFilter(), context().ignoreResiduals(), context().caseSensitive(), context().returnColumnStats());
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles(TableOperations ops, Snapshot snapshot, Expression rowFilter,
                                                   boolean ignoreResiduals, boolean caseSensitive, boolean colStats) {
    ManifestGroup manifestGroup = new ManifestGroup(ops.io(), dataManifests, deleteManifests)
        .caseSensitive(caseSensitive)
        .select(colStats ? SCAN_WITH_STATS_COLUMNS : SCAN_COLUMNS)
        .filterData(rowFilter)
        .specsById(ops.current().specsById())
        .ignoreDeleted();

    if (ignoreResiduals) {
      manifestGroup = manifestGroup.ignoreResiduals();
    }

    if (PLAN_SCANS_WITH_WORKER_POOL && dataManifests.size() > 1) {
      manifestGroup = manifestGroup.planWith(ThreadPools.getWorkerPool());
    }

    return manifestGroup.planFiles();
  }

  public static StaticDataTableScan of(Table table, TableOperations ops) {
    return new StaticDataTableScan(table, ops);
  }
}
