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
package org.apache.iceberg.flink.maintenance.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Lists the metadata files referenced by the table. */
@Internal
public class ListMetadataFiles extends ProcessFunction<Trigger, String> {
  private static final Logger LOG = LoggerFactory.getLogger(ListMetadataFiles.class);

  private final String taskName;
  private final int taskIndex;
  private transient Counter errorCounter;
  private final TableLoader tableLoader;
  private transient Table table;

  public ListMetadataFiles(String taskName, int taskIndex, TableLoader tableLoader) {
    Preconditions.checkNotNull(taskName, "Task name should no be null");
    Preconditions.checkNotNull(tableLoader, "TableLoader should no be null");
    this.tableLoader = tableLoader;
    this.taskName = taskName;
    this.taskIndex = taskIndex;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    tableLoader.open();
    this.table = tableLoader.loadTable();
    this.errorCounter =
        TableMaintenanceMetrics.groupFor(getRuntimeContext(), table.name(), taskName, taskIndex)
            .counter(TableMaintenanceMetrics.ERROR_COUNTER);
  }

  @Override
  public void processElement(Trigger trigger, Context ctx, Collector<String> collector)
      throws Exception {
    try {
      table
          .snapshots()
          .forEach(
              snapshot -> {
                // Manifest lists
                collector.collect(snapshot.manifestListLocation());
                // Snapshot JSONs
                ReachableFileUtil.metadataFileLocations(table, false).forEach(collector::collect);
                // Statistics files
                ReachableFileUtil.statisticsFilesLocations(table).forEach(collector::collect);
                // Version hint file for Hadoop catalogs
                collector.collect(ReachableFileUtil.versionHintLocation(table));

                // Emit the manifest file locations
                snapshot.allManifests(table.io()).stream()
                    .map(ManifestFile::path)
                    .forEach(collector::collect);
              });
    } catch (Exception e) {
      LOG.error("Exception listing metadata files for {} at {}", table, ctx.timestamp(), e);
      ctx.output(DeleteOrphanFiles.ERROR_STREAM, e);
      errorCounter.inc();
    }
  }
}
