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
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Loading the table's manifest files and filtering them based on the partition spec ID. */
@Internal
public class AllManifests extends ProcessFunction<Trigger, ManifestFile> {
  private static final Logger LOG = LoggerFactory.getLogger(AllManifests.class);

  private transient Counter errorCounter;
  private transient Table table;
  private final TableLoader tableLoader;

  private final String taskName;
  private final int taskIndex;

  public AllManifests(TableLoader tableLoader, String taskName, int taskIndex) {
    Preconditions.checkNotNull(tableLoader, "TableLoader should no be null");
    this.tableLoader = tableLoader;
    this.taskName = taskName;
    this.taskIndex = taskIndex;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    if (!this.tableLoader.isOpen()) {
      tableLoader.open();
    }

    this.table = tableLoader.loadTable();
    this.errorCounter =
        TableMaintenanceMetrics.groupFor(getRuntimeContext(), table.name(), taskName, taskIndex)
            .counter(TableMaintenanceMetrics.ERROR_COUNTER);
  }

  @Override
  public void processElement(Trigger trigger, Context ctx, Collector<ManifestFile> out)
      throws Exception {
    try {
      table.currentSnapshot().allManifests(table.io()).stream()
          .filter(m -> m.partitionSpecId() == table.spec().specId())
          .forEach(out::collect);
    } catch (Exception e) {
      LOG.error("Exception fetching manifests for {} at {}", table, ctx.timestamp(), e);
      ctx.output(TaskResultAggregator.ERROR_STREAM, e);
      errorCounter.inc();
    }
  }

  @Override
  public void close() throws Exception {
    super.close();

    if (tableLoader != null) {
      tableLoader.close();
      this.table = null;
    }
  }
}
