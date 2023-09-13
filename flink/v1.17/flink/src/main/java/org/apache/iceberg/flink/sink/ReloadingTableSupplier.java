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
package org.apache.iceberg.flink.sink;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental
public class ReloadingTableSupplier implements SerializableSupplier<Table> {

  private static final Logger LOG = LoggerFactory.getLogger(ReloadingTableSupplier.class);

  static final double JITTER_PCT = 0.01;

  private final Table initialTable;
  private final TableLoader tableLoader;
  private final long reloadIntervalMs;
  private long nextReloadTimeMs;
  private transient Table table;

  public ReloadingTableSupplier(
      Table initialTable, TableLoader tableLoader, long reloadIntervalMs) {
    Preconditions.checkArgument(initialTable != null, "initialTable cannot be null");
    Preconditions.checkArgument(tableLoader != null, "tableLoader cannot be null");
    Preconditions.checkArgument(reloadIntervalMs > 0, "reloadIntervalMs must be > 0");
    this.initialTable = initialTable;
    this.table = initialTable;
    this.tableLoader = tableLoader;
    this.reloadIntervalMs = reloadIntervalMs;
    this.nextReloadTimeMs = calcNextReloadTimeMs(System.currentTimeMillis());
  }

  @VisibleForTesting
  long calcNextReloadTimeMs(long now) {
    long jitter = Math.round(Math.random() * JITTER_PCT * reloadIntervalMs);
    return now + reloadIntervalMs + jitter;
  }

  @Override
  public Table get() {
    if (table == null) {
      table = initialTable;
    }

    if (System.currentTimeMillis() > nextReloadTimeMs) {
      if (!tableLoader.isOpen()) {
        tableLoader.open();
      }
      table = tableLoader.loadTable();
      nextReloadTimeMs = calcNextReloadTimeMs(System.currentTimeMillis());
      LOG.info(
          "Table {} reloaded, next load time is at {}",
          table.name(),
          DateTimeUtil.formatTimestampMillis(nextReloadTimeMs));
    }

    return table;
  }
}
