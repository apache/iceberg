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
package org.apache.iceberg.flink;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental
public class ReloadingTableSupplier implements TableSupplier {

  private static final Logger LOG = LoggerFactory.getLogger(ReloadingTableSupplier.class);

  private final Table initialTable;
  private final TableLoader tableLoader;
  private final long minReloadIntervalMs;
  private long nextReloadTimeMs;
  private transient Table table;

  public ReloadingTableSupplier(
      Table initialTable, TableLoader tableLoader, long minReloadIntervalMs) {
    Preconditions.checkArgument(initialTable != null, "initialTable cannot be null");
    Preconditions.checkArgument(tableLoader != null, "tableLoader cannot be null");
    Preconditions.checkArgument(minReloadIntervalMs > 0, "minReloadIntervalMs must be > 0");
    this.initialTable = initialTable;
    this.table = initialTable;
    this.tableLoader = tableLoader;
    this.minReloadIntervalMs = minReloadIntervalMs;
    this.nextReloadTimeMs = calcNextReloadTimeMs(System.currentTimeMillis());
  }

  private long calcNextReloadTimeMs(long now) {
    return now + minReloadIntervalMs;
  }

  @Override
  public Table get() {
    if (table == null) {
      this.table = initialTable;
    }
    return table;
  }

  @Override
  public void refreshTable() {
    if (System.currentTimeMillis() > nextReloadTimeMs) {
      if (!tableLoader.isOpen()) {
        tableLoader.open();
      }
      this.table = tableLoader.loadTable();
      nextReloadTimeMs = calcNextReloadTimeMs(System.currentTimeMillis());
      LOG.info(
          "Table {} reloaded, next min load time threshold is {}",
          table.name(),
          DateTimeUtil.formatTimestampMillis(nextReloadTimeMs));
    }
  }
}
