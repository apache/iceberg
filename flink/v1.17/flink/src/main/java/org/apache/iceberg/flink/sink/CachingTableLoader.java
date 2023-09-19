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

import java.io.IOException;
import java.time.Duration;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A table loader that will only reload a table after a certain interval has passed. WARNING: This
 * table loader should be used carefully when used with writer tasks. It could result in heavy load
 * on a catalog for jobs with many writers.
 */
class CachingTableLoader implements TableLoader {

  private static final Logger LOG = LoggerFactory.getLogger(CachingTableLoader.class);

  private final Table initialTable;
  private final TableLoader tableLoader;
  private final Duration tableRefreshInterval;
  private long nextReloadTimeMs;
  private transient Table table;

  public CachingTableLoader(
      Table initialTable, TableLoader tableLoader, Duration tableRefreshInterval) {
    Preconditions.checkArgument(initialTable != null, "initialTable cannot be null");
    Preconditions.checkArgument(tableLoader != null, "tableLoader cannot be null");
    Preconditions.checkArgument(
        tableRefreshInterval != null, "tableRefreshInterval cannot be null");
    this.initialTable = initialTable;
    this.table = initialTable;
    this.tableLoader = tableLoader;
    this.tableRefreshInterval = tableRefreshInterval;
    this.nextReloadTimeMs = System.currentTimeMillis() + tableRefreshInterval.toMillis();
  }

  @Override
  public void open() {
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }
  }

  @Override
  public boolean isOpen() {
    return tableLoader.isOpen();
  }

  @Override
  public Table loadTable() {
    if (table == null) {
      this.table = initialTable;
    }

    if (System.currentTimeMillis() > nextReloadTimeMs) {
      try {
        this.table = tableLoader.loadTable();
        nextReloadTimeMs = System.currentTimeMillis() + tableRefreshInterval.toMillis();

        LOG.info(
            "Table {} reloaded, next min load time threshold is {}",
            table.name(),
            DateTimeUtil.formatTimestampMillis(nextReloadTimeMs));
      } catch (Exception e) {
        LOG.warn("An error occurred reloading table {}, table was not reloaded", table.name(), e);
      }
    }

    return table;
  }

  @Override
  @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
  public TableLoader clone() {
    return new CachingTableLoader(initialTable, tableLoader, tableRefreshInterval);
  }

  @Override
  public void close() throws IOException {
    tableLoader.close();
  }
}
