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

import java.time.Duration;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A table loader that will only reload a table after a certain interval has passed. WARNING: This
 * table loader should be used carefully when used with writer tasks. It could result in heavy load
 * on a catalog for jobs with many writers.
 */
class CachingTableSupplier implements SerializableSupplier<Table> {

  private static final Logger LOG = LoggerFactory.getLogger(CachingTableSupplier.class);

  private final Table initialTable;
  private final TableLoader tableLoader;
  private final Duration tableRefreshInterval;
  private long lastLoadTimeMillis;
  private transient Table table;

  CachingTableSupplier(
      SerializableTable initialTable, TableLoader tableLoader, Duration tableRefreshInterval) {
    Preconditions.checkArgument(initialTable != null, "initialTable cannot be null");
    Preconditions.checkArgument(tableLoader != null, "tableLoader cannot be null");
    Preconditions.checkArgument(
        tableRefreshInterval != null, "tableRefreshInterval cannot be null");
    this.initialTable = initialTable;
    this.table = initialTable;
    this.tableLoader = tableLoader;
    this.tableRefreshInterval = tableRefreshInterval;
    this.lastLoadTimeMillis = System.currentTimeMillis();
  }

  @Override
  public Table get() {
    if (table == null) {
      this.table = initialTable;
    }
    return table;
  }

  Table initialTable() {
    return initialTable;
  }

  void refreshTable() {
    if (System.currentTimeMillis() > lastLoadTimeMillis + tableRefreshInterval.toMillis()) {
      try {
        if (!tableLoader.isOpen()) {
          tableLoader.open();
        }

        this.table = tableLoader.loadTable();
        this.lastLoadTimeMillis = System.currentTimeMillis();

        LOG.info(
            "Table {} reloaded, next min load time threshold is {}",
            table.name(),
            DateTimeUtil.formatTimestampMillis(
                lastLoadTimeMillis + tableRefreshInterval.toMillis()));
      } catch (Exception e) {
        LOG.warn("An error occurred reloading table {}, table was not reloaded", table.name(), e);
      }
    }
  }
}
