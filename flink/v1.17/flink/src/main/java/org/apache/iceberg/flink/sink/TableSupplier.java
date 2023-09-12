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
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.util.SerializableSupplier;

public class TableSupplier implements SerializableSupplier<Table> {

  // FIXME!! make configurable
  private static final long REFRESH_INTERVAL_MS = Duration.ofMinutes(30).toMillis();

  private final Table initialTable;
  private final TableLoader tableLoader;
  private long lastTableLoadMs;
  private transient Table table;

  public TableSupplier(Table initialTable, TableLoader tableLoader) {
    Preconditions.checkArgument(initialTable != null, "initialTable cannot be null");
    Preconditions.checkArgument(tableLoader != null, "tableLoader cannot be null");
    this.initialTable = initialTable;
    this.table = initialTable;
    this.tableLoader = tableLoader;
    this.lastTableLoadMs = System.currentTimeMillis();
  }

  @Override
  public Table get() {
    if (table == null) {
      table = initialTable;
    }

    if (System.currentTimeMillis() - lastTableLoadMs > REFRESH_INTERVAL_MS) {
      table = tableLoader.loadTable();
      lastTableLoadMs = System.currentTimeMillis();
    }

    return table;
  }
}
