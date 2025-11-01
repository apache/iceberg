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
package org.apache.iceberg.flink.actions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.ExpireSnapshots;
import org.apache.iceberg.flink.maintenance.api.RewriteDataFiles;
import org.apache.iceberg.flink.maintenance.api.TableMaintenanceAction;

public class Actions {

  public static final Configuration CONFIG =
      new Configuration()
          // disable classloader check as Avro may cache class/object in the serializers.
          .set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);

  private final StreamExecutionEnvironment env;
  private final Table table;
  private final TableLoader tableLoader;

  /**
   * @deprecated Use {@link #Actions(StreamExecutionEnvironment, TableLoader)} instead. Constructs
   *     an Actions instance for the given table.
   */
  @Deprecated
  private Actions(StreamExecutionEnvironment env, Table table) {
    this.env = env;
    this.table = table;
    this.tableLoader = null;
  }

  private Actions(StreamExecutionEnvironment env, TableLoader tableLoader) {
    this.env = env;
    this.tableLoader = tableLoader;
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    this.table = tableLoader.loadTable();
  }

  /**
   * @param table the Iceberg table to perform actions on
   * @return an Actions instance
   * @deprecated Use {@link #forTableLoader(StreamExecutionEnvironment, TableLoader)} instead.
   *     <p>Creates an Actions instance for the given table using the default execution environment.
   */
  @Deprecated
  public static Actions forTable(StreamExecutionEnvironment env, Table table) {
    return new Actions(env, table);
  }

  public static Actions forTableLoader(StreamExecutionEnvironment env, TableLoader tableLoader) {
    return new Actions(env, tableLoader);
  }

  /**
   * @param table the Iceberg table to perform actions on
   * @return an Actions instance
   * @deprecated Use {@link #forTableLoader(TableLoader)} instead.
   *     <p>Creates an Actions instance for the given table using the default execution environment.
   */
  @Deprecated
  public static Actions forTable(Table table) {
    return new Actions(StreamExecutionEnvironment.getExecutionEnvironment(CONFIG), table);
  }

  public static Actions forTableLoader(TableLoader tableLoader) {
    return new Actions(StreamExecutionEnvironment.getExecutionEnvironment(CONFIG), tableLoader);
  }

  /**
   * @deprecated Use {@link #rewriteDataFiles(RewriteDataFiles.Builder)} instead. Constructs an
   *     RewriteDataFilesAction instance for rewrite data files.
   */
  @Deprecated
  public RewriteDataFilesAction rewriteDataFiles() {
    return new RewriteDataFilesAction(env, table);
  }

  public TableMaintenanceAction rewriteDataFiles(RewriteDataFiles.Builder builder) {
    return new TableMaintenanceAction(env, tableLoader, builder, System.currentTimeMillis());
  }

  public TableMaintenanceAction expireSnapshots(ExpireSnapshots.Builder builder) {
    return new TableMaintenanceAction(env, tableLoader, builder, System.currentTimeMillis());
  }
}
