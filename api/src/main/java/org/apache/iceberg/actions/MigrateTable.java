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
package org.apache.iceberg.actions;

import java.util.Map;

/** An action that migrates an existing table to Iceberg. */
public interface MigrateTable extends Action<MigrateTable, MigrateTable.Result> {
  /**
   * Sets table properties in the newly created Iceberg table. Any properties with the same key name
   * will be overwritten.
   *
   * @param properties a map of properties to set
   * @return this for method chaining
   */
  MigrateTable tableProperties(Map<String, String> properties);

  /**
   * Sets a table property in the newly created Iceberg table. Any properties with the same key will
   * be overwritten.
   *
   * @param name a table property name
   * @param value a table property value
   * @return this for method chaining
   */
  MigrateTable tableProperty(String name, String value);

  /**
   * Drops the backup of the original table after a successful migration
   *
   * @return this for method chaining
   */
  default MigrateTable dropBackup() {
    throw new UnsupportedOperationException("Dropping a backup is not supported");
  }

  /**
   * Sets a table name for the backup of the original table.
   *
   * @param tableName the table name for backup
   * @return this for method chaining
   */
  default MigrateTable backupTableName(String tableName) {
    throw new UnsupportedOperationException("Backup table name cannot be specified");
  }

  /**
   * Sets the number of threads to use for file reading. The default is 1.
   *
   * @param numThreads the number of threads
   * @return this for method chaining
   */
  default MigrateTable parallelism(int numThreads) {
    throw new UnsupportedOperationException("Setting parallelism is not supported");
  }

  /** The action result that contains a summary of the execution. */
  interface Result {
    /** Returns the number of migrated data files. */
    long migratedDataFilesCount();
  }
}
