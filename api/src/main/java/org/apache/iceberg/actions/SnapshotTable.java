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
import java.util.concurrent.ExecutorService;

/** An action that creates an independent snapshot of an existing table. */
public interface SnapshotTable extends Action<SnapshotTable, SnapshotTable.Result> {
  /**
   * Sets the table identifier for the newly created Iceberg table.
   *
   * @param destTableIdent the destination table identifier
   * @return this for method chaining
   */
  SnapshotTable as(String destTableIdent);

  /**
   * Sets the table location for the newly created Iceberg table.
   *
   * @param location a table location
   * @return this for method chaining
   */
  SnapshotTable tableLocation(String location);

  /**
   * Sets table properties in the newly created Iceberg table. Any properties with the same key name
   * will be overwritten.
   *
   * @param properties a map of properties to be included
   * @return this for method chaining
   */
  SnapshotTable tableProperties(Map<String, String> properties);

  /**
   * Sets a table property in the newly created Iceberg table. Any properties with the same key name
   * will be overwritten.
   *
   * @param key the key of the property to add
   * @param value the value of the property to add
   * @return this for method chaining
   */
  SnapshotTable tableProperty(String key, String value);

  /**
   * Sets the executor service to use for parallel file reading. The default is not using executor
   * service.
   *
   * @param service executor service
   * @return this for method chaining
   */
  default SnapshotTable executeWith(ExecutorService service) {
    throw new UnsupportedOperationException("Setting executor service is not supported");
  }

  /** The action result that contains a summary of the execution. */
  interface Result {
    /** Returns the number of imported data files. */
    long importedDataFilesCount();
  }
}
