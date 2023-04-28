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
package org.apache.iceberg.delta;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.actions.Action;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.immutables.value.Value;

/** Snapshot an existing Delta Lake table to Iceberg in place. */
@Value.Enclosing
public interface SnapshotDeltaLakeTable
    extends Action<SnapshotDeltaLakeTable, SnapshotDeltaLakeTable.Result> {

  /**
   * Sets table properties in the newly created Iceberg table. Any properties with the same key name
   * will be overwritten.
   *
   * @param properties a map of properties to set
   * @return this for method chaining
   */
  SnapshotDeltaLakeTable tableProperties(Map<String, String> properties);

  /**
   * Sets a table property in the newly created Iceberg table. Any properties with the same key will
   * be overwritten.
   *
   * @param name a table property name
   * @param value a table property value
   * @return this for method chaining
   */
  SnapshotDeltaLakeTable tableProperty(String name, String value);

  /**
   * Sets the location of the newly created Iceberg table. Default location is the same as the Delta
   * Lake table.
   *
   * @param location a path to the new table location
   * @return this for method chaining
   */
  SnapshotDeltaLakeTable tableLocation(String location);

  /**
   * Sets the identifier of the newly created Iceberg table. This is required to be set before
   * execute the action.
   *
   * @param identifier a table identifier (namespace, name) @Returns this for method chaining
   */
  SnapshotDeltaLakeTable as(TableIdentifier identifier);

  /**
   * Sets the catalog of the newly created Iceberg table. This is required to be set before execute
   * the action
   *
   * @param catalog a catalog @Returns this for method chaining
   */
  SnapshotDeltaLakeTable icebergCatalog(Catalog catalog);

  /**
   * Sets the Hadoop configuration used to access delta lake table's logs and datafiles. This is
   * required to be set before execute the action.
   *
   * @param conf a Hadoop configuration @Returns this for method chaining
   */
  SnapshotDeltaLakeTable deltaLakeConfiguration(Configuration conf);

  /** The action result that contains a summary of the execution. */
  @Value.Immutable
  interface Result {

    /** Returns the number of migrated data files. */
    long snapshotDataFilesCount();
  }
}
