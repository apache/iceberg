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
package org.apache.iceberg.hudi;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.actions.Action;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

public interface SnapshotHudiTable extends Action<SnapshotHudiTable, SnapshotHudiTable.Result> {

  /**
   * Sets table properties in the newly created Iceberg table. Any properties with the same key name
   * will be overwritten.
   *
   * @param properties a map of properties to set
   * @return this for method chaining
   */
  SnapshotHudiTable tableProperties(Map<String, String> properties);

  /**
   * Sets a table property in the newly created Iceberg table. Any properties with the same key will
   * be overwritten.
   *
   * @param name a table property name
   * @param value a table property value
   * @return this for method chaining
   */
  SnapshotHudiTable tableProperty(String name, String value);

  /**
   * Sets the location of the newly created Iceberg table. Default location is the same as the Hudi
   * table.
   *
   * @param location a path to the new table location
   * @return this for method chaining
   */
  SnapshotHudiTable tableLocation(String location);

  /**
   * Sets the identifier of the newly created Iceberg table. This is required to be set before
   * execute the action.
   *
   * @param identifier a table identifier (namespace, name) @Returns this for method chaining
   */
  SnapshotHudiTable as(TableIdentifier identifier);

  /**
   * Sets the catalog of the newly created Iceberg table. This is required to be set before execute
   * the action
   *
   * @param catalog a catalog @Returns this for method chaining
   */
  SnapshotHudiTable icebergCatalog(Catalog catalog);

  /**
   * Sets the Hadoop configuration used to access hudi table's timeline and file groups. This is
   * required to be set before execute the action.
   *
   * @param conf a Hadoop configuration @Returns this for method chaining
   */
  SnapshotHudiTable hoodieConfiguration(Configuration conf);

  /** The action result that contains a summary of the execution. */
  interface Result {

    /** Returns the number of snapshot data files. */
    long snapshotFilesCount();
  }
}
