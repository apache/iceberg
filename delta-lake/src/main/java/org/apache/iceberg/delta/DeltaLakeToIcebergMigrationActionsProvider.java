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

/**
 * An API that provide actions for migration from a delta lake table to an iceberg table. Query
 * engines can use {@code defaultActions()} to access default action implementations, or implement
 * this provider to supply a different implementation if necessary.
 */
public interface DeltaLakeToIcebergMigrationActionsProvider {

  /**
   * Initiates an action to snapshot an existing Delta Lake table to an Iceberg table.
   *
   * @param sourceTableLocation the location of the Delta Lake table
   * @return a {@link SnapshotDeltaLakeTable} action
   */
  default SnapshotDeltaLakeTable snapshotDeltaLakeTable(String sourceTableLocation) {
    return new BaseSnapshotDeltaLakeTableAction(sourceTableLocation);
  }

  /**
   * Get the default implementation of {@link DeltaLakeToIcebergMigrationActionsProvider}
   *
   * @return an instance with access to all default actions
   */
  static DeltaLakeToIcebergMigrationActionsProvider defaultActions() {
    return DefaultDeltaLakeToIcebergMigrationActions.defaultMigrationActions();
  }

  class DefaultDeltaLakeToIcebergMigrationActions
      implements DeltaLakeToIcebergMigrationActionsProvider {
    private static final DefaultDeltaLakeToIcebergMigrationActions defaultMigrationActions =
        new DefaultDeltaLakeToIcebergMigrationActions();

    private DefaultDeltaLakeToIcebergMigrationActions() {}

    static DefaultDeltaLakeToIcebergMigrationActions defaultMigrationActions() {
      return defaultMigrationActions;
    }
  }
}
