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
package org.apache.iceberg.delta.actions;

/**
 * An API that should be implemented by query engine integrations that want to support migration
 * from Delta Lake table to Iceberg table.
 */
public interface SupportMigrateDeltaLake {

  /** Initiates an action to migrate an existing Delta Lake table to Iceberg. */
  default MigrateDeltaLakeTable migrateDeltaLakeTable(String tableIdent, String deltaS3Location) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement migrateDeltaLakeTable");
  }
}
