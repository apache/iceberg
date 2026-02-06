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

import org.apache.iceberg.StatisticsFile;

/** An action that collects statistics of an Iceberg table and writes to Puffin files. */
public interface ComputeTableStats extends Action<ComputeTableStats, ComputeTableStats.Result> {
  /**
   * Choose the set of columns to collect stats, by default all columns are chosen.
   *
   * @param columns a set of column names to be analyzed
   * @return this for method chaining
   */
  ComputeTableStats columns(String... columns);

  /**
   * Choose the table snapshot to compute stats, by default the current snapshot is used.
   *
   * @param snapshotId long ID of the snapshot for which stats need to be computed
   * @return this for method chaining
   */
  ComputeTableStats snapshot(long snapshotId);

  /** The result of table statistics collection. */
  interface Result {

    /** Returns statistics file or none if no statistics were collected. */
    StatisticsFile statisticsFile();
  }
}
