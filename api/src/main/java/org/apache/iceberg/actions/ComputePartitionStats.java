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

import org.apache.iceberg.PartitionStatisticsFile;

/**
 * An action that computes and writes the partition statistics of an Iceberg table. Current snapshot
 * is used by default.
 */
public interface ComputePartitionStats
    extends Action<ComputePartitionStats, ComputePartitionStats.Result> {
  /**
   * Choose the table snapshot to compute partition stats.
   *
   * @param snapshotId long ID of the snapshot for which stats need to be computed
   * @return this for method chaining
   */
  ComputePartitionStats snapshot(long snapshotId);

  /** The result of partition statistics collection. */
  interface Result {

    /** Returns statistics file or null if no statistics were collected. */
    PartitionStatisticsFile statisticsFile();
  }
}
