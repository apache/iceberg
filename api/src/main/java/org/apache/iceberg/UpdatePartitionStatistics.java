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
package org.apache.iceberg;

import java.util.List;

/** API for updating partition statistics files in a table. */
public interface UpdatePartitionStatistics extends PendingUpdate<List<PartitionStatisticsFile>> {
  /**
   * Set the table's partition statistics file for given snapshot, replacing the previous partition
   * statistics file for the snapshot if any exists.
   *
   * @return this for method chaining
   */
  UpdatePartitionStatistics setPartitionStatistics(PartitionStatisticsFile partitionStatisticsFile);

  /**
   * Remove the table's partition statistics file for given snapshot.
   *
   * @return this for method chaining
   */
  UpdatePartitionStatistics removePartitionStatistics(long snapshotId);
}
