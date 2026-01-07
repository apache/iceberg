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

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;

/** API for configuring partition statistics scan. */
public interface PartitionStatisticsScan {

  /**
   * Create a new scan from this scan's configuration that will use the given snapshot by ID.
   *
   * @param snapshotId a snapshot ID
   * @return a new scan based on this with the given snapshot ID
   * @throws IllegalArgumentException if the snapshot cannot be found
   */
  PartitionStatisticsScan useSnapshot(long snapshotId);

  /**
   * Create a new scan from the results of this, where partitions are filtered by the {@link
   * Expression}.
   *
   * @param filter a filter expression
   * @return a new scan based on this with results filtered by the expression
   */
  PartitionStatisticsScan filter(Expression filter);

  /**
   * Create a new scan from this with the schema as its projection.
   *
   * @param schema a projection schema
   * @return a new scan based on this with the given projection
   */
  PartitionStatisticsScan project(Schema schema);

  /**
   * Scans a partition statistics file belonging to a particular snapshot
   *
   * @return an Iterable of partition statistics
   */
  CloseableIterable<PartitionStatistics> scan();
}
