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
package org.apache.iceberg.tornadovm;

import java.util.List;
import java.util.function.Consumer;

/** A merge-on-read scan of every live row of a table, on the device or on the CPU. */
public interface MergeOnReadScan {

  /**
   * {@code SELECT groupColumn, COUNT(*), SUM(c1), ... GROUP BY groupColumn} over the live rows, or
   * one group keyed 0 when {@code groupColumn} is null. Sum columns are DOUBLE; a null is skipped
   * by its SUM and still counted by COUNT(*).
   */
  AggregateResult aggregate(String groupColumn, String... sumColumns);

  /**
   * {@code SELECT bucket(buckets, column), COUNT(*), SUM(c1), ... GROUP BY 1} over the live rows,
   * with Iceberg's {@code bucket[N]} partition transform of a required LONG column: how many live
   * rows, and how much of each sum, each bucket of a {@code bucket(N, column)} partitioning holds.
   */
  AggregateResult aggregateByBucket(String column, int buckets, String... sumColumns);

  /**
   * {@code SELECT columns} over the live rows, handed over a batch of data files at a time. The
   * rows are only valid during the call, and the consumer may be called from several threads.
   *
   * @return the number of live rows
   */
  long materialize(List<String> columns, Consumer<LiveRows> consumer);

  /** Timings of the last call. */
  ScanMetrics metrics();
}
