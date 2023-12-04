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
package org.apache.iceberg.flink.sink.shuffle;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;

/**
 * DataStatistics defines the interface to collect data distribution information.
 *
 * <p>Data statistics tracks traffic volume distribution across data keys. For low-cardinality key,
 * a simple map of (key, count) can be used. For high-cardinality key, probabilistic data structures
 * (sketching) can be used.
 */
@Internal
interface DataStatistics<D extends DataStatistics, S> {

  /**
   * Check if data statistics contains any statistics information.
   *
   * @return true if data statistics doesn't contain any statistics information
   */
  boolean isEmpty();

  /**
   * Add data key to data statistics.
   *
   * @param key generate from data by applying key selector
   */
  void add(RowData key);

  /**
   * Merge current statistics with other statistics.
   *
   * @param otherStatistics the statistics to be merged
   */
  void merge(D otherStatistics);

  /**
   * Get the underline statistics.
   *
   * @return the underline statistics
   */
  S statistics();
}
