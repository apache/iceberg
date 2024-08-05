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

import java.util.Map;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.SortKey;

/**
 * DataStatistics defines the interface to collect data distribution information.
 *
 * <p>Data statistics tracks traffic volume distribution across data keys. For low-cardinality key,
 * a simple map of (key, count) can be used. For high-cardinality key, probabilistic data structures
 * (sketching) can be used.
 */
@Internal
interface DataStatistics {

  StatisticsType type();

  boolean isEmpty();

  /** Add row sortKey to data statistics. */
  void add(SortKey sortKey);

  /**
   * Get the collected statistics. Could be a {@link Map} (low cardinality) or {@link
   * ReservoirItemsSketch} (high cardinality)
   */
  Object result();
}
