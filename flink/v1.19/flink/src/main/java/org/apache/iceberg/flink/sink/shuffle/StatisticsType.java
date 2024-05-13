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

/**
 * Range distribution requires gathering statistics on the sort keys to determine proper range
 * boundaries to distribute/cluster rows before writer operators.
 */
public enum StatisticsType {
  /**
   * Tracks the data statistics as {@code Map<SortKey, Long>} frequency. It works better for
   * low-cardinality scenarios (like country, event_type, etc.) where the cardinalities are in
   * hundreds or thousands.
   *
   * <ul>
   *   <li>Pro: accurate measurement on the statistics/weight of every key.
   *   <li>Con: memory footprint can be large if the key cardinality is high.
   * </ul>
   */
  Map,

  /**
   * Sample the sort keys via reservoir sampling. Then split the range partitions via range bounds
   * from sampled values. It works better for high-cardinality scenarios (like device_id, user_id,
   * uuid etc.) where the cardinalities can be in millions or billions.
   *
   * <ul>
   *   <li>Pro: relatively low memory footprint for high-cardinality sort keys.
   *   <li>Con: memory footprint can be large if the key cardinality is high.
   * </ul>
   */
  Sketch,

  /**
   * Initially use Map for statistics tracking. If key cardinality turns out to be high,
   * automatically switch to sketch sampling.
   */
  Auto
}
