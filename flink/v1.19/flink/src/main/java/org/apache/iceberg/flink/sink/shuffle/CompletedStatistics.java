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

import java.util.Arrays;
import java.util.Map;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;

/**
 * This is what {@link AggregatedStatisticsTracker} returns upon a completed statistics aggregation
 * from all subtasks. It contains the raw statistics (Map or reservoir samples).
 */
class CompletedStatistics {
  private final long checkpointId;
  private final StatisticsType type;
  private final Map<SortKey, Long> keyFrequency;
  private final SortKey[] keySamples;

  static CompletedStatistics fromKeyFrequency(long checkpointId, Map<SortKey, Long> stats) {
    return new CompletedStatistics(checkpointId, StatisticsType.Map, stats, null);
  }

  static CompletedStatistics fromKeySamples(long checkpointId, SortKey[] keySamples) {
    return new CompletedStatistics(checkpointId, StatisticsType.Sketch, null, keySamples);
  }

  CompletedStatistics(
      long checkpointId,
      StatisticsType type,
      Map<SortKey, Long> keyFrequency,
      SortKey[] keySamples) {
    this.checkpointId = checkpointId;
    this.type = type;
    this.keyFrequency = keyFrequency;
    this.keySamples = keySamples;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("checkpointId", checkpointId)
        .add("type", type)
        .add("keyFrequency", keyFrequency)
        .add("keySamples", keySamples)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof CompletedStatistics)) {
      return false;
    }

    CompletedStatistics other = (CompletedStatistics) o;
    return Objects.equal(checkpointId, other.checkpointId)
        && Objects.equal(type, other.type)
        && Objects.equal(keyFrequency, other.keyFrequency())
        && Arrays.equals(keySamples, other.keySamples());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(checkpointId, type, keyFrequency, keySamples);
  }

  long checkpointId() {
    return checkpointId;
  }

  StatisticsType type() {
    return type;
  }

  Map<SortKey, Long> keyFrequency() {
    return keyFrequency;
  }

  SortKey[] keySamples() {
    return keySamples;
  }
}
