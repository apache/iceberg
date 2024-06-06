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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * AggregatedStatistics is used by {@link DataStatisticsCoordinator} to collect {@link
 * DataStatistics} from {@link DataStatisticsOperator} subtasks for specific checkpoint. It stores
 * the merged {@link DataStatistics} result from all reported subtasks.
 */
class AggregatedStatistics implements Serializable {
  private final long checkpointId;
  private final StatisticsType type;
  private final Map<SortKey, Long> keyFrequency;
  private final SortKey[] keySamples;

  AggregatedStatistics(
      long checkpointId,
      StatisticsType type,
      Map<SortKey, Long> keyFrequency,
      SortKey[] keySamples) {
    Preconditions.checkArgument(
        (keyFrequency != null && keySamples == null)
            || (keyFrequency == null && keySamples != null),
        "Invalid key frequency or range bounds: both are non-null or null");
    this.checkpointId = checkpointId;
    this.type = type;
    this.keyFrequency = keyFrequency;
    this.keySamples = keySamples;
  }

  static AggregatedStatistics fromKeyFrequency(long checkpointId, Map<SortKey, Long> stats) {
    return new AggregatedStatistics(checkpointId, StatisticsType.Map, stats, null);
  }

  static AggregatedStatistics fromKeySamples(long checkpointId, SortKey[] stats) {
    return new AggregatedStatistics(checkpointId, StatisticsType.Sketch, null, stats);
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

    if (!(o instanceof AggregatedStatistics)) {
      return false;
    }

    AggregatedStatistics other = (AggregatedStatistics) o;
    return Objects.equal(checkpointId, other.checkpointId())
        && Objects.equal(type, other.type())
        && Objects.equal(keyFrequency, other.keyFrequency())
        && Arrays.equals(keySamples, other.keySamples());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(checkpointId, type, keyFrequency, keySamples);
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

  long checkpointId() {
    return checkpointId;
  }
}
