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
  private final SortKey[] keys;

  private transient Integer hashCode;

  AggregatedStatistics(
      long checkpointId, StatisticsType type, Map<SortKey, Long> keyFrequency, SortKey[] keys) {
    Preconditions.checkArgument(
        (keyFrequency != null && keys == null) || (keyFrequency == null && keys != null),
        "Invalid key frequency or range bounds: both are non-null or null");
    this.checkpointId = checkpointId;
    this.type = type;
    this.keyFrequency = keyFrequency;
    this.keys = keys;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("checkpointId", checkpointId)
        .add("type", type)
        .add("keyFrequency", keyFrequency)
        .add("keys", keys)
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
        && Arrays.equals(keys, other.keys());
  }

  @Override
  public int hashCode() {
    // implemented caching because coordinator can call the hashCode many times.
    // when subtasks request statistics refresh upon initialization for reconciliation purpose,
    // hashCode is used to check if there is any difference btw coordinator and operator state.
    if (hashCode == null) {
      this.hashCode = Objects.hashCode(checkpointId, type, keyFrequency, keys);
    }

    return hashCode;
  }

  StatisticsType type() {
    return type;
  }

  Map<SortKey, Long> keyFrequency() {
    return keyFrequency;
  }

  protected SortKey[] keys() {
    return keys;
  }

  long checkpointId() {
    return checkpointId;
  }
}
