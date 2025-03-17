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
import org.apache.iceberg.SortKey;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * This is used by {@link RangePartitioner} for guiding range partitioning. This is what is sent to
 * the operator subtasks. For sketch statistics, it only contains much smaller range bounds than the
 * complete raw samples.
 */
class GlobalStatistics {
  private final long checkpointId;
  private final StatisticsType type;
  private final MapAssignment mapAssignment;
  private final SortKey[] rangeBounds;

  private transient Integer hashCode;

  GlobalStatistics(
      long checkpointId, StatisticsType type, MapAssignment mapAssignment, SortKey[] rangeBounds) {
    Preconditions.checkArgument(
        (mapAssignment != null && rangeBounds == null)
            || (mapAssignment == null && rangeBounds != null),
        "Invalid key assignment or range bounds: both are non-null or null");
    this.checkpointId = checkpointId;
    this.type = type;
    this.mapAssignment = mapAssignment;
    this.rangeBounds = rangeBounds;
  }

  static GlobalStatistics fromMapAssignment(long checkpointId, MapAssignment mapAssignment) {
    return new GlobalStatistics(checkpointId, StatisticsType.Map, mapAssignment, null);
  }

  static GlobalStatistics fromRangeBounds(long checkpointId, SortKey[] rangeBounds) {
    return new GlobalStatistics(checkpointId, StatisticsType.Sketch, null, rangeBounds);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("checkpointId", checkpointId)
        .add("type", type)
        .add("mapAssignment", mapAssignment)
        .add("rangeBounds", rangeBounds)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof GlobalStatistics)) {
      return false;
    }

    GlobalStatistics other = (GlobalStatistics) o;
    return Objects.equal(checkpointId, other.checkpointId)
        && Objects.equal(type, other.type)
        && Objects.equal(mapAssignment, other.mapAssignment())
        && Arrays.equals(rangeBounds, other.rangeBounds());
  }

  @Override
  public int hashCode() {
    // implemented caching because coordinator can call the hashCode many times.
    // when subtasks request statistics refresh upon initialization for reconciliation purpose,
    // hashCode is used to check if there is any difference btw coordinator and operator state.
    if (hashCode == null) {
      this.hashCode = Objects.hashCode(checkpointId, type, mapAssignment, rangeBounds);
    }

    return hashCode;
  }

  long checkpointId() {
    return checkpointId;
  }

  StatisticsType type() {
    return type;
  }

  MapAssignment mapAssignment() {
    return mapAssignment;
  }

  SortKey[] rangeBounds() {
    return rangeBounds;
  }
}
