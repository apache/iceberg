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
import org.apache.iceberg.SortKey;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/** MapDataStatistics uses map to count key frequency */
class MapDataStatistics implements DataStatistics {
  private final Map<SortKey, Long> keyFrequency;

  MapDataStatistics() {
    this.keyFrequency = Maps.newHashMap();
  }

  MapDataStatistics(Map<SortKey, Long> keyFrequency) {
    this.keyFrequency = keyFrequency;
  }

  @Override
  public StatisticsType type() {
    return StatisticsType.Map;
  }

  @Override
  public boolean isEmpty() {
    return keyFrequency.size() == 0;
  }

  @Override
  public void add(SortKey sortKey) {
    if (keyFrequency.containsKey(sortKey)) {
      keyFrequency.merge(sortKey, 1L, Long::sum);
    } else {
      // clone the sort key before adding to map because input sortKey object can be reused
      SortKey copiedKey = sortKey.copy();
      keyFrequency.put(copiedKey, 1L);
    }
  }

  @Override
  public Object result() {
    return keyFrequency;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("map", keyFrequency).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof MapDataStatistics)) {
      return false;
    }

    MapDataStatistics other = (MapDataStatistics) o;
    return Objects.equal(keyFrequency, other.keyFrequency);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(keyFrequency);
  }
}
