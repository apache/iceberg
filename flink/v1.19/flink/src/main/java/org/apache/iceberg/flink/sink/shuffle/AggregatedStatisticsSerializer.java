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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.EnumSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.iceberg.SortKey;

abstract class AggregatedStatisticsSerializer<T extends AggregatedStatistics>
    extends TypeSerializer<T> {
  private final TypeSerializer<SortKey> sortKeySerializer;
  private final EnumSerializer<StatisticsType> statisticsTypeSerializer;
  private final MapSerializer<SortKey, Long> keyFrequencySerializer;
  private final ListSerializer<SortKey> keySamplesSerializer;

  AggregatedStatisticsSerializer(TypeSerializer<SortKey> sortKeySerializer) {
    this.sortKeySerializer = sortKeySerializer;
    this.statisticsTypeSerializer = new EnumSerializer<>(StatisticsType.class);
    this.keyFrequencySerializer = new MapSerializer<>(sortKeySerializer, LongSerializer.INSTANCE);
    this.keySamplesSerializer = new ListSerializer<>(sortKeySerializer);
  }

  protected TypeSerializer<SortKey> sortKeySerializer() {
    return sortKeySerializer;
  }

  protected abstract T createInstance(
      long checkpointId,
      StatisticsType type,
      Map<SortKey, Long> keyFrequency,
      SortKey[] rangeBounds);

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public T createInstance() {
    return createInstance(0, StatisticsType.Map, Collections.emptyMap(), null);
  }

  @Override
  public T copy(T from) {
    return createInstance(from.checkpointId(), from.type(), from.keyFrequency(), from.keys());
  }

  @Override
  public T copy(T from, T reuse) {
    // no benefit of reuse
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(T record, DataOutputView target) throws IOException {
    target.writeLong(record.checkpointId());
    statisticsTypeSerializer.serialize(record.type(), target);
    if (record.type() == StatisticsType.Map) {
      keyFrequencySerializer.serialize(record.keyFrequency(), target);
    } else {
      keySamplesSerializer.serialize(Arrays.asList(record.keys()), target);
    }
  }

  @Override
  public T deserialize(DataInputView source) throws IOException {
    long checkpointId = source.readLong();
    StatisticsType type = statisticsTypeSerializer.deserialize(source);
    Map<SortKey, Long> keyFrequency = null;
    SortKey[] rangeBounds = null;
    if (type == StatisticsType.Map) {
      keyFrequency = keyFrequencySerializer.deserialize(source);
    } else {
      List<SortKey> sortKeys = keySamplesSerializer.deserialize(source);
      rangeBounds = new SortKey[sortKeys.size()];
      rangeBounds = sortKeys.toArray(rangeBounds);
    }

    return createInstance(checkpointId, type, keyFrequency, rangeBounds);
  }

  @Override
  public T deserialize(T reuse, DataInputView source) throws IOException {
    // not much benefit to reuse
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    serialize(deserialize(source), target);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof AggregatedStatisticsSerializer)) {
      return false;
    }

    @SuppressWarnings("unchecked")
    AggregatedStatisticsSerializer<T> other = (AggregatedStatisticsSerializer<T>) obj;
    return Objects.equals(sortKeySerializer, other.sortKeySerializer);
  }

  @Override
  public int hashCode() {
    return sortKeySerializer.hashCode();
  }
}
