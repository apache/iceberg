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
import java.util.Map;
import java.util.Objects;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.EnumSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

@Internal
class DataStatisticsSerializer extends TypeSerializer<DataStatistics> {
  private final TypeSerializer<SortKey> sortKeySerializer;
  private final EnumSerializer<StatisticsType> statisticsTypeSerializer;
  private final MapSerializer<SortKey, Long> mapSerializer;
  private final SortKeySketchSerializer sketchSerializer;

  DataStatisticsSerializer(TypeSerializer<SortKey> sortKeySerializer) {
    this.sortKeySerializer = sortKeySerializer;
    this.statisticsTypeSerializer = new EnumSerializer<>(StatisticsType.class);
    this.mapSerializer = new MapSerializer<>(sortKeySerializer, LongSerializer.INSTANCE);
    this.sketchSerializer = new SortKeySketchSerializer(sortKeySerializer);
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @SuppressWarnings("ReferenceEquality")
  @Override
  public TypeSerializer<DataStatistics> duplicate() {
    TypeSerializer<SortKey> duplicateSortKeySerializer = sortKeySerializer.duplicate();
    return (duplicateSortKeySerializer == sortKeySerializer)
        ? this
        : new DataStatisticsSerializer(duplicateSortKeySerializer);
  }

  @Override
  public DataStatistics createInstance() {
    return new MapDataStatistics();
  }

  @SuppressWarnings("unchecked")
  @Override
  public DataStatistics copy(DataStatistics obj) {
    StatisticsType statisticsType = obj.type();
    if (statisticsType == StatisticsType.Map) {
      MapDataStatistics from = (MapDataStatistics) obj;
      Map<SortKey, Long> fromStats = (Map<SortKey, Long>) from.result();
      Map<SortKey, Long> toStats = Maps.newHashMap(fromStats);
      return new MapDataStatistics(toStats);
    } else if (statisticsType == StatisticsType.Sketch) {
      // because ReservoirItemsSketch doesn't expose enough public methods for cloning,
      // this implementation adopted the less efficient serialization and deserialization.
      SketchDataStatistics from = (SketchDataStatistics) obj;
      ReservoirItemsSketch<SortKey> fromStats = (ReservoirItemsSketch<SortKey>) from.result();
      byte[] bytes = fromStats.toByteArray(sketchSerializer);
      Memory memory = Memory.wrap(bytes);
      ReservoirItemsSketch<SortKey> toStats =
          ReservoirItemsSketch.heapify(memory, sketchSerializer);
      return new SketchDataStatistics(toStats);
    } else {
      throw new IllegalArgumentException("Unsupported data statistics type: " + statisticsType);
    }
  }

  @Override
  public DataStatistics copy(DataStatistics from, DataStatistics reuse) {
    // not much benefit to reuse
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void serialize(DataStatistics obj, DataOutputView target) throws IOException {
    StatisticsType statisticsType = obj.type();
    statisticsTypeSerializer.serialize(obj.type(), target);
    if (statisticsType == StatisticsType.Map) {
      Map<SortKey, Long> mapStatistics = (Map<SortKey, Long>) obj.result();
      mapSerializer.serialize(mapStatistics, target);
    } else if (statisticsType == StatisticsType.Sketch) {
      ReservoirItemsSketch<SortKey> sketch = (ReservoirItemsSketch<SortKey>) obj.result();
      byte[] sketchBytes = sketch.toByteArray(sketchSerializer);
      target.writeInt(sketchBytes.length);
      target.write(sketchBytes);
    } else {
      throw new IllegalArgumentException("Unsupported data statistics type: " + statisticsType);
    }
  }

  @Override
  public DataStatistics deserialize(DataInputView source) throws IOException {
    StatisticsType statisticsType = statisticsTypeSerializer.deserialize(source);
    if (statisticsType == StatisticsType.Map) {
      Map<SortKey, Long> mapStatistics = mapSerializer.deserialize(source);
      return new MapDataStatistics(mapStatistics);
    } else if (statisticsType == StatisticsType.Sketch) {
      int numBytes = source.readInt();
      byte[] sketchBytes = new byte[numBytes];
      source.read(sketchBytes);
      Memory sketchMemory = Memory.wrap(sketchBytes);
      ReservoirItemsSketch<SortKey> sketch =
          ReservoirItemsSketch.heapify(sketchMemory, sketchSerializer);
      return new SketchDataStatistics(sketch);
    } else {
      throw new IllegalArgumentException("Unsupported data statistics type: " + statisticsType);
    }
  }

  @Override
  public DataStatistics deserialize(DataStatistics reuse, DataInputView source) throws IOException {
    // not much benefit to reuse
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    serialize(deserialize(source), target);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DataStatisticsSerializer)) {
      return false;
    }

    DataStatisticsSerializer other = (DataStatisticsSerializer) obj;
    return Objects.equals(sortKeySerializer, other.sortKeySerializer);
  }

  @Override
  public int hashCode() {
    return sortKeySerializer.hashCode();
  }

  @Override
  public TypeSerializerSnapshot<DataStatistics> snapshotConfiguration() {
    return new DataStatisticsSerializerSnapshot(this);
  }

  public static class DataStatisticsSerializerSnapshot
      extends CompositeTypeSerializerSnapshot<DataStatistics, DataStatisticsSerializer> {
    private static final int CURRENT_VERSION = 1;

    /** Constructor for read instantiation. */
    @SuppressWarnings({"unused", "checkstyle:RedundantModifier"})
    public DataStatisticsSerializerSnapshot() {}

    @SuppressWarnings("checkstyle:RedundantModifier")
    public DataStatisticsSerializerSnapshot(DataStatisticsSerializer serializer) {
      super(serializer);
    }

    @Override
    protected int getCurrentOuterSnapshotVersion() {
      return CURRENT_VERSION;
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(DataStatisticsSerializer outerSerializer) {
      return new TypeSerializer<?>[] {outerSerializer.sortKeySerializer};
    }

    @Override
    protected DataStatisticsSerializer createOuterSerializerWithNestedSerializers(
        TypeSerializer<?>[] nestedSerializers) {
      SortKeySerializer sortKeySerializer = (SortKeySerializer) nestedSerializers[0];
      return new DataStatisticsSerializer(sortKeySerializer);
    }
  }
}
