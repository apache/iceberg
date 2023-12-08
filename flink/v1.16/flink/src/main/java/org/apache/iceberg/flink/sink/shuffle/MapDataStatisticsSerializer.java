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
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

@Internal
class MapDataStatisticsSerializer
    extends TypeSerializer<DataStatistics<MapDataStatistics, Map<SortKey, Long>>> {
  private final MapSerializer<SortKey, Long> mapSerializer;

  static MapDataStatisticsSerializer fromSortKeySerializer(
      TypeSerializer<SortKey> sortKeySerializer) {
    return new MapDataStatisticsSerializer(
        new MapSerializer<>(sortKeySerializer, LongSerializer.INSTANCE));
  }

  MapDataStatisticsSerializer(MapSerializer<SortKey, Long> mapSerializer) {
    this.mapSerializer = mapSerializer;
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @SuppressWarnings("ReferenceEquality")
  @Override
  public TypeSerializer<DataStatistics<MapDataStatistics, Map<SortKey, Long>>> duplicate() {
    MapSerializer<SortKey, Long> duplicateMapSerializer =
        (MapSerializer<SortKey, Long>) mapSerializer.duplicate();
    return (duplicateMapSerializer == mapSerializer)
        ? this
        : new MapDataStatisticsSerializer(duplicateMapSerializer);
  }

  @Override
  public MapDataStatistics createInstance() {
    return new MapDataStatistics();
  }

  @Override
  public MapDataStatistics copy(DataStatistics<MapDataStatistics, Map<SortKey, Long>> obj) {
    Preconditions.checkArgument(
        obj instanceof MapDataStatistics, "Invalid data statistics type: " + obj.getClass());
    MapDataStatistics from = (MapDataStatistics) obj;
    TypeSerializer<SortKey> keySerializer = mapSerializer.getKeySerializer();
    Map<SortKey, Long> newMap = Maps.newHashMapWithExpectedSize(from.statistics().size());
    for (Map.Entry<SortKey, Long> entry : from.statistics().entrySet()) {
      SortKey newKey = keySerializer.copy(entry.getKey());
      // no need to copy value since it is just a Long
      newMap.put(newKey, entry.getValue());
    }

    return new MapDataStatistics(newMap);
  }

  @Override
  public DataStatistics<MapDataStatistics, Map<SortKey, Long>> copy(
      DataStatistics<MapDataStatistics, Map<SortKey, Long>> from,
      DataStatistics<MapDataStatistics, Map<SortKey, Long>> reuse) {
    // not much benefit to reuse
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(
      DataStatistics<MapDataStatistics, Map<SortKey, Long>> obj, DataOutputView target)
      throws IOException {
    Preconditions.checkArgument(
        obj instanceof MapDataStatistics, "Invalid data statistics type: " + obj.getClass());
    MapDataStatistics mapStatistics = (MapDataStatistics) obj;
    mapSerializer.serialize(mapStatistics.statistics(), target);
  }

  @Override
  public DataStatistics<MapDataStatistics, Map<SortKey, Long>> deserialize(DataInputView source)
      throws IOException {
    return new MapDataStatistics(mapSerializer.deserialize(source));
  }

  @Override
  public DataStatistics<MapDataStatistics, Map<SortKey, Long>> deserialize(
      DataStatistics<MapDataStatistics, Map<SortKey, Long>> reuse, DataInputView source)
      throws IOException {
    // not much benefit to reuse
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    mapSerializer.copy(source, target);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof MapDataStatisticsSerializer)) {
      return false;
    }

    MapDataStatisticsSerializer other = (MapDataStatisticsSerializer) obj;
    return Objects.equals(mapSerializer, other.mapSerializer);
  }

  @Override
  public int hashCode() {
    return mapSerializer.hashCode();
  }

  @Override
  public TypeSerializerSnapshot<DataStatistics<MapDataStatistics, Map<SortKey, Long>>>
      snapshotConfiguration() {
    return new MapDataStatisticsSerializerSnapshot(this);
  }

  public static class MapDataStatisticsSerializerSnapshot
      extends CompositeTypeSerializerSnapshot<
          DataStatistics<MapDataStatistics, Map<SortKey, Long>>, MapDataStatisticsSerializer> {
    private static final int CURRENT_VERSION = 1;

    // constructors need to public. Otherwise, Flink state restore would complain
    // "The class has no (implicit) public nullary constructor".
    @SuppressWarnings("checkstyle:RedundantModifier")
    public MapDataStatisticsSerializerSnapshot() {
      super(MapDataStatisticsSerializer.class);
    }

    @SuppressWarnings("checkstyle:RedundantModifier")
    public MapDataStatisticsSerializerSnapshot(MapDataStatisticsSerializer serializer) {
      super(serializer);
    }

    @Override
    protected int getCurrentOuterSnapshotVersion() {
      return CURRENT_VERSION;
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(
        MapDataStatisticsSerializer outerSerializer) {
      return new TypeSerializer<?>[] {outerSerializer.mapSerializer};
    }

    @Override
    protected MapDataStatisticsSerializer createOuterSerializerWithNestedSerializers(
        TypeSerializer<?>[] nestedSerializers) {
      @SuppressWarnings("unchecked")
      MapSerializer<SortKey, Long> mapSerializer =
          (MapSerializer<SortKey, Long>) nestedSerializers[0];
      return new MapDataStatisticsSerializer(mapSerializer);
    }
  }
}
