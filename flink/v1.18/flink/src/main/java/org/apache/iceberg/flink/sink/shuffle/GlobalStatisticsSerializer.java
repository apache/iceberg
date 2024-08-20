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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.EnumSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

class GlobalStatisticsSerializer extends TypeSerializer<GlobalStatistics> {
  private final TypeSerializer<SortKey> sortKeySerializer;
  private final EnumSerializer<StatisticsType> statisticsTypeSerializer;
  private final ListSerializer<SortKey> rangeBoundsSerializer;
  private final ListSerializer<Integer> intsSerializer;
  private final ListSerializer<Long> longsSerializer;

  GlobalStatisticsSerializer(TypeSerializer<SortKey> sortKeySerializer) {
    this.sortKeySerializer = sortKeySerializer;
    this.statisticsTypeSerializer = new EnumSerializer<>(StatisticsType.class);
    this.rangeBoundsSerializer = new ListSerializer<>(sortKeySerializer);
    this.intsSerializer = new ListSerializer<>(IntSerializer.INSTANCE);
    this.longsSerializer = new ListSerializer<>(LongSerializer.INSTANCE);
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<GlobalStatistics> duplicate() {
    return new GlobalStatisticsSerializer(sortKeySerializer);
  }

  @Override
  public GlobalStatistics createInstance() {
    return GlobalStatistics.fromRangeBounds(0L, new SortKey[0]);
  }

  @Override
  public GlobalStatistics copy(GlobalStatistics from) {
    return new GlobalStatistics(
        from.checkpointId(), from.type(), from.mapAssignment(), from.rangeBounds());
  }

  @Override
  public GlobalStatistics copy(GlobalStatistics from, GlobalStatistics reuse) {
    // no benefit of reuse
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(GlobalStatistics record, DataOutputView target) throws IOException {
    target.writeLong(record.checkpointId());
    statisticsTypeSerializer.serialize(record.type(), target);
    if (record.type() == StatisticsType.Map) {
      MapAssignment mapAssignment = record.mapAssignment();
      target.writeInt(mapAssignment.numPartitions());
      target.writeInt(mapAssignment.keyAssignments().size());
      for (Map.Entry<SortKey, KeyAssignment> entry : mapAssignment.keyAssignments().entrySet()) {
        sortKeySerializer.serialize(entry.getKey(), target);
        KeyAssignment keyAssignment = entry.getValue();
        intsSerializer.serialize(keyAssignment.assignedSubtasks(), target);
        longsSerializer.serialize(keyAssignment.subtaskWeightsWithCloseFileCost(), target);
        target.writeLong(keyAssignment.closeFileCostWeight());
      }
    } else {
      rangeBoundsSerializer.serialize(Arrays.asList(record.rangeBounds()), target);
    }
  }

  @Override
  public GlobalStatistics deserialize(DataInputView source) throws IOException {
    long checkpointId = source.readLong();
    StatisticsType type = statisticsTypeSerializer.deserialize(source);
    if (type == StatisticsType.Map) {
      int numPartitions = source.readInt();
      int mapSize = source.readInt();
      Map<SortKey, KeyAssignment> keyAssignments = Maps.newHashMapWithExpectedSize(mapSize);
      for (int i = 0; i < mapSize; ++i) {
        SortKey sortKey = sortKeySerializer.deserialize(source);
        List<Integer> assignedSubtasks = intsSerializer.deserialize(source);
        List<Long> subtaskWeightsWithCloseFileCost = longsSerializer.deserialize(source);
        long closeFileCostWeight = source.readLong();
        keyAssignments.put(
            sortKey,
            new KeyAssignment(
                assignedSubtasks, subtaskWeightsWithCloseFileCost, closeFileCostWeight));
      }

      return GlobalStatistics.fromMapAssignment(
          checkpointId, new MapAssignment(numPartitions, keyAssignments));
    } else {
      List<SortKey> sortKeys = rangeBoundsSerializer.deserialize(source);
      SortKey[] rangeBounds = new SortKey[sortKeys.size()];
      return GlobalStatistics.fromRangeBounds(checkpointId, sortKeys.toArray(rangeBounds));
    }
  }

  @Override
  public GlobalStatistics deserialize(GlobalStatistics reuse, DataInputView source)
      throws IOException {
    // not much benefit to reuse
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    serialize(deserialize(source), target);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    GlobalStatisticsSerializer other = (GlobalStatisticsSerializer) obj;
    return Objects.equals(sortKeySerializer, other.sortKeySerializer);
  }

  @Override
  public int hashCode() {
    return sortKeySerializer.hashCode();
  }

  @Override
  public TypeSerializerSnapshot<GlobalStatistics> snapshotConfiguration() {
    return new GlobalStatisticsSerializerSnapshot(this);
  }

  public static class GlobalStatisticsSerializerSnapshot
      extends CompositeTypeSerializerSnapshot<GlobalStatistics, GlobalStatisticsSerializer> {
    private static final int CURRENT_VERSION = 1;

    /** Constructor for read instantiation. */
    @SuppressWarnings({"unused", "checkstyle:RedundantModifier"})
    public GlobalStatisticsSerializerSnapshot() {
      super(GlobalStatisticsSerializer.class);
    }

    @SuppressWarnings("checkstyle:RedundantModifier")
    public GlobalStatisticsSerializerSnapshot(GlobalStatisticsSerializer serializer) {
      super(serializer);
    }

    @Override
    protected int getCurrentOuterSnapshotVersion() {
      return CURRENT_VERSION;
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(GlobalStatisticsSerializer outerSerializer) {
      return new TypeSerializer<?>[] {outerSerializer.sortKeySerializer};
    }

    @Override
    protected GlobalStatisticsSerializer createOuterSerializerWithNestedSerializers(
        TypeSerializer<?>[] nestedSerializers) {
      SortKeySerializer sortKeySerializer = (SortKeySerializer) nestedSerializers[0];
      return new GlobalStatisticsSerializer(sortKeySerializer);
    }
  }
}
