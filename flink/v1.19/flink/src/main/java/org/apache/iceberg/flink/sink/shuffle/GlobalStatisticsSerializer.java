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
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.iceberg.SortKey;

class GlobalStatisticsSerializer extends AggregatedStatisticsSerializer<GlobalStatistics> {
  GlobalStatisticsSerializer(TypeSerializer<SortKey> sortKeySerializer) {
    super(sortKeySerializer);
  }

  @Override
  public GlobalStatistics createInstance(
      long checkpointId,
      StatisticsType type,
      Map<SortKey, Long> keyFrequency,
      SortKey[] rangeBounds) {
    return new GlobalStatistics(checkpointId, type, keyFrequency, rangeBounds);
  }

  @Override
  public TypeSerializer<GlobalStatistics> duplicate() {
    return new GlobalStatisticsSerializer(super.sortKeySerializer());
  }

  @Override
  public TypeSerializerSnapshot<GlobalStatistics> snapshotConfiguration() {
    return new CompletedStatisticsSerializerSnapshot(this);
  }

  public static class CompletedStatisticsSerializerSnapshot
      extends CompositeTypeSerializerSnapshot<GlobalStatistics, GlobalStatisticsSerializer> {
    private static final int CURRENT_VERSION = 1;

    /** Constructor for read instantiation. */
    @SuppressWarnings({"unused", "checkstyle:RedundantModifier"})
    public CompletedStatisticsSerializerSnapshot() {}

    @SuppressWarnings("checkstyle:RedundantModifier")
    public CompletedStatisticsSerializerSnapshot(GlobalStatisticsSerializer serializer) {
      super(serializer);
    }

    @Override
    protected int getCurrentOuterSnapshotVersion() {
      return CURRENT_VERSION;
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(GlobalStatisticsSerializer outerSerializer) {
      return new TypeSerializer<?>[] {outerSerializer.sortKeySerializer()};
    }

    @Override
    protected GlobalStatisticsSerializer createOuterSerializerWithNestedSerializers(
        TypeSerializer<?>[] nestedSerializers) {
      SortKeySerializer sortKeySerializer = (SortKeySerializer) nestedSerializers[0];
      return new GlobalStatisticsSerializer(sortKeySerializer);
    }
  }
}
