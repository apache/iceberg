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
import java.util.Objects;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.RowData;

@Internal
class StatisticsOrRecordSerializer extends TypeSerializer<StatisticsOrRecord> {
  private final TypeSerializer<GlobalStatistics> statisticsSerializer;
  private final TypeSerializer<RowData> recordSerializer;

  StatisticsOrRecordSerializer(
      TypeSerializer<GlobalStatistics> statisticsSerializer,
      TypeSerializer<RowData> recordSerializer) {
    this.statisticsSerializer = statisticsSerializer;
    this.recordSerializer = recordSerializer;
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @SuppressWarnings("ReferenceEquality")
  @Override
  public TypeSerializer<StatisticsOrRecord> duplicate() {
    TypeSerializer<GlobalStatistics> duplicateStatisticsSerializer =
        statisticsSerializer.duplicate();
    TypeSerializer<RowData> duplicateRowDataSerializer = recordSerializer.duplicate();
    if ((statisticsSerializer != duplicateStatisticsSerializer)
        || (recordSerializer != duplicateRowDataSerializer)) {
      return new StatisticsOrRecordSerializer(
          duplicateStatisticsSerializer, duplicateRowDataSerializer);
    } else {
      return this;
    }
  }

  @Override
  public StatisticsOrRecord createInstance() {
    // arbitrarily always create RowData value instance
    return StatisticsOrRecord.fromRecord(recordSerializer.createInstance());
  }

  @Override
  public StatisticsOrRecord copy(StatisticsOrRecord from) {
    if (from.hasRecord()) {
      return StatisticsOrRecord.fromRecord(recordSerializer.copy(from.record()));
    } else {
      return StatisticsOrRecord.fromStatistics(statisticsSerializer.copy(from.statistics()));
    }
  }

  @Override
  public StatisticsOrRecord copy(StatisticsOrRecord from, StatisticsOrRecord reuse) {
    StatisticsOrRecord to;
    if (from.hasRecord()) {
      to = StatisticsOrRecord.reuseRecord(reuse, recordSerializer);
      RowData record = recordSerializer.copy(from.record(), to.record());
      to.record(record);
    } else {
      to = StatisticsOrRecord.reuseStatistics(reuse, statisticsSerializer);
      GlobalStatistics statistics = statisticsSerializer.copy(from.statistics(), to.statistics());
      to.statistics(statistics);
    }

    return to;
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(StatisticsOrRecord statisticsOrRecord, DataOutputView target)
      throws IOException {
    if (statisticsOrRecord.hasRecord()) {
      target.writeBoolean(true);
      recordSerializer.serialize(statisticsOrRecord.record(), target);
    } else {
      target.writeBoolean(false);
      statisticsSerializer.serialize(statisticsOrRecord.statistics(), target);
    }
  }

  @Override
  public StatisticsOrRecord deserialize(DataInputView source) throws IOException {
    boolean isRecord = source.readBoolean();
    if (isRecord) {
      return StatisticsOrRecord.fromRecord(recordSerializer.deserialize(source));
    } else {
      return StatisticsOrRecord.fromStatistics(statisticsSerializer.deserialize(source));
    }
  }

  @Override
  public StatisticsOrRecord deserialize(StatisticsOrRecord reuse, DataInputView source)
      throws IOException {
    StatisticsOrRecord to;
    boolean isRecord = source.readBoolean();
    if (isRecord) {
      to = StatisticsOrRecord.reuseRecord(reuse, recordSerializer);
      RowData record = recordSerializer.deserialize(to.record(), source);
      to.record(record);
    } else {
      to = StatisticsOrRecord.reuseStatistics(reuse, statisticsSerializer);
      GlobalStatistics statistics = statisticsSerializer.deserialize(to.statistics(), source);
      to.statistics(statistics);
    }

    return to;
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    boolean hasRecord = source.readBoolean();
    target.writeBoolean(hasRecord);
    if (hasRecord) {
      recordSerializer.copy(source, target);
    } else {
      statisticsSerializer.copy(source, target);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof StatisticsOrRecordSerializer)) {
      return false;
    }

    StatisticsOrRecordSerializer other = (StatisticsOrRecordSerializer) obj;
    return Objects.equals(statisticsSerializer, other.statisticsSerializer)
        && Objects.equals(recordSerializer, other.recordSerializer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statisticsSerializer, recordSerializer);
  }

  @Override
  public TypeSerializerSnapshot<StatisticsOrRecord> snapshotConfiguration() {
    return new StatisticsOrRecordSerializerSnapshot(this);
  }

  public static class StatisticsOrRecordSerializerSnapshot
      extends CompositeTypeSerializerSnapshot<StatisticsOrRecord, StatisticsOrRecordSerializer> {
    private static final int CURRENT_VERSION = 1;

    /** Constructor for read instantiation. */
    @SuppressWarnings({"unused", "checkstyle:RedundantModifier"})
    public StatisticsOrRecordSerializerSnapshot() {
      super(StatisticsOrRecordSerializer.class);
    }

    @SuppressWarnings("checkstyle:RedundantModifier")
    public StatisticsOrRecordSerializerSnapshot(StatisticsOrRecordSerializer serializer) {
      super(serializer);
    }

    @SuppressWarnings("checkstyle:RedundantModifier")
    @Override
    protected int getCurrentOuterSnapshotVersion() {
      return CURRENT_VERSION;
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(
        StatisticsOrRecordSerializer outerSerializer) {
      return new TypeSerializer<?>[] {
        outerSerializer.statisticsSerializer, outerSerializer.recordSerializer
      };
    }

    @SuppressWarnings("unchecked")
    @Override
    protected StatisticsOrRecordSerializer createOuterSerializerWithNestedSerializers(
        TypeSerializer<?>[] nestedSerializers) {
      TypeSerializer<GlobalStatistics> statisticsSerializer =
          (TypeSerializer<GlobalStatistics>) nestedSerializers[0];
      TypeSerializer<RowData> recordSerializer = (TypeSerializer<RowData>) nestedSerializers[1];
      return new StatisticsOrRecordSerializer(statisticsSerializer, recordSerializer);
    }
  }
}
