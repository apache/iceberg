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
class DataStatisticsOrRecordSerializer<D extends DataStatistics<D, S>, S>
    extends TypeSerializer<DataStatisticsOrRecord<D, S>> {
  private final TypeSerializer<DataStatistics<D, S>> statisticsSerializer;
  private final TypeSerializer<RowData> recordSerializer;

  DataStatisticsOrRecordSerializer(
      TypeSerializer<DataStatistics<D, S>> statisticsSerializer,
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
  public TypeSerializer<DataStatisticsOrRecord<D, S>> duplicate() {
    TypeSerializer<DataStatistics<D, S>> duplicateStatisticsSerializer =
        statisticsSerializer.duplicate();
    TypeSerializer<RowData> duplicateRowDataSerializer = recordSerializer.duplicate();
    if ((statisticsSerializer != duplicateStatisticsSerializer)
        || (recordSerializer != duplicateRowDataSerializer)) {
      return new DataStatisticsOrRecordSerializer<>(
          duplicateStatisticsSerializer, duplicateRowDataSerializer);
    } else {
      return this;
    }
  }

  @Override
  public DataStatisticsOrRecord<D, S> createInstance() {
    // arbitrarily always create RowData value instance
    return DataStatisticsOrRecord.fromRecord(recordSerializer.createInstance());
  }

  @Override
  public DataStatisticsOrRecord<D, S> copy(DataStatisticsOrRecord<D, S> from) {
    if (from.hasRecord()) {
      return DataStatisticsOrRecord.fromRecord(recordSerializer.copy(from.record()));
    } else {
      return DataStatisticsOrRecord.fromDataStatistics(
          statisticsSerializer.copy(from.dataStatistics()));
    }
  }

  @Override
  public DataStatisticsOrRecord<D, S> copy(
      DataStatisticsOrRecord<D, S> from, DataStatisticsOrRecord<D, S> reuse) {
    DataStatisticsOrRecord<D, S> to;
    if (from.hasRecord()) {
      to = DataStatisticsOrRecord.reuseRecord(reuse, recordSerializer);
      RowData record = recordSerializer.copy(from.record(), to.record());
      to.record(record);
    } else {
      to = DataStatisticsOrRecord.reuseStatistics(reuse, statisticsSerializer);
      DataStatistics<D, S> statistics =
          statisticsSerializer.copy(from.dataStatistics(), to.dataStatistics());
      to.dataStatistics(statistics);
    }

    return to;
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(DataStatisticsOrRecord<D, S> statisticsOrRecord, DataOutputView target)
      throws IOException {
    if (statisticsOrRecord.hasRecord()) {
      target.writeBoolean(true);
      recordSerializer.serialize(statisticsOrRecord.record(), target);
    } else {
      target.writeBoolean(false);
      statisticsSerializer.serialize(statisticsOrRecord.dataStatistics(), target);
    }
  }

  @Override
  public DataStatisticsOrRecord<D, S> deserialize(DataInputView source) throws IOException {
    boolean isRecord = source.readBoolean();
    if (isRecord) {
      return DataStatisticsOrRecord.fromRecord(recordSerializer.deserialize(source));
    } else {
      return DataStatisticsOrRecord.fromDataStatistics(statisticsSerializer.deserialize(source));
    }
  }

  @Override
  public DataStatisticsOrRecord<D, S> deserialize(
      DataStatisticsOrRecord<D, S> reuse, DataInputView source) throws IOException {
    DataStatisticsOrRecord<D, S> to;
    boolean isRecord = source.readBoolean();
    if (isRecord) {
      to = DataStatisticsOrRecord.reuseRecord(reuse, recordSerializer);
      RowData record = recordSerializer.deserialize(to.record(), source);
      to.record(record);
    } else {
      to = DataStatisticsOrRecord.reuseStatistics(reuse, statisticsSerializer);
      DataStatistics<D, S> statistics =
          statisticsSerializer.deserialize(to.dataStatistics(), source);
      to.dataStatistics(statistics);
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
    if (!(obj instanceof DataStatisticsOrRecordSerializer)) {
      return false;
    }

    @SuppressWarnings("unchecked")
    DataStatisticsOrRecordSerializer<D, S> other = (DataStatisticsOrRecordSerializer<D, S>) obj;
    return Objects.equals(statisticsSerializer, other.statisticsSerializer)
        && Objects.equals(recordSerializer, other.recordSerializer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statisticsSerializer, recordSerializer);
  }

  @Override
  public TypeSerializerSnapshot<DataStatisticsOrRecord<D, S>> snapshotConfiguration() {
    return new DataStatisticsOrRecordSerializerSnapshot<>(this);
  }

  public static class DataStatisticsOrRecordSerializerSnapshot<D extends DataStatistics<D, S>, S>
      extends CompositeTypeSerializerSnapshot<
          DataStatisticsOrRecord<D, S>, DataStatisticsOrRecordSerializer<D, S>> {
    private static final int CURRENT_VERSION = 1;

    // constructors need to public. Otherwise, Flink state restore would complain
    // "The class has no (implicit) public nullary constructor".
    @SuppressWarnings("checkstyle:RedundantModifier")
    public DataStatisticsOrRecordSerializerSnapshot() {
      super(DataStatisticsOrRecordSerializer.class);
    }

    @SuppressWarnings("checkstyle:RedundantModifier")
    public DataStatisticsOrRecordSerializerSnapshot(
        DataStatisticsOrRecordSerializer<D, S> serializer) {
      super(serializer);
    }

    @SuppressWarnings("checkstyle:RedundantModifier")
    @Override
    protected int getCurrentOuterSnapshotVersion() {
      return CURRENT_VERSION;
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(
        DataStatisticsOrRecordSerializer<D, S> outerSerializer) {
      return new TypeSerializer<?>[] {
        outerSerializer.statisticsSerializer, outerSerializer.recordSerializer
      };
    }

    @SuppressWarnings("unchecked")
    @Override
    protected DataStatisticsOrRecordSerializer<D, S> createOuterSerializerWithNestedSerializers(
        TypeSerializer<?>[] nestedSerializers) {
      TypeSerializer<DataStatistics<D, S>> statisticsSerializer =
          (TypeSerializer<DataStatistics<D, S>>) nestedSerializers[0];
      TypeSerializer<RowData> recordSerializer = (TypeSerializer<RowData>) nestedSerializers[1];
      return new DataStatisticsOrRecordSerializer<>(statisticsSerializer, recordSerializer);
    }
  }
}
