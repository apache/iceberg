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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

/**
 * DataStatisticsUtil is the utility to serialize and deserialize {@link DataStatistics} and {@link
 * AggregatedStatistics}
 */
class DataStatisticsUtil {

  private DataStatisticsUtil() {}

  static <D extends DataStatistics<D, S>, S> byte[] serializeDataStatistics(
      DataStatistics<D, S> dataStatistics,
      TypeSerializer<DataStatistics<D, S>> statisticsSerializer) {
    DataOutputSerializer out = new DataOutputSerializer(64);
    try {
      statisticsSerializer.serialize(dataStatistics, out);
      return out.getCopyOfBuffer();
    } catch (IOException e) {
      throw new IllegalStateException("Fail to serialize data statistics", e);
    }
  }

  @SuppressWarnings("unchecked")
  static <D extends DataStatistics<D, S>, S> D deserializeDataStatistics(
      byte[] bytes, TypeSerializer<DataStatistics<D, S>> statisticsSerializer) {
    DataInputDeserializer input = new DataInputDeserializer(bytes, 0, bytes.length);
    try {
      return (D) statisticsSerializer.deserialize(input);
    } catch (IOException e) {
      throw new IllegalStateException("Fail to deserialize data statistics", e);
    }
  }

  static <D extends DataStatistics<D, S>, S> byte[] serializeAggregatedStatistics(
      AggregatedStatistics<D, S> aggregatedStatistics,
      TypeSerializer<DataStatistics<D, S>> statisticsSerializer)
      throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bytes);

    DataOutputSerializer outSerializer = new DataOutputSerializer(64);
    out.writeLong(aggregatedStatistics.checkpointId());
    statisticsSerializer.serialize(aggregatedStatistics.dataStatistics(), outSerializer);
    byte[] statisticsBytes = outSerializer.getCopyOfBuffer();
    out.writeInt(statisticsBytes.length);
    out.write(statisticsBytes);
    out.flush();

    return bytes.toByteArray();
  }

  @SuppressWarnings("unchecked")
  static <D extends DataStatistics<D, S>, S>
      AggregatedStatistics<D, S> deserializeAggregatedStatistics(
          byte[] bytes, TypeSerializer<DataStatistics<D, S>> statisticsSerializer)
          throws IOException {
    ByteArrayInputStream bytesIn = new ByteArrayInputStream(bytes);
    ObjectInputStream in = new ObjectInputStream(bytesIn);

    long completedCheckpointId = in.readLong();
    int statisticsBytesLength = in.readInt();
    byte[] statisticsBytes = new byte[statisticsBytesLength];
    in.readFully(statisticsBytes);
    DataInputDeserializer input =
        new DataInputDeserializer(statisticsBytes, 0, statisticsBytesLength);
    DataStatistics<D, S> dataStatistics = statisticsSerializer.deserialize(input);

    return new AggregatedStatistics<>(completedCheckpointId, dataStatistics);
  }
}
