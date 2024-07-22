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
import java.io.UncheckedIOException;
import javax.annotation.Nullable;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

class StatisticsUtil {

  private StatisticsUtil() {}

  static DataStatistics createTaskStatistics(
      StatisticsType type, int operatorParallelism, int numPartitions) {
    if (type == StatisticsType.Map) {
      return new MapDataStatistics();
    } else {
      return new SketchDataStatistics(
          SketchUtil.determineOperatorReservoirSize(operatorParallelism, numPartitions));
    }
  }

  static byte[] serializeDataStatistics(
      DataStatistics dataStatistics, TypeSerializer<DataStatistics> statisticsSerializer) {
    DataOutputSerializer out = new DataOutputSerializer(64);
    try {
      statisticsSerializer.serialize(dataStatistics, out);
      return out.getCopyOfBuffer();
    } catch (IOException e) {
      throw new UncheckedIOException("Fail to serialize data statistics", e);
    }
  }

  static DataStatistics deserializeDataStatistics(
      byte[] bytes, TypeSerializer<DataStatistics> statisticsSerializer) {
    DataInputDeserializer input = new DataInputDeserializer(bytes, 0, bytes.length);
    try {
      return statisticsSerializer.deserialize(input);
    } catch (IOException e) {
      throw new UncheckedIOException("Fail to deserialize data statistics", e);
    }
  }

  static byte[] serializeCompletedStatistics(
      CompletedStatistics completedStatistics,
      TypeSerializer<CompletedStatistics> statisticsSerializer) {
    try {
      DataOutputSerializer out = new DataOutputSerializer(1024);
      statisticsSerializer.serialize(completedStatistics, out);
      return out.getCopyOfBuffer();
    } catch (IOException e) {
      throw new UncheckedIOException("Fail to serialize aggregated statistics", e);
    }
  }

  static CompletedStatistics deserializeCompletedStatistics(
      byte[] bytes, TypeSerializer<CompletedStatistics> statisticsSerializer) {
    try {
      DataInputDeserializer input = new DataInputDeserializer(bytes);
      return statisticsSerializer.deserialize(input);
    } catch (IOException e) {
      throw new UncheckedIOException("Fail to deserialize aggregated statistics", e);
    }
  }

  static byte[] serializeGlobalStatistics(
      GlobalStatistics globalStatistics, TypeSerializer<GlobalStatistics> statisticsSerializer) {
    try {
      DataOutputSerializer out = new DataOutputSerializer(1024);
      statisticsSerializer.serialize(globalStatistics, out);
      return out.getCopyOfBuffer();
    } catch (IOException e) {
      throw new UncheckedIOException("Fail to serialize aggregated statistics", e);
    }
  }

  static GlobalStatistics deserializeGlobalStatistics(
      byte[] bytes, TypeSerializer<GlobalStatistics> statisticsSerializer) {
    try {
      DataInputDeserializer input = new DataInputDeserializer(bytes);
      return statisticsSerializer.deserialize(input);
    } catch (IOException e) {
      throw new UncheckedIOException("Fail to deserialize aggregated statistics", e);
    }
  }

  static StatisticsType collectType(StatisticsType config) {
    return config == StatisticsType.Sketch ? StatisticsType.Sketch : StatisticsType.Map;
  }

  static StatisticsType collectType(StatisticsType config, @Nullable GlobalStatistics statistics) {
    if (statistics != null) {
      return statistics.type();
    }

    return collectType(config);
  }

  static StatisticsType collectType(
      StatisticsType config, @Nullable CompletedStatistics statistics) {
    if (statistics != null) {
      return statistics.type();
    }

    return collectType(config);
  }
}
