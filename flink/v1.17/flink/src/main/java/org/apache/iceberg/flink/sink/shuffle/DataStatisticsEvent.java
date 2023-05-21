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
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * DataStatisticsEvent is sent between data statistics coordinator and operator to transmit data
 * statistics
 */
@Internal
class DataStatisticsEvent<D extends DataStatistics<D, S>, S> implements OperatorEvent {

  private static final long serialVersionUID = 1L;
  private final long checkpointId;
  private final byte[] dataStatisticsBytes;

  DataStatisticsEvent(
      long checkpointId,
      DataStatistics<D, S> dataStatistics,
      TypeSerializer<DataStatistics<D, S>> statisticsSerializer) {
    this.checkpointId = checkpointId;
    DataOutputSerializer out = new DataOutputSerializer(64);
    try {
      statisticsSerializer.serialize(dataStatistics, out);
      this.dataStatisticsBytes = out.getCopyOfBuffer();
    } catch (IOException e) {
      throw new IllegalStateException("Fail to serialize data statistics", e);
    }
  }

  long checkpointId() {
    return checkpointId;
  }

  @SuppressWarnings("unchecked")
  D dataStatistics(TypeSerializer<DataStatistics<D, S>> statisticsSerializer) {
    DataInputDeserializer input =
        new DataInputDeserializer(dataStatisticsBytes, 0, dataStatisticsBytes.length);
    D dataStatistics;

    try {
      dataStatistics = (D) statisticsSerializer.deserialize(input);
    } catch (IOException e) {
      throw new IllegalStateException("Fail to serialize data statistics", e);
    }

    return dataStatistics;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("checkpointId", checkpointId)
        .add("dataStatisticsBytes", dataStatisticsBytes)
        .toString();
  }
}
