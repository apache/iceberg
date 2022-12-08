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

import java.io.Serializable;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Shuffle operator can help to improve data clustering based on the key.
 *
 * <p>It collects the data statistics information, sends to coordinator and gets the global data
 * distribution weight from coordinator. Then it will ingest the weight into data stream(wrap by a
 * class{@link ShuffleRecordWrapper}) and send to partitioner.
 */
@Internal
public class ShuffleOperator<T, K extends Serializable>
    extends AbstractStreamOperator<ShuffleRecordWrapper<T, K>>
    implements OneInputStreamOperator<T, ShuffleRecordWrapper<T, K>>, OperatorEventHandler {

  private static final long serialVersionUID = 1L;

  private final KeySelector<T, K> keySelector;
  // the type of the key to collect data statistics
  private final TypeInformation<K> keyType;
  private final OperatorEventGateway operatorEventGateway;
  // key is generated by applying KeySelector to record
  // value is the times key occurs
  private transient Map<K, Long> localDataStatisticsMap;
  private transient Map<K, Long> globalDataDistributionWeightMap;
  private transient ListState<Map<K, Long>> globalDataDistributionWeightState;

  public ShuffleOperator(
      KeySelector<T, K> keySelector,
      TypeInformation<K> keyType,
      OperatorEventGateway operatorEventGateway) {
    this.keySelector = keySelector;
    this.keyType = keyType;
    this.operatorEventGateway = operatorEventGateway;
  }

  @VisibleForTesting
  ListStateDescriptor<Map<K, Long>> generateGlobalDataDistributionWeightDescriptor() {
    return new ListStateDescriptor<>(
        "globalDataDistributionWeight", new MapTypeInfo<>(keyType, TypeInformation.of(Long.class)));
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    localDataStatisticsMap = Maps.newHashMap();
    globalDataDistributionWeightState =
        context
            .getOperatorStateStore()
            .getListState(generateGlobalDataDistributionWeightDescriptor());

    if (globalDataDistributionWeightState.get() != null
        && globalDataDistributionWeightState.get().iterator().hasNext()) {
      globalDataDistributionWeightMap = globalDataDistributionWeightState.get().iterator().next();
    } else {
      globalDataDistributionWeightMap = Maps.newHashMap();
    }
  }

  @Override
  public void open() throws Exception {
    // TODO: handle scaling up
    if (globalDataDistributionWeightMap != null && globalDataDistributionWeightMap.size() > 0) {
      output.collect(
          new StreamRecord<>(
              ShuffleRecordWrapper.fromDistribution(globalDataDistributionWeightMap)));
    }
  }

  @Override
  public void handleOperatorEvent(OperatorEvent evt) {
    // TODO: receive event with globalDataDistributionWeight from coordinator and update
    // globalDataDistributionWeightMap
  }

  @Override
  public void processElement(StreamRecord<T> streamRecord) throws Exception {
    final K key = keySelector.getKey(streamRecord.getValue());
    localDataStatisticsMap.put(key, localDataStatisticsMap.getOrDefault(key, 0L) + 1);
    output.collect(new StreamRecord<>(ShuffleRecordWrapper.fromRecord(streamRecord.getValue())));
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    long checkpointId = context.getCheckpointId();
    LOG.debug("Taking shuffle operator snapshot for checkpoint {}", checkpointId);

    // Set the globalDataDistributionWeightState state to the latest global value.
    if (globalDataDistributionWeightMap != null && globalDataDistributionWeightMap.size() > 0) {
      globalDataDistributionWeightState.clear();
      globalDataDistributionWeightState.add(globalDataDistributionWeightMap);
    }

    // TODO: send to coordinator
    // For now we make it simple to send globalDataDistributionWeightState at checkpoint

    // Reset the local data count
    localDataStatisticsMap.clear();
  }

  @VisibleForTesting
  Map<K, Long> localDataStatisticsMap() {
    return localDataStatisticsMap;
  }
}
