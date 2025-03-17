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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;

@Internal
public class DataStatisticsOperatorFactory extends AbstractStreamOperatorFactory<StatisticsOrRecord>
    implements CoordinatedOperatorFactory<StatisticsOrRecord>,
        OneInputStreamOperatorFactory<RowData, StatisticsOrRecord> {

  private final Schema schema;
  private final SortOrder sortOrder;
  private final int downstreamParallelism;
  private final StatisticsType type;
  private final double closeFileCostWeightPercentage;

  public DataStatisticsOperatorFactory(
      Schema schema,
      SortOrder sortOrder,
      int downstreamParallelism,
      StatisticsType type,
      double closeFileCostWeightPercentage) {
    this.schema = schema;
    this.sortOrder = sortOrder;
    this.downstreamParallelism = downstreamParallelism;
    this.type = type;
    this.closeFileCostWeightPercentage = closeFileCostWeightPercentage;
  }

  @Override
  public OperatorCoordinator.Provider getCoordinatorProvider(
      String operatorName, OperatorID operatorID) {
    return new DataStatisticsCoordinatorProvider(
        operatorName,
        operatorID,
        schema,
        sortOrder,
        downstreamParallelism,
        type,
        closeFileCostWeightPercentage);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends StreamOperator<StatisticsOrRecord>> T createStreamOperator(
      StreamOperatorParameters<StatisticsOrRecord> parameters) {
    OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
    String operatorName = parameters.getStreamConfig().getOperatorName();
    OperatorEventGateway gateway =
        parameters.getOperatorEventDispatcher().getOperatorEventGateway(operatorId);

    DataStatisticsOperator rangeStatisticsOperator =
        new DataStatisticsOperator(
            operatorName, schema, sortOrder, gateway, downstreamParallelism, type);

    rangeStatisticsOperator.setup(
        parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
    parameters
        .getOperatorEventDispatcher()
        .registerEventHandler(operatorId, rangeStatisticsOperator);

    return (T) rangeStatisticsOperator;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
    return DataStatisticsOperator.class;
  }
}
