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
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;

/**
 * DataStatisticsCoordinatorProvider provides the method to create new {@link
 * DataStatisticsCoordinator}
 */
@Internal
public class DataStatisticsCoordinatorProvider extends RecreateOnResetOperatorCoordinator.Provider {

  private final String operatorName;
  private final Schema schema;
  private final SortOrder sortOrder;
  private final int downstreamParallelism;
  private final StatisticsType type;
  private final double closeFileCostWeightPercentage;

  public DataStatisticsCoordinatorProvider(
      String operatorName,
      OperatorID operatorID,
      Schema schema,
      SortOrder sortOrder,
      int downstreamParallelism,
      StatisticsType type,
      double closeFileCostWeightPercentage) {
    super(operatorID);
    this.operatorName = operatorName;
    this.schema = schema;
    this.sortOrder = sortOrder;
    this.downstreamParallelism = downstreamParallelism;
    this.type = type;
    this.closeFileCostWeightPercentage = closeFileCostWeightPercentage;
  }

  @Override
  public OperatorCoordinator getCoordinator(OperatorCoordinator.Context context) {
    return new DataStatisticsCoordinator(
        operatorName,
        context,
        schema,
        sortOrder,
        downstreamParallelism,
        type,
        closeFileCostWeightPercentage);
  }
}
