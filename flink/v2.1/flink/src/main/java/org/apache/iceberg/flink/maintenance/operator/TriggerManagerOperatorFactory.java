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
package org.apache.iceberg.flink.maintenance.operator;

import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.iceberg.flink.maintenance.api.Trigger;

@Internal
public class TriggerManagerOperatorFactory extends AbstractStreamOperatorFactory<Trigger>
    implements CoordinatedOperatorFactory<Trigger>,
        OneInputStreamOperatorFactory<TableChange, Trigger> {

  private final String lockId;
  private final List<String> maintenanceTaskNames;
  private final List<TriggerEvaluator> evaluators;
  private final long minFireDelayMs;
  private final long lockCheckDelayMs;

  public TriggerManagerOperatorFactory(
      String lockId,
      List<String> maintenanceTaskNames,
      List<TriggerEvaluator> evaluators,
      long minFireDelayMs,
      long lockCheckDelayMs) {
    this.lockId = lockId;
    this.maintenanceTaskNames = maintenanceTaskNames;
    this.evaluators = evaluators;
    this.minFireDelayMs = minFireDelayMs;
    this.lockCheckDelayMs = lockCheckDelayMs;
  }

  @Override
  public OperatorCoordinator.Provider getCoordinatorProvider(
      String operatorName, OperatorID operatorID) {
    return new TableMaintenanceCoordinatorProvider(operatorName, operatorID);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends StreamOperator<Trigger>> T createStreamOperator(
      StreamOperatorParameters<Trigger> parameters) {
    OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
    OperatorEventGateway gateway =
        parameters.getOperatorEventDispatcher().getOperatorEventGateway(operatorId);

    TriggerManagerOperator triggerManagerOperator =
        new TriggerManagerOperator(
            parameters,
            gateway,
            maintenanceTaskNames,
            evaluators,
            minFireDelayMs,
            lockCheckDelayMs,
            lockId);

    parameters
        .getOperatorEventDispatcher()
        .registerEventHandler(operatorId, triggerManagerOperator);

    return (T) triggerManagerOperator;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
    return TriggerManagerOperator.class;
  }
}
