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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Timeout(value = 10)
@Execution(ExecutionMode.SAME_THREAD)
class TestTriggerManagerCoordinator extends TestBaseCoordinator {

  private EventReceivingTasks receivingTasks;
  private EventReceivingTasks receivingTasks1;

  @BeforeEach
  public void before() {
    this.receivingTasks = EventReceivingTasks.createForRunningTasks();
    this.receivingTasks1 = EventReceivingTasks.createForRunningTasks();
  }

  @Test
  public void testEventHandling() throws Exception {
    try (TriggerManagerCoordinator triggerManagerCoordinator =
            createCoordinator(OPERATOR_NAME, TEST_OPERATOR_ID);
        TriggerManagerCoordinator triggerManagerCoordinator1 =
            createCoordinator(OPERATOR_NAME_1, TEST_OPERATOR_ID_1)) {

      triggerManagerCoordinator.start();
      triggerManagerCoordinator1.start();

      setAllTasksReady(triggerManagerCoordinator, receivingTasks);
      setAllTasksReady(triggerManagerCoordinator1, receivingTasks1);

      triggerManagerCoordinator.handleEventFromOperator(0, 0, LOCK_REGISTER_EVENT);
      waitForCoordinatorToProcessActions(triggerManagerCoordinator);
      assertThat(receivingTasks.getSentEventsForSubtask(0).size()).isEqualTo(1);
      assertThat(receivingTasks.getSentEventsForSubtask(0).get(0))
          .isEqualTo(LOCK_REGISTER_ACK_EVENT);

      // release lock from coordinator1 and get one event from coordinator
      triggerManagerCoordinator1.handleReleaseLock(LOCK_RELEASE_EVENT);
      waitForCoordinatorToProcessActions(triggerManagerCoordinator1);
      assertThat(receivingTasks.getSentEventsForSubtask(0).size()).isEqualTo(2);
      assertThat(receivingTasks1.getSentEventsForSubtask(0).size()).isEqualTo(0);
    }
  }

  @Test
  public void testEventArriveBeforeRegister() throws Exception {
    try (TriggerManagerCoordinator triggerManagerCoordinator =
        createCoordinator(OPERATOR_NAME, TEST_OPERATOR_ID)) {

      triggerManagerCoordinator.start();

      setAllTasksReady(triggerManagerCoordinator, receivingTasks);

      // release event arrive before register
      triggerManagerCoordinator.handleReleaseLock(LOCK_RELEASE_EVENT);
      assertThat(triggerManagerCoordinator.pendingReleaseEvents()).hasSize(1);

      triggerManagerCoordinator.handleEventFromOperator(0, 0, LOCK_REGISTER_EVENT);
      waitForCoordinatorToProcessActions(triggerManagerCoordinator);
      assertThat(receivingTasks.getSentEventsForSubtask(0).size()).isEqualTo(2);

      assertThat(triggerManagerCoordinator.pendingReleaseEvents()).hasSize(0);
    }
  }

  protected static TriggerManagerCoordinator createCoordinator(
      String operatorName, OperatorID operatorID) {
    return new TriggerManagerCoordinator(
        operatorName, new MockOperatorCoordinatorContext(operatorID, 1));
  }
}
