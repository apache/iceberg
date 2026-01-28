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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 10)
class TestTableMaintenanceCoordinator extends OperatorTestBase {
  private static final String OPERATOR_NAME = "TestCoordinator";
  private static final String OPERATOR_NAME_1 = "TestCoordinator_1";
  private static final OperatorID TEST_OPERATOR_ID = new OperatorID(1234L, 5678L);
  private static final OperatorID TEST_OPERATOR_ID_1 = new OperatorID(1235L, 5679L);
  private static final int NUM_SUBTASKS = 1;
  private static final LockRegisterEvent LOCK_REGISTER_EVENT =
      new LockRegisterEvent(DUMMY_TABLE_NAME, 1L);
  private static final LockReleasedEvent LOCK_RELEASE_EVENT =
      new LockReleasedEvent(DUMMY_TABLE_NAME, 1L);

  private EventReceivingTasks receivingTasks;
  private EventReceivingTasks receivingTasks1;

  @BeforeEach
  public void before() {
    this.receivingTasks = EventReceivingTasks.createForRunningTasks();
    this.receivingTasks1 = EventReceivingTasks.createForRunningTasks();
  }

  private void tasksReady(TableMaintenanceCoordinator coordinator) {
    setAllTasksReady(coordinator, receivingTasks);
  }

  @Test
  public void testThrowExceptionWhenNotStarted() throws Exception {
    try (TableMaintenanceCoordinator tableMaintenanceCoordinator =
        createCoordinator(OPERATOR_NAME, TEST_OPERATOR_ID)) {
      String failureMessage = "The coordinator has not started yet.";
      assertThatThrownBy(
              () -> tableMaintenanceCoordinator.handleEventFromOperator(0, 0, LOCK_REGISTER_EVENT))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(failureMessage);
      assertThatThrownBy(() -> tableMaintenanceCoordinator.executionAttemptFailed(0, 0, null))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(failureMessage);
      assertThatThrownBy(() -> tableMaintenanceCoordinator.checkpointCoordinator(0, null))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(failureMessage);
    }
  }

  @Test
  public void testEventHandling() throws Exception {
    try (TableMaintenanceCoordinator tableMaintenanceCoordinator =
            createCoordinator(OPERATOR_NAME, TEST_OPERATOR_ID);
        TableMaintenanceCoordinator tableMaintenanceCoordinator1 =
            createCoordinator(OPERATOR_NAME_1, TEST_OPERATOR_ID_1)) {

      tableMaintenanceCoordinator.start();
      tableMaintenanceCoordinator1.start();

      tasksReady(tableMaintenanceCoordinator);
      tasksReady(tableMaintenanceCoordinator1);

      tableMaintenanceCoordinator.handleEventFromOperator(0, 0, LOCK_REGISTER_EVENT);
      waitForCoordinatorToProcessActions(tableMaintenanceCoordinator);
      assertThat(receivingTasks.getSentEventsForSubtask(0).size()).isEqualTo(0);

      // release lock from coordinator1 and get one event from coordinator
      tableMaintenanceCoordinator1.handleEventFromOperator(0, 0, LOCK_RELEASE_EVENT);
      waitForCoordinatorToProcessActions(tableMaintenanceCoordinator1);
      assertThat(receivingTasks.getSentEventsForSubtask(0).size()).isEqualTo(1);
      assertThat(receivingTasks1.getSentEventsForSubtask(0).size()).isEqualTo(0);
    }
  }

  private static TableMaintenanceCoordinator createCoordinator(
      String operatorName, OperatorID operatorID) {
    return new TableMaintenanceCoordinator(
        operatorName, new MockOperatorCoordinatorContext(operatorID, 1));
  }

  private static void setAllTasksReady(
      TableMaintenanceCoordinator tableMaintenanceCoordinator, EventReceivingTasks receivingTasks) {
    for (int i = 0; i < NUM_SUBTASKS; i++) {
      tableMaintenanceCoordinator.executionAttemptReady(
          i, 0, receivingTasks.createGatewayForSubtask(i, 0));
    }
  }

  private static void waitForCoordinatorToProcessActions(TableMaintenanceCoordinator coordinator) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    coordinator.callInCoordinatorThread(
        () -> {
          future.complete(null);
          return null;
        },
        "Coordinator fails to process action");

    try {
      future.get();
    } catch (InterruptedException e) {
      throw new AssertionError("test interrupted");
    } catch (ExecutionException e) {
      ExceptionUtils.rethrow(ExceptionUtils.stripExecutionException(e));
    }
  }
}
