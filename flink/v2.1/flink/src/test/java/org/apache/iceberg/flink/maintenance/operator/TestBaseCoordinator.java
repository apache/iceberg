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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 10)
class TestBaseCoordinator extends OperatorTestBase {
  protected static final String OPERATOR_NAME = "TestCoordinator";
  protected static final String OPERATOR_NAME_1 = "TestCoordinator_1";
  protected static final OperatorID TEST_OPERATOR_ID = new OperatorID(1234L, 5678L);
  protected static final OperatorID TEST_OPERATOR_ID_1 = new OperatorID(1235L, 5679L);
  protected static final int NUM_SUBTASKS = 1;
  protected static final LockRegisterEvent LOCK_REGISTER_EVENT =
      new LockRegisterEvent(DUMMY_TABLE_NAME, 1L);
  protected static final LockRegisterAckEvent LOCK_REGISTER_ACK_EVENT =
      new LockRegisterAckEvent(DUMMY_TABLE_NAME, 1L);
  protected static final LockReleaseEvent LOCK_RELEASE_EVENT =
      new LockReleaseEvent(DUMMY_TABLE_NAME, 1L);

  protected static void setAllTasksReady(
      BaseCoordinator baseCoordinator, EventReceivingTasks receivingTasks) {
    for (int i = 0; i < NUM_SUBTASKS; i++) {
      baseCoordinator.executionAttemptReady(i, 0, receivingTasks.createGatewayForSubtask(i, 0));
    }
  }

  protected static void waitForCoordinatorToProcessActions(BaseCoordinator coordinator) {
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
