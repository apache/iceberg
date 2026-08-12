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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.exceptions.NoSuchPlanIdException;
import org.apache.iceberg.exceptions.NoSuchPlanTaskException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestInMemoryPlanningState {

  private final InMemoryPlanningState state = InMemoryPlanningState.getInstance();

  @AfterEach
  public void cleanup() {
    state.clear();
  }

  @Test
  public void releasePlanTaskRemovesFetchedState() {
    FileScanTask task = Mockito.mock(FileScanTask.class);
    String planTaskKey = "plan-1-table-1-0";
    state.addPlanTask(planTaskKey, ImmutableList.of(task));
    state.addNextPlanTask(planTaskKey, "plan-1-table-1-1");

    // State is readable before release
    assertThat(state.fileScanTasksForPlanTask(planTaskKey)).containsExactly(task);
    assertThat(state.nextPlanTask(planTaskKey)).containsExactly("plan-1-table-1-1");

    state.releasePlanTask(planTaskKey);

    // Both the file scan tasks and the next-task link are gone after release
    assertThatThrownBy(() -> state.fileScanTasksForPlanTask(planTaskKey))
        .isInstanceOf(NoSuchPlanTaskException.class);
    assertThat(state.nextPlanTask(planTaskKey)).isEmpty();
  }

  @Test
  public void releaseAsyncPlanForTaskRemovesAsyncState() {
    // planId is "async-plan-1"; the key format is {planId}-{tableId}-{sequence}
    String planId = "async-plan-1";
    String planTaskKey = planId + "-table-1-0";
    state.addAsyncPlan(planId);
    assertThat(state.asyncPlanStatus(planId)).isEqualTo(PlanStatus.SUBMITTED);

    state.releaseAsyncPlanForTask(planTaskKey);

    assertThatThrownBy(() -> state.asyncPlanStatus(planId))
        .isInstanceOf(NoSuchPlanIdException.class);
  }

  @Test
  public void releaseAsyncPlanForTaskIgnoresMalformedKeys() {
    // Keys without two hyphens must not throw and must not remove unrelated state
    String planId = "some-plan";
    state.addAsyncPlan(planId);

    state.releaseAsyncPlanForTask("no-hyphens");
    state.releaseAsyncPlanForTask("only-one-hyphen");

    // Unrelated plan state is untouched
    assertThat(state.asyncPlanStatus(planId)).isEqualTo(PlanStatus.SUBMITTED);
  }

  @Test
  public void releasePlanTaskIsIdempotentForUnknownKeys() {
    // Releasing a key that was never added must be a no-op, not an error
    state.releasePlanTask("plan-x-table-y-0");
    assertThat(state.nextPlanTask("plan-x-table-y-0")).isEmpty();
  }
}
