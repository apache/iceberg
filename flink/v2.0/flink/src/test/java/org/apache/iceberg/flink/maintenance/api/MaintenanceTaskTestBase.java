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
package org.apache.iceberg.flink.maintenance.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.function.Supplier;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.maintenance.operator.CollectingSink;
import org.apache.iceberg.flink.maintenance.operator.ManualSource;
import org.apache.iceberg.flink.maintenance.operator.OperatorTestBase;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.extension.RegisterExtension;

class MaintenanceTaskTestBase extends OperatorTestBase {
  private static final int TESTING_TASK_ID = 0;
  private static final Duration POLL_DURATION = Duration.ofSeconds(5);

  @RegisterExtension MaintenanceTaskInfraExtension infra = new MaintenanceTaskInfraExtension();

  void runAndWaitForSuccess(
      StreamExecutionEnvironment env,
      ManualSource<Trigger> triggerSource,
      CollectingSink<TaskResult> collectingSink,
      Supplier<Boolean> waitForCondition)
      throws Exception {
    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      // Do a single task run
      long time = System.currentTimeMillis();
      triggerSource.sendRecord(Trigger.create(time, TESTING_TASK_ID), time);

      TaskResult result = collectingSink.poll(POLL_DURATION);

      assertThat(result.startEpoch()).isEqualTo(time);
      assertThat(result.success()).isTrue();
      assertThat(result.taskIndex()).isEqualTo(TESTING_TASK_ID);

      Awaitility.await().until(waitForCondition::get);
    } finally {
      closeJobClient(jobClient);
    }
  }
}
