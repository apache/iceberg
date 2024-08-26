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
package org.apache.iceberg.flink.maintenance.stream;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.function.Supplier;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.operator.CollectingSink;
import org.apache.iceberg.flink.maintenance.operator.ManualSource;
import org.apache.iceberg.flink.maintenance.operator.OperatorTestBase;
import org.apache.iceberg.flink.maintenance.operator.TaskResult;
import org.apache.iceberg.flink.maintenance.operator.Trigger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.extension.RegisterExtension;

class ScheduledBuilderTestBase extends OperatorTestBase {
  private static final int TESTING_TASK_ID = 0;
  private static final Duration POLL_DURATION = Duration.ofSeconds(5);
  static final String DB_NAME = "db";

  @RegisterExtension ScheduledInfraExtension infra = new ScheduledInfraExtension();

  /**
   * Triggers a maintenance tasks and waits for the successful result. The {@link Table} is
   * refreshed for convenience reasons.
   *
   * @param env used for testing
   * @param triggerSource used for manually emitting the trigger
   * @param collectingSink used for collecting the result
   * @param table used for generating the payload
   * @throws Exception if any
   */
  void runAndWaitForSuccess(
      StreamExecutionEnvironment env,
      ManualSource<Trigger> triggerSource,
      CollectingSink<TaskResult> collectingSink,
      Supplier<Boolean> checkSideEffect,
      Table table)
      throws Exception {
    table.refresh();
    SerializableTable payload = (SerializableTable) SerializableTable.copyOf(table);

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      // Do a single task run
      long time = System.currentTimeMillis();
      triggerSource.sendRecord(Trigger.create(time, payload, TESTING_TASK_ID), time);

      TaskResult result = collectingSink.poll(POLL_DURATION);

      assertThat(result.startEpoch()).isEqualTo(time);
      assertThat(result.success()).isTrue();
      assertThat(result.taskIndex()).isEqualTo(TESTING_TASK_ID);

      Awaitility.await().until(() -> checkSideEffect.get());
    } finally {
      closeJobClient(jobClient);
    }

    table.refresh();
  }
}
