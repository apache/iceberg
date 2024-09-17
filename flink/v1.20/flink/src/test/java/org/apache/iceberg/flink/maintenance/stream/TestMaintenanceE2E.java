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
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.OperatorTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestMaintenanceE2E extends OperatorTestBase {
  private StreamExecutionEnvironment env;

  @BeforeEach
  public void beforeEach() {
    env = StreamExecutionEnvironment.getExecutionEnvironment();
  }

  @Test
  void testE2e() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    TableMaintenance.forTable(env, tableLoader, LOCK_FACTORY)
        .uidSuffix("E2eTestUID")
        .rateLimit(Duration.ofMinutes(10))
        .lockCheckDelay(Duration.ofSeconds(10))
        .add(
            ExpireSnapshots.builder()
                .scheduleOnCommitCount(10)
                .maxSnapshotAge(Duration.ofMinutes(10))
                .retainLast(5)
                .deleteWorkerPoolSize(5)
                .parallelism(8))
        .append();

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      // Just make sure that we are able to instantiate the flow
      assertThat(jobClient).isNotNull();
    } finally {
      closeJobClient(jobClient);
    }
  }
}
