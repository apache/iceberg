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

import java.util.List;
import java.util.Queue;
import java.util.Set;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.api.TaskResult;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestExpireSnapshotsProcessor extends OperatorTestBase {
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testExpire(boolean success) throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");
    SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(table);

    List<TaskResult> actual;
    Queue<StreamRecord<String>> deletes;
    try (OneInputStreamOperatorTestHarness<Trigger, TaskResult> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(
            new ExpireSnapshotsProcessor(tableLoader(), 0L, 1, 10))) {
      testHarness.open();

      if (!success) {
        // Cause an exception
        dropTable();
      }

      testHarness.processElement(
          Trigger.create(10, serializableTable, 11), System.currentTimeMillis());
      deletes = testHarness.getSideOutput(ExpireSnapshotsProcessor.DELETE_STREAM);
      actual = testHarness.extractOutputValues();
    }

    assertThat(actual).hasSize(1);
    TaskResult result = actual.get(0);
    assertThat(result.startEpoch()).isEqualTo(10);
    assertThat(result.taskIndex()).isEqualTo(11);
    assertThat(result.success()).isEqualTo(success);

    if (success) {
      assertThat(result.exceptions()).isNotNull().isEmpty();

      table.refresh();
      Set<Snapshot> snapshots = Sets.newHashSet(table.snapshots());
      assertThat(snapshots).hasSize(1);
      assertThat(deletes).hasSize(1);
    } else {
      assertThat(result.exceptions()).isNotNull().hasSize(1);
      assertThat(deletes).isNull();
    }
  }
}
