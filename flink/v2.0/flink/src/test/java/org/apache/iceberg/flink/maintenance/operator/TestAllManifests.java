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
import java.util.stream.Collectors;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.junit.jupiter.api.Test;

class TestAllManifests extends OperatorTestBase {

  @Test
  void testAllManifests() throws Exception {

    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");

    table.refresh();

    List<ManifestFile> actual;
    try (OneInputStreamOperatorTestHarness<Trigger, ManifestFile> testHarness = harness()) {
      testHarness.open();

      OperatorTestBase.trigger(testHarness);
      actual = testHarness.extractOutputValues();
    }

    assertThat(actual).hasSize(2);
    assertThat(actual)
        .isEqualTo(
            table.currentSnapshot().allManifests(table.io()).stream()
                .filter(m -> m.partitionSpecId() == table.spec().specId())
                .collect(Collectors.toList()));
  }

  private OneInputStreamOperatorTestHarness<Trigger, ManifestFile> harness() throws Exception {
    return ProcessFunctionTestHarnesses.forProcessFunction(
        new AllManifests(tableLoader(), OperatorTestBase.DUMMY_TABLE_NAME, 0));
  }
}
