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

import java.util.Arrays;
import java.util.List;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
class TestListFileSystemFiles extends OperatorTestBase {
  @Parameter(index = 0)
  private boolean usePrefixListing;

  @Parameter(index = 1)
  private int maxListingDepth;

  @Parameter(index = 2)
  private int maxListingDirectSubDirs;

  @Parameters(name = "usePrefixListing={0},maxListingDepth={1},maxListingDirectSubDirs={2}")
  public static List<Object[]> parameters() {
    return Arrays.asList(
        new Object[] {true, Integer.MAX_VALUE, Integer.MAX_VALUE},
        new Object[] {false, Integer.MAX_VALUE, Integer.MAX_VALUE},
        new Object[] {true, 1, 5},
        new Object[] {false, 1, 5},
        new Object[] {true, 2, 5},
        new Object[] {false, 2, 5});
  }

  @TestTemplate
  void testMetadataFilesWithTable() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");
    try (OneInputStreamOperatorTestHarness<Trigger, String> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(
            new ListFileSystemFiles(
                OperatorTestBase.DUMMY_TABLE_NAME,
                0,
                tableLoader(),
                table.location(),
                0,
                usePrefixListing,
                maxListingDepth,
                maxListingDirectSubDirs))) {
      testHarness.open();
      OperatorTestBase.trigger(testHarness);

      if (usePrefixListing) {
        assertThat(testHarness.extractOutputValues()).hasSize(11);
        assertThat(testHarness.getSideOutput(DeleteOrphanFiles.DIR_TASK_STREAM)).isNull();
        assertThat(testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM)).isNull();
      } else {
        if (maxListingDepth > 1 && maxListingDirectSubDirs > 1) {
          assertThat(testHarness.extractOutputValues()).hasSize(11);
          assertThat(testHarness.getSideOutput(DeleteOrphanFiles.DIR_TASK_STREAM)).isNull();
          assertThat(testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM)).isNull();
        } else {
          assertThat(testHarness.extractOutputValues()).isEmpty();
          assertThat(testHarness.getSideOutput(DeleteOrphanFiles.DIR_TASK_STREAM)).hasSize(1);
          assertThat(testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM)).isNull();
        }
      }
    }
  }

  @TestTemplate
  void testMetadataFilesWithPartitionTable() throws Exception {
    Table table = createPartitionedTable();
    insertPartitioned(table, 1, "p1");
    insertPartitioned(table, 2, "p1");
    insertPartitioned(table, 3, "p2");
    insertPartitioned(table, 4, "p2");
    try (OneInputStreamOperatorTestHarness<Trigger, String> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(
            new ListFileSystemFiles(
                OperatorTestBase.DUMMY_TABLE_NAME,
                0,
                tableLoader(),
                table.location(),
                0,
                usePrefixListing,
                maxListingDepth,
                maxListingDirectSubDirs))) {
      testHarness.open();
      OperatorTestBase.trigger(testHarness);

      if (usePrefixListing) {
        assertThat(testHarness.extractOutputValues()).hasSize(14);
        assertThat(testHarness.getSideOutput(DeleteOrphanFiles.DIR_TASK_STREAM)).isNull();
        assertThat(testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM)).isNull();
      } else {
        if (maxListingDepth > 1 && maxListingDirectSubDirs > 1) {
          assertThat(testHarness.extractOutputValues()).hasSize(14);
          assertThat(testHarness.getSideOutput(DeleteOrphanFiles.DIR_TASK_STREAM)).isNull();
          assertThat(testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM)).isNull();
        } else {
          assertThat(testHarness.extractOutputValues()).isEmpty();
          assertThat(testHarness.getSideOutput(DeleteOrphanFiles.DIR_TASK_STREAM)).hasSize(1);
          assertThat(testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM)).isNull();
        }
      }
    }
  }

  @TestTemplate
  void testMetadataFilesWithEmptyTable() throws Exception {
    Table table = createTable();
    try (OneInputStreamOperatorTestHarness<Trigger, String> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(
            new ListFileSystemFiles(
                OperatorTestBase.DUMMY_TABLE_NAME,
                0,
                tableLoader(),
                table.location(),
                0,
                usePrefixListing,
                maxListingDepth,
                maxListingDirectSubDirs))) {
      testHarness.open();
      OperatorTestBase.trigger(testHarness);

      if (usePrefixListing) {
        assertThat(testHarness.extractOutputValues()).hasSize(2);
        assertThat(testHarness.getSideOutput(DeleteOrphanFiles.DIR_TASK_STREAM)).isNull();
        assertThat(testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM)).isNull();
      } else {
        if (maxListingDepth > 1 && maxListingDirectSubDirs > 1) {
          assertThat(testHarness.extractOutputValues()).hasSize(2);
          assertThat(testHarness.getSideOutput(DeleteOrphanFiles.DIR_TASK_STREAM)).isNull();
          assertThat(testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM)).isNull();
        } else {
          assertThat(testHarness.extractOutputValues()).isEmpty();
          assertThat(testHarness.getSideOutput(DeleteOrphanFiles.DIR_TASK_STREAM)).hasSize(1);
          assertThat(testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM)).isNull();
        }
      }
    }
  }
}
