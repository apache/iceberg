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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestBatchScans extends TestBase {

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  @TestTemplate
  public void testDataTableScan() {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    if (formatVersion > 1) {
      table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    }

    BatchScan scan = table.newBatchScan();

    List<ScanTask> tasks = planTasks(scan);
    assertThat(tasks).hasSize(2);

    FileScanTask t1 = tasks.get(0).asFileScanTask();
    assertThat(FILE_A.path()).as("Task file must match").isEqualTo(t1.file().path());
    V1Assert.assertEquals("Task deletes size must match", 0, t1.deletes().size());
    V2Assert.assertEquals("Task deletes size must match", 1, t1.deletes().size());

    FileScanTask t2 = tasks.get(1).asFileScanTask();
    assertThat(FILE_B.path()).as("Task file must match").isEqualTo(t2.file().path());
    assertThat(t2.deletes()).as("Task deletes size must match").hasSize(0);

    List<ScanTaskGroup<ScanTask>> taskGroups = planTaskGroups(scan);
    assertThat(taskGroups).as("Expected 1 task group").hasSize(1);

    ScanTaskGroup<ScanTask> tg = taskGroups.get(0);
    assertThat(tg.tasks()).as("Task number must match").hasSize(2);
    V1Assert.assertEquals("Files count must match", 2, tg.filesCount());
    V2Assert.assertEquals("Files count must match", 3, tg.filesCount());
  }

  @TestTemplate
  public void testFilesTableScan() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_B).commit();

    List<String> manifestPaths =
        table.currentSnapshot().dataManifests(table.io()).stream()
            .map(ManifestFile::path)
            .sorted()
            .collect(Collectors.toList());
    assertThat(manifestPaths).as("Must have 2 manifests").hasSize(2);

    FilesTable filesTable = new FilesTable(table);

    BatchScan scan = filesTable.newBatchScan();

    List<ScanTask> tasks = planTasks(scan);
    assertThat(tasks).as("Expected 2 tasks").hasSize(2);

    FileScanTask t1 = tasks.get(0).asFileScanTask();
    assertThat(manifestPaths).first().as("Task file must match").isEqualTo(t1.file().path());

    FileScanTask t2 = tasks.get(1).asFileScanTask();
    assertThat(manifestPaths).element(1).as("Task file must match").isEqualTo(t2.file().path());

    List<ScanTaskGroup<ScanTask>> taskGroups = planTaskGroups(scan);
    assertThat(taskGroups).as("Expected 1 task group").hasSize(1);

    ScanTaskGroup<ScanTask> tg = taskGroups.get(0);
    assertThat(tg.tasks()).as("Task number must match").hasSize(2);
  }

  // plans tasks and reorders them by file name to have deterministic order
  private List<ScanTask> planTasks(BatchScan scan) {
    try (CloseableIterable<ScanTask> tasks = scan.planFiles()) {
      List<ScanTask> tasksAsList = Lists.newArrayList(tasks);
      tasksAsList.sort((t1, t2) -> path(t1).compareTo(path(t2)));
      return tasksAsList;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private List<ScanTaskGroup<ScanTask>> planTaskGroups(BatchScan scan) {
    try (CloseableIterable<ScanTaskGroup<ScanTask>> taskGroups = scan.planTasks()) {
      return Lists.newArrayList(taskGroups);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String path(ScanTask task) {
    return ((ContentScanTask<?>) task).file().path().toString();
  }
}
