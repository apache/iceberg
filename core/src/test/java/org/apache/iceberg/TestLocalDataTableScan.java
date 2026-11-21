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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.TestTemplate;

public class TestLocalDataTableScan
    extends DataTableScanTestBase<TableScan, FileScanTask, CombinedScanTask> {

  @Override
  protected TableScan useRef(TableScan scan, String ref) {
    return scan.useRef(ref);
  }

  @Override
  protected TableScan useSnapshot(TableScan scan, long snapshotId) {
    return scan.useSnapshot(snapshotId);
  }

  @Override
  protected TableScan asOfTime(TableScan scan, long timestampMillis) {
    return scan.asOfTime(timestampMillis);
  }

  @Override
  protected TableScan newScan() {
    return table.newScan();
  }

  @TestTemplate
  public void minRowsRequestedStopsPlanning() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).appendFile(FILE_C).commit();

    List<String> planned = plannedFiles(table.newScan().minRowsRequested(FILE_A.recordCount()));

    assertThat(planned).containsExactly(FILE_A.location());
  }

  @TestTemplate
  public void filesWithDeletesDoNotCountTowardMinRowsRequested() throws IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(2);
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).appendFile(FILE_C).commit();
    table.newRowDelta().addDeletes(fileADeletes()).addDeletes(fileBDeletes()).commit();

    List<String> planned = plannedFiles(table.newScan().minRowsRequested(FILE_C.recordCount()));

    assertThat(planned).containsExactly(FILE_A.location(), FILE_B.location(), FILE_C.location());
  }

  @TestTemplate
  public void residualFilterDisablesMinRowsRequested() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).appendFile(FILE_C).commit();

    List<String> planned =
        plannedFiles(
            table
                .newScan()
                .filter(Expressions.greaterThan("id", 5))
                .minRowsRequested(FILE_A.recordCount()));

    assertThat(planned).containsExactly(FILE_A.location(), FILE_B.location(), FILE_C.location());
  }

  @TestTemplate
  public void ignoredResidualsDisableMinRowsRequested() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).appendFile(FILE_C).commit();

    List<String> planned =
        plannedFiles(
            table
                .newScan()
                .filter(Expressions.greaterThan("id", 5))
                .ignoreResiduals()
                .minRowsRequested(FILE_A.recordCount()));

    assertThat(planned).containsExactly(FILE_A.location(), FILE_B.location(), FILE_C.location());
  }

  private static List<String> plannedFiles(TableScan scan) throws IOException {
    List<String> files = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        files.add(task.file().location());
      }
    }

    return files;
  }
}
