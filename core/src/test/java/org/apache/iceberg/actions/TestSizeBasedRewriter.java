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
package org.apache.iceberg.actions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

@ExtendWith(ParameterizedTestExtension.class)
class TestSizeBasedRewriter extends TestBase {

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  @TestTemplate
  void testSplitSizeLowerBound() {
    FileScanTask task1 = new MockFileScanTask(mockDataFile());
    FileScanTask task2 = new MockFileScanTask(mockDataFile());
    FileScanTask task3 = new MockFileScanTask(mockDataFile());
    FileScanTask task4 = new MockFileScanTask(mockDataFile());
    List<FileScanTask> tasks = ImmutableList.of(task1, task2, task3, task4);

    RewriteFileGroupPlanner planner = new TestingPlanner(table, Expressions.alwaysTrue(), 1, tasks);

    long minFileSize = 256L * 1024 * 1024;
    long targetFileSize = 512L * 1024 * 1024;
    long maxFileSize = 768L * 1024 * 1024;

    Map<String, String> options =
        ImmutableMap.of(
            RewriteFileGroupPlanner.MIN_FILE_SIZE_BYTES, String.valueOf(minFileSize),
            RewriteFileGroupPlanner.TARGET_FILE_SIZE_BYTES, String.valueOf(targetFileSize),
            RewriteFileGroupPlanner.MAX_FILE_SIZE_BYTES, String.valueOf(maxFileSize));
    planner.init(options);

    // the total task size is 580 MB and the target file size is 512 MB
    // the remainder must be written into a separate file as it exceeds 10%

    RewriteFileGroup group = planner.plan().groups().iterator().next();

    assertThat(group.expectedOutputFiles()).isEqualTo(2);

    // the split size must be >= targetFileSize and < maxFileSize
    long splitSize = group.sizeInBytes();
    assertThat(splitSize).isGreaterThanOrEqualTo(targetFileSize).isLessThan(maxFileSize);
  }

  private static class TestingPlanner extends RewriteFileGroupPlanner {
    private final List<FileScanTask> tasks;

    private TestingPlanner(
        Table table, Expression filter, long snapshotId, List<FileScanTask> tasks) {
      super(table, filter, snapshotId, false);
      this.tasks = tasks;
    }

    @Override
    CloseableIterable<FileScanTask> tasks() {
      return CloseableIterable.withNoopClose(tasks);
    }
  }

  private DataFile mockDataFile() {
    DataFile file = Mockito.mock(DataFile.class);
    when(file.partition()).thenReturn(Mockito.mock(StructLike.class));
    when(file.fileSizeInBytes()).thenReturn(145L * 1024 * 1024);
    return file;
  }
}
