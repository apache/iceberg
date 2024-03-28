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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSizeBasedRewriter extends TestBase {

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  @TestTemplate
  public void testSplitSizeLowerBound() {
    SizeBasedDataFileRewriterImpl rewriter = new SizeBasedDataFileRewriterImpl(table);

    FileScanTask task1 = new MockFileScanTask(145L * 1024 * 1024);
    FileScanTask task2 = new MockFileScanTask(145L * 1024 * 1024);
    FileScanTask task3 = new MockFileScanTask(145L * 1024 * 1024);
    FileScanTask task4 = new MockFileScanTask(145L * 1024 * 1024);
    List<FileScanTask> tasks = ImmutableList.of(task1, task2, task3, task4);

    long minFileSize = 256L * 1024 * 1024;
    long targetFileSize = 512L * 1024 * 1024;
    long maxFileSize = 768L * 1024 * 1024;

    Map<String, String> options =
        ImmutableMap.of(
            SizeBasedDataRewriter.MIN_FILE_SIZE_BYTES, String.valueOf(minFileSize),
            SizeBasedDataRewriter.TARGET_FILE_SIZE_BYTES, String.valueOf(targetFileSize),
            SizeBasedDataRewriter.MAX_FILE_SIZE_BYTES, String.valueOf(maxFileSize));
    rewriter.init(options);

    // the total task size is 580 MB and the target file size is 512 MB
    // the remainder must be written into a separate file as it exceeds 10%
    long numOutputFiles = rewriter.computeNumOutputFiles(tasks);
    assertThat(numOutputFiles).isEqualTo(2);

    // the split size must be >= targetFileSize and < maxFileSize
    long splitSize = rewriter.computeSplitSize(tasks);
    assertThat(splitSize).isGreaterThanOrEqualTo(targetFileSize);
    assertThat(splitSize).isLessThan(maxFileSize);
  }

  private static class SizeBasedDataFileRewriterImpl extends SizeBasedDataRewriter {

    SizeBasedDataFileRewriterImpl(Table table) {
      super(table);
    }

    @Override
    public Set<DataFile> rewrite(List<FileScanTask> group) {
      throw new UnsupportedOperationException("Not implemented");
    }

    public long computeSplitSize(List<FileScanTask> group) {
      return splitSize(inputSize(group));
    }

    public long computeNumOutputFiles(List<FileScanTask> group) {
      return numOutputFiles(inputSize(group));
    }
  }
}
