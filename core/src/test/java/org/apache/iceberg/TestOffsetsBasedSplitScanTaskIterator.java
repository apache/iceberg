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

import java.util.List;
import org.apache.iceberg.BaseFileScanTask.SplitScanTask;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class TestOffsetsBasedSplitScanTaskIterator {
  @Test
  public void testSplits() {
    // case when the last row group has more than one byte
    verify(
        asList(4L, 10L, 15L, 18L, 30L, 45L),
        48L,
        asList(
            asList(4L, 6L),
            asList(10L, 5L),
            asList(15L, 3L),
            asList(18L, 12L),
            asList(30L, 15L),
            asList(45L, 3L)));

    // case when the last row group has one byte
    verify(
        asList(4L, 10L, 15L, 18L, 30L, 47L),
        48L,
        asList(
            asList(4L, 6L),
            asList(10L, 5L),
            asList(15L, 3L),
            asList(18L, 12L),
            asList(30L, 17L),
            asList(47L, 1L)));
  }

  private static void verify(
      List<Long> offsetRanges, long fileLen, List<List<Long>> offsetLenPairs) {
    FileScanTask mockFileScanTask = new MockFileScanTask(fileLen);
    SplitScanTaskIterator<FileScanTask> splitTaskIterator =
        new OffsetsAwareSplitScanTaskIterator<>(
            mockFileScanTask,
            mockFileScanTask.length(),
            offsetRanges,
            TestOffsetsBasedSplitScanTaskIterator::createSplitTask);
    List<FileScanTask> tasks = Lists.newArrayList(splitTaskIterator);
    Assert.assertEquals("Number of tasks don't match", offsetLenPairs.size(), tasks.size());

    for (int i = 0; i < tasks.size(); i++) {
      FileScanTask task = tasks.get(i);
      List<Long> split = offsetLenPairs.get(i);
      long offset = split.get(0);
      long length = split.get(1);
      Assert.assertEquals(offset, task.start());
      Assert.assertEquals(length, task.length());
    }
  }

  private static <T> List<T> asList(T... items) {
    return Lists.newArrayList(items);
  }

  private static FileScanTask createSplitTask(FileScanTask parentTask, long offset, long length) {
    return new SplitScanTask(offset, length, parentTask);
  }
}
