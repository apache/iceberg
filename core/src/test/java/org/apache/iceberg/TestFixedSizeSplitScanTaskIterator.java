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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.BaseFileScanTask.FixedSizeSplitScanTaskIterator;

public class TestFixedSizeSplitScanTaskIterator {
  @Test
  public void testSplits() {
    verify(15L, 100L, asList(
        asList(0L, 15L), asList(15L, 15L), asList(30L, 15L), asList(45L, 15L), asList(60L, 15L),
        asList(75L, 15L), asList(90L, 10L)));
    verify(10L, 10L, asList(asList(0L, 10L)));
    verify(20L, 10L, asList(asList(0L, 10L)));
  }

  private static void verify(long splitSize, long fileLen, List<List<Long>> offsetLenPairs) {
    List<FileScanTask> tasks = Lists.newArrayList(
        new FixedSizeSplitScanTaskIterator(splitSize, new MockFileScanTask(fileLen)));
    for (int i = 0; i < tasks.size(); i++) {
      FileScanTask task = tasks.get(i);
      List<Long> split = offsetLenPairs.get(i);
      long offset = split.get(0);
      long length = split.get(1);
      Assert.assertEquals(offset, task.start());
      Assert.assertEquals(length, task.length());
    }
  }

  private <T> List<T> asList(T... items) {
    return Lists.newArrayList(items);
  }
}
