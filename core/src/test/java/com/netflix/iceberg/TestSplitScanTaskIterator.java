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

package com.netflix.iceberg;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;

import static com.netflix.iceberg.BaseFileScanTask.SplitScanTaskIterator;

public class TestSplitScanTaskIterator {
  @Test
  public void testSplits() {
    verify(15L, 100L,
        Lists.newArrayList(0L, 15L, 15L, 15L, 30L, 15L, 45L, 15L, 60L, 15L, 75L, 15L, 90L, 10L));
    verify(10L, 10L,
        Lists.newArrayList(0L, 10L));
    verify(20L, 10L,
        Lists.newArrayList(0L, 10L));
  }

  private static void verify(long splitSize, long fileLen, List<Long> offsetLenPairs) {
    List<FileScanTask> tasks = Lists.newArrayList(new SplitScanTaskIterator(splitSize, new MockFileScanTask(fileLen)));
    int i = 0;
    for (FileScanTask task : tasks) {
      long offset = offsetLenPairs.get(i);
      long length = offsetLenPairs.get(i + 1);
      Assert.assertEquals(offset, task.start());
      Assert.assertEquals(length, task.length());
      i += 2;
    }
  }

  private static class MockFileScanTask extends BaseFileScanTask {
    private final long length;

    MockFileScanTask(long length) {
      super(null, null, null, null);
      this.length = length;
    }

    @Override
    public long length() {
      return length;
    }
  }
}
