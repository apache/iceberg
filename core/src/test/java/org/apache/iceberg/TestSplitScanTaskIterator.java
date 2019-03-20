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

import com.google.common.collect.Lists;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.BaseFileScanTask.SplitScanTaskIterator;

public class TestSplitScanTaskIterator {
  @Test
  public void testSplits() {
    verify(15L, 100L, l(l(0L, 15L), l(15L, 15L), l(30L, 15L), l(45L, 15L), l(60L, 15L), l(75L, 15L), l(90L, 10L)));
    verify(10L, 10L, l(l(0L, 10L)));
    verify(20L, 10L, l(l(0L, 10L)));
  }

  private static void verify(long splitSize, long fileLen, List<List<Long>> offsetLenPairs) {
    List<FileScanTask> tasks = Lists.newArrayList(new SplitScanTaskIterator(splitSize, new MockFileScanTask(fileLen)));
    int i = 0;
    for (FileScanTask task : tasks) {
      List<Long> split = offsetLenPairs.get(i);
      long offset = split.get(0);
      long length = split.get(1);
      Assert.assertEquals(offset, task.start());
      Assert.assertEquals(length, task.length());
      i += 1;
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

  private <T> List<T> l(T... items) {
    return Lists.newArrayList(items);
  }
}
