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
import com.netflix.iceberg.expressions.Expression;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;
import static com.netflix.iceberg.BaseFileScanTask.SplitScanTaskIterator;

public class TestSplitScanTaskIterator {
  @Test
  public void testSplits() {
    long splitSize = 15L;
    List<FileScanTask> tasks = Lists.newArrayList(new SplitScanTaskIterator(splitSize, new MockFileScanTask(100L)));
    Assert.assertEquals(7, tasks.size());
    long offset = 0;
    for (int i = 0; i < tasks.size(); i++) {
      FileScanTask task = tasks.get(i);
      Assert.assertEquals(offset, task.start());
      if (i == tasks.size() - 1) {
        Assert.assertEquals(10, task.length());
      } else {
        Assert.assertEquals(splitSize, task.length());
      }
      offset += splitSize;
    }

    tasks = Lists.newArrayList(new SplitScanTaskIterator(10, new MockFileScanTask(10)));
    Assert.assertEquals(1, tasks.size());
    Assert.assertEquals(tasks.get(0).start(), 0);
    Assert.assertEquals(tasks.get(0).length(), 10);

    tasks = Lists.newArrayList(new SplitScanTaskIterator(20, new MockFileScanTask(10)));
    Assert.assertEquals(1, tasks.size());
    Assert.assertEquals(tasks.get(0).start(), 0);
    Assert.assertEquals(tasks.get(0).length(), 10);
  }

  private static class MockFileScanTask implements FileScanTask {
    private final long length;

    MockFileScanTask(long length) {
      this.length = length;
    }

    @Override
    public DataFile file() {
      return null;
    }

    @Override
    public PartitionSpec spec() {
      return null;
    }

    @Override
    public long start() {
      return 0;
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public Expression residual() {
      return null;
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      return null;
    }
  }
}
