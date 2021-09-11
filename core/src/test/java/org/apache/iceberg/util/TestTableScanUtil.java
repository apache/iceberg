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

package org.apache.iceberg.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang.ArrayUtils;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class TestTableScanUtil {

  private List<FileScanTask> tasksWithDataAndDeleteSizes(List<Pair<Long, Long[]>> sizePairs) {
    return sizePairs.stream().map(
        sizePair -> MockFileScanTask.mockTaskWithDataAndDeleteSizes(sizePair.first(),
            ArrayUtils.toPrimitive(sizePair.second()))
    ).collect(Collectors.toList());
  }

  @Test
  public void testPlanTaskWithDeleteFiles() {
    List<FileScanTask> testFiles = tasksWithDataAndDeleteSizes(
        Arrays.asList(
            Pair.of(100L, new Long[] {20L, 20L}),
            Pair.of(250L, new Long[0]),
            Pair.of(100L, new Long[] {40L, 40L, 40L}),
            Pair.of(510L, new Long[] {1L, 1L}),
            Pair.of(1L, new Long[0]),
            Pair.of(100L, new Long[] {40L, 40L, 40L})
        ));

    List<CombinedScanTask> combinedScanTasks = Lists.newArrayList(
        TableScanUtil.planTasks(CloseableIterable.withNoopClose(testFiles), 512L, 1, 4L)
    );

    List<CombinedScanTask> expectedCombinedTasks = Arrays.asList(
        new BaseCombinedScanTask(Arrays.asList(testFiles.get(0), testFiles.get(1))),
        new BaseCombinedScanTask(Collections.singletonList(testFiles.get(3))),
        new BaseCombinedScanTask(Arrays.asList(testFiles.get(2), testFiles.get(4), testFiles.get(5)))
    );

    Assert.assertEquals("Should plan 3 Combined tasks since there is delete files to be considered",
        3, combinedScanTasks.size());
    for (int i = 0; i < expectedCombinedTasks.size(); ++i) {
      Assert.assertEquals("Scan tasks detail in combined task check failed",
          expectedCombinedTasks.get(i).files(), combinedScanTasks.get(i).files());
    }
  }
}
