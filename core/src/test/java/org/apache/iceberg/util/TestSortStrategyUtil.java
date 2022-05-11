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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestSortStrategyUtil {
  private static final double FLOATING_POINT_DELTA = 0.001;
  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.LongType.get()),
      optional(2, "strCol", Types.StringType.get()),
      required(3, "intCol", Types.IntegerType.get())
  );

  private static final SortOrder SORT_ORDER1 = SortOrder.builderFor(SCHEMA.select("intCol"))
      .asc("intCol")
      .build();
  private static final SortOrder SORT_ORDER2 = SortOrder.builderFor(SCHEMA.select("intCol", "strCol"))
      .asc("intCol")
      .asc("strCol")
      .build();

  private static DataFile buildDataFile(int minInt, int maxInt) {
    return buildDataFile(minInt, maxInt, "", "");
  }

  private static DataFile buildDataFile(int minInt, int maxInt, String minStr, String maxStr) {
    return new TestHelpers.TestDataFile("test_file", TestHelpers.Row.of(), 100,
        // any value counts, including nulls
        ImmutableMap.of(),
        // null value counts
        ImmutableMap.of(),
        // nan value counts
        ImmutableMap.of(),
        // lower bounds
        ImmutableMap.of(
            2, toByteBuffer(Types.StringType.get(), minStr),
            3, toByteBuffer(Types.IntegerType.get(), minInt)
        ),
        // upper bounds
        ImmutableMap.of(
            2, toByteBuffer(Types.StringType.get(), maxStr),
            3, toByteBuffer(Types.IntegerType.get(), maxInt)
        )
    );
  }

  private static FileScanTask buildFileScanTask(int minInt, int maxInt) {
    return new MockFileScanTask(buildDataFile(minInt, maxInt));
  }

  private List<ByteBuffer> getDataPoint(int intVal) {
    return ImmutableList.of(toByteBuffer(Types.IntegerType.get(), intVal));
  }

  private List<ByteBuffer> getDataPoint(int intVal, String strVal) {
    return ImmutableList.of(
        toByteBuffer(Types.IntegerType.get(), intVal),
        toByteBuffer(Types.StringType.get(), strVal)
    );
  }

  private void testOverlapCountHelper(List<DataFile> files, int intVal, int expected) {
    int actual = SortStrategyUtil.overlapCount(
        getDataPoint(intVal),
        files,
        SortStrategyUtil.getSortColumnIndices(SORT_ORDER1),
        SORT_ORDER1.schema()
    );
    Assert.assertEquals(expected, actual, FLOATING_POINT_DELTA);
  }

  private void testOverlapCountHelper(List<DataFile> files, int intVal, String strVal, int expected) {
    int actual = SortStrategyUtil.overlapCount(
        getDataPoint(intVal, strVal),
        files,
        SortStrategyUtil.getSortColumnIndices(SORT_ORDER2),
        SORT_ORDER2.schema()
    );
    Assert.assertEquals(expected, actual, FLOATING_POINT_DELTA);
  }

  @Test
  public void testOverlapCount() {
    List<DataFile> files1 = ImmutableList.of(
        buildDataFile(1, 100),
        buildDataFile(3, 98),
        buildDataFile(5, 96),
        buildDataFile(7, 94),
        buildDataFile(9, 92)
    );
    testOverlapCountHelper(files1, 0, 0);
    testOverlapCountHelper(files1, 101, 0);
    testOverlapCountHelper(files1, 2, 1);
    testOverlapCountHelper(files1, 3, 2);
    testOverlapCountHelper(files1, 95, 3);
    testOverlapCountHelper(files1, 94, 4);
    testOverlapCountHelper(files1, 9, 5);
    testOverlapCountHelper(files1, 10, 5);
    testOverlapCountHelper(files1, 92, 5);
    testOverlapCountHelper(files1, 91, 5);

    List<DataFile> files2 = ImmutableList.of(
        buildDataFile(5, 5, "aa", "z"),
        buildDataFile(5, 5, "bb", "y"),
        buildDataFile(5, 5, "cc", "x"),
        buildDataFile(1, 94, "dd", "w"),
        buildDataFile(3, 92, "ee", "v")
    );
    testOverlapCountHelper(files2, 0, "h", 0);
    testOverlapCountHelper(files2, 95, "g", 0);
    testOverlapCountHelper(files2, 3, "g", 2);
    testOverlapCountHelper(files2, 5, "a", 2);
    testOverlapCountHelper(files2, 5, "b", 3);
    testOverlapCountHelper(files2, 5, "bb", 4);
    testOverlapCountHelper(files2, 5, "xx", 4);
    testOverlapCountHelper(files2, 5, "h", 5);
  }

  private void testSortednessScoreHelper1(List<DataFile> files, double expected) {
    double actual = SortStrategyUtil.sortednessScore(files, SORT_ORDER1);
    Assert.assertEquals(expected, actual, FLOATING_POINT_DELTA);
  }

  private void testSortednessScoreHelper2(List<DataFile> files, double expected) {
    double actual = SortStrategyUtil.sortednessScore(files, SORT_ORDER2);
    Assert.assertEquals(expected, actual, FLOATING_POINT_DELTA);
  }

  @Test
  public void testSortednessScore() {
    testSortednessScoreHelper1(
        ImmutableList.of(
            buildDataFile(1, 3)
        ),
        1.0
    );

    testSortednessScoreHelper1(
        ImmutableList.of(
            buildDataFile(1, 3),
            buildDataFile(1, 3),
            buildDataFile(1, 3),
            buildDataFile(1, 3),
            buildDataFile(1, 3)
        ),
        0.0
    );

    testSortednessScoreHelper1(
        ImmutableList.of(
            buildDataFile(1, 3),
            buildDataFile(1, 3),
            buildDataFile(1, 3),
            buildDataFile(1, 3),
            buildDataFile(1, 5)
        ),
        0.1
    );

    testSortednessScoreHelper1(
        ImmutableList.of(
            buildDataFile(1, 10),
            buildDataFile(2, 9),
            buildDataFile(3, 8),
            buildDataFile(4, 7),
            buildDataFile(5, 6)
        ),
        0.5
    );

    testSortednessScoreHelper1(
        ImmutableList.of(
            buildDataFile(1, 2),
            buildDataFile(3, 4),
            buildDataFile(5, 6),
            buildDataFile(7, 8),
            buildDataFile(1, 8)
        ),
        0.75
    );

    testSortednessScoreHelper1(
        ImmutableList.of(
            buildDataFile(1, 3),
            buildDataFile(2, 5),
            buildDataFile(4, 7),
            buildDataFile(6, 9),
            buildDataFile(8, 10)
        ),
        0.8
    );

    testSortednessScoreHelper1(
        ImmutableList.of(
            buildDataFile(1, 3),
            buildDataFile(2, 4),
            buildDataFile(5, 6),
            buildDataFile(7, 8),
            buildDataFile(9, 10)
        ),
        0.95
    );

    testSortednessScoreHelper1(
        ImmutableList.of(
            buildDataFile(1, 2),
            buildDataFile(3, 4),
            buildDataFile(5, 6),
            buildDataFile(7, 8),
            buildDataFile(9, 10)
        ),
        1.0
    );

    testSortednessScoreHelper2(
        ImmutableList.of(
            buildDataFile(1, 2, "a", "b"),
            buildDataFile(3, 4, "a", "b"),
            buildDataFile(5, 6, "a", "b"),
            buildDataFile(7, 8, "a", "b"),
            buildDataFile(9, 10, "a", "b")
        ),
        1.0
    );

    testSortednessScoreHelper2(
        ImmutableList.of(
            buildDataFile(1, 1, "a", "bb"),
            buildDataFile(1, 1, "b", "c"),
            buildDataFile(1, 1, "d", "e"),
            buildDataFile(1, 1, "f", "g"),
            buildDataFile(1, 1, "h", "i")
        ),
        0.95
    );

    testSortednessScoreHelper2(
        ImmutableList.of(
            buildDataFile(1, 1, "aa", "z"),
            buildDataFile(1, 1, "bb", "y"),
            buildDataFile(1, 1, "cc", "x"),
            buildDataFile(1, 1, "dd", "w"),
            buildDataFile(1, 1, "ee", "v")
        ),
        0.5
    );

    testSortednessScoreHelper2(
        ImmutableList.of(
            buildDataFile(1, 1, "a", "b"),
            buildDataFile(1, 1, "a", "b"),
            buildDataFile(1, 1, "a", "b"),
            buildDataFile(1, 1, "a", "b"),
            buildDataFile(1, 1, "a", "b")
        ),
        0.0
    );
  }

  private void testRemoveSortedFilesHelper(List<FileScanTask> files, int expectedResultCount) {
    int actual = Iterables.size(SortStrategyUtil.removeSortedFiles(files, SORT_ORDER1));
    Assert.assertEquals(expectedResultCount, actual);
  }

  @Test
  public void testRemoveSortedFiles() {
    testRemoveSortedFilesHelper(
        ImmutableList.of(
            buildFileScanTask(1, 2),
            buildFileScanTask(3, 4),
            buildFileScanTask(5, 6),
            buildFileScanTask(7, 8),
            buildFileScanTask(9, 10)
        ),
        0
    );

    testRemoveSortedFilesHelper(
        ImmutableList.of(
            buildFileScanTask(1, 3),
            buildFileScanTask(2, 4),
            buildFileScanTask(5, 6),
            buildFileScanTask(7, 8),
            buildFileScanTask(9, 10)
        ),
        2
    );

    testRemoveSortedFilesHelper(
        ImmutableList.of(
            buildFileScanTask(1, 2),
            buildFileScanTask(3, 4),
            buildFileScanTask(5, 6),
            buildFileScanTask(3, 8),
            buildFileScanTask(9, 10)
        ),
        3
    );

    testRemoveSortedFilesHelper(
        ImmutableList.of(
            buildFileScanTask(1, 2),
            buildFileScanTask(3, 4),
            buildFileScanTask(5, 6),
            buildFileScanTask(7, 8),
            buildFileScanTask(1, 10)
        ),
        5
    );
  }
}
