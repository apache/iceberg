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


import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSortStrategy extends TableTestBase {

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {2}; // We don't actually use the format version since everything is mock
  }

  @Override
  public void setupTable() throws Exception {
    super.setupTable();
    table.replaceSortOrder().asc("data").commit();
  }

  private static final long MB = 1024 * 1024;

  public TestSortStrategy(int formatVersion) {
    super(formatVersion);
  }

  class TestSortStrategyImpl extends SortStrategy {

    @Override
    public Table table() {
      return table;
    }

    @Override
    public Set<DataFile> rewriteFiles(List<FileScanTask> filesToRewrite) {
      throw new UnsupportedOperationException();
    }
  }

  private SortStrategy defaultSort() {
    return (SortStrategy) new TestSortStrategyImpl().options(Collections.emptyMap());
  }

  private List<FileScanTask> tasksForSortOrder(int sortOrderId, int... fileSizesMB) {
    ImmutableList.Builder<FileScanTask> files = ImmutableList.builder();
    IntStream.of(fileSizesMB).forEach(length -> files.add(MockFileScanTask.mockTask(length * MB, sortOrderId)));
    return files.build();
  }

  @Test
  public void testInvalidSortOrder() {
    AssertHelpers.assertThrows("Should not allow an unsorted Sort order", IllegalArgumentException.class,
        () -> defaultSort().sortOrder(SortOrder.unsorted()).options(Collections.emptyMap()));

    AssertHelpers.assertThrows("Should not allow a Sort order with bad columns", ValidationException.class,
        () -> {
          Schema badSchema = new Schema(
              ImmutableList.of(Types.NestedField.required(0, "nonexistant", Types.IntegerType.get())));

          defaultSort()
              .sortOrder(SortOrder.builderFor(badSchema).asc("nonexistant").build())
              .options(Collections.emptyMap());
        });
  }

  @Test
  public void testRewriteAll() {
    List<FileScanTask> expected = ImmutableList.<FileScanTask>builder()
        .addAll(tasksForSortOrder(table.sortOrder().orderId(), 10, 2000, 500, 490, 520))
        .build();

    RewriteStrategy strategy = defaultSort().options(ImmutableMap.of(SortStrategy.REWRITE_ALL, "true"));
    List<FileScanTask> actual = ImmutableList.copyOf(strategy.selectFilesToRewrite(expected));

    Assert.assertEquals("Should mark all files for rewrite",
        expected, actual);
  }

  @Test
  public void testMisSizedRatioThreshold() {
    List<FileScanTask> expected = ImmutableList.<FileScanTask>builder()
        .addAll(tasksForSortOrder(table.sortOrder().orderId(), 400, 500, 520, 590, 620))
        .build();

    // Note that the actual mis-sized ratio of the above file is 3/5 = 0.6
    RewriteStrategy strategy = defaultSort().options(ImmutableMap.of(
        SortStrategy.MIS_SIZED_RATIO_THRESHOLD, "0.59",
        SortStrategy.MIN_FILE_SIZE_BYTES, Long.toString(450 * MB),
        SortStrategy.MAX_FILE_SIZE_BYTES, Long.toString(550 * MB)));
    List<FileScanTask> actual = ImmutableList.copyOf(strategy.selectFilesToRewrite(expected));
    Assert.assertEquals("Should mark all files for rewrite",
        expected, actual);
  }

  @Test
  public void testSortednessScoreThreshold() {
    List<FileScanTask> expected = ImmutableList.<FileScanTask>builder()
        .addAll(tasksForSortOrder(table.sortOrder().orderId(), 500, 500, 510, 495))
        .build();

    // Note that the sortedness score of existing data files of the table is 0.0
    RewriteStrategy strategy = defaultSort().options(ImmutableMap.of(
        SortStrategy.SORTEDNESS_SCORE_THRESHOLD, "0.01"));
    List<FileScanTask> actual = ImmutableList.copyOf(strategy.selectFilesToRewrite(expected));
    Assert.assertEquals("Should mark all files for rewrite",
        expected, actual);
  }

  @Test
  public void testHaveGoodFileSizes() {
    List<FileScanTask> fileScanTasks = ImmutableList.<FileScanTask>builder()
        .addAll(tasksForSortOrder(table.sortOrder().orderId(), 498, 500, 500, 551))
        .build();

    SortStrategy rewriteAllStrategy = (SortStrategy) defaultSort().options(ImmutableMap.of(
        SortStrategy.MAX_FILE_SIZE_BYTES, Long.toString(550 * MB),
        SortStrategy.MIN_FILE_SIZE_BYTES, Long.toString(499 * MB)));

    Assert.assertFalse(rewriteAllStrategy.haveGoodFileSizes(fileScanTasks));

    SortStrategy rewriteNoneStrategy = (SortStrategy) defaultSort().options(ImmutableMap.of(
        SortStrategy.MAX_FILE_SIZE_BYTES, Long.toString(560 * MB),
        SortStrategy.MIN_FILE_SIZE_BYTES, Long.toString(490 * MB)));

    Assert.assertTrue(rewriteNoneStrategy.haveGoodFileSizes(fileScanTasks));
  }
}
