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
package org.apache.iceberg.spark.actions;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.SizeBasedDataRewriter;
import org.apache.iceberg.actions.SizeBasedFileRewriter;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestSparkFileRewriter extends SparkTestBase {

  private static final TableIdentifier TABLE_IDENT = TableIdentifier.of("default", "tbl");
  private static final Schema SCHEMA =
      new Schema(
          NestedField.required(1, "id", IntegerType.get()),
          NestedField.required(2, "dep", StringType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("dep").build();
  private static final SortOrder SORT_ORDER = SortOrder.builderFor(SCHEMA).asc("id").build();

  @After
  public void removeTable() {
    catalog.dropTable(TABLE_IDENT);
  }

  @Test
  public void testBinPackDataSelectFiles() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkBinPackDataRewriter rewriter = new SparkBinPackDataRewriter(spark, table);

    checkDataFileSizeFiltering(rewriter);
    checkDataFilesDeleteThreshold(rewriter);
    checkDataFileGroupWithEnoughFiles(rewriter);
    checkDataFileGroupWithEnoughData(rewriter);
    checkDataFileGroupWithTooMuchData(rewriter);
  }

  @Test
  public void testSortDataSelectFiles() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkSortDataRewriter rewriter = new SparkSortDataRewriter(spark, table, SORT_ORDER);

    checkDataFileSizeFiltering(rewriter);
    checkDataFilesDeleteThreshold(rewriter);
    checkDataFileGroupWithEnoughFiles(rewriter);
    checkDataFileGroupWithEnoughData(rewriter);
    checkDataFileGroupWithTooMuchData(rewriter);
  }

  @Test
  public void testZOrderDataSelectFiles() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    ImmutableList<String> zOrderCols = ImmutableList.of("id");
    SparkZOrderDataRewriter rewriter = new SparkZOrderDataRewriter(spark, table, zOrderCols);

    checkDataFileSizeFiltering(rewriter);
    checkDataFilesDeleteThreshold(rewriter);
    checkDataFileGroupWithEnoughFiles(rewriter);
    checkDataFileGroupWithEnoughData(rewriter);
    checkDataFileGroupWithTooMuchData(rewriter);
  }

  private void checkDataFileSizeFiltering(SizeBasedDataRewriter rewriter) {
    FileScanTask tooSmallTask = new MockFileScanTask(100L);
    FileScanTask optimal = new MockFileScanTask(450);
    FileScanTask tooBigTask = new MockFileScanTask(1000L);
    List<FileScanTask> tasks = ImmutableList.of(tooSmallTask, optimal, tooBigTask);

    Map<String, String> options =
        ImmutableMap.of(
            SizeBasedDataRewriter.MIN_FILE_SIZE_BYTES, "250",
            SizeBasedDataRewriter.TARGET_FILE_SIZE_BYTES, "500",
            SizeBasedDataRewriter.MAX_FILE_SIZE_BYTES, "750",
            SizeBasedDataRewriter.DELETE_FILE_THRESHOLD, String.valueOf(Integer.MAX_VALUE));
    rewriter.init(options);

    Iterable<List<FileScanTask>> groups = rewriter.planFileGroups(tasks);
    Assert.assertEquals("Must have 1 group", 1, Iterables.size(groups));
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    Assert.assertEquals("Must rewrite 2 files", 2, group.size());
  }

  private void checkDataFilesDeleteThreshold(SizeBasedDataRewriter rewriter) {
    FileScanTask tooManyDeletesTask = MockFileScanTask.mockTaskWithDeletes(1000L, 3);
    FileScanTask optimalTask = MockFileScanTask.mockTaskWithDeletes(1000L, 1);
    List<FileScanTask> tasks = ImmutableList.of(tooManyDeletesTask, optimalTask);

    Map<String, String> options =
        ImmutableMap.of(
            SizeBasedDataRewriter.MIN_FILE_SIZE_BYTES, "1",
            SizeBasedDataRewriter.TARGET_FILE_SIZE_BYTES, "2000",
            SizeBasedDataRewriter.MAX_FILE_SIZE_BYTES, "5000",
            SizeBasedDataRewriter.DELETE_FILE_THRESHOLD, "2");
    rewriter.init(options);

    Iterable<List<FileScanTask>> groups = rewriter.planFileGroups(tasks);
    Assert.assertEquals("Must have 1 group", 1, Iterables.size(groups));
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    Assert.assertEquals("Must rewrite 1 file", 1, group.size());
  }

  private void checkDataFileGroupWithEnoughFiles(SizeBasedDataRewriter rewriter) {
    List<FileScanTask> tasks =
        ImmutableList.of(
            new MockFileScanTask(100L),
            new MockFileScanTask(100L),
            new MockFileScanTask(100L),
            new MockFileScanTask(100L));

    Map<String, String> options =
        ImmutableMap.of(
            SizeBasedDataRewriter.MIN_INPUT_FILES, "3",
            SizeBasedDataRewriter.MIN_FILE_SIZE_BYTES, "150",
            SizeBasedDataRewriter.TARGET_FILE_SIZE_BYTES, "1000",
            SizeBasedDataRewriter.MAX_FILE_SIZE_BYTES, "5000",
            SizeBasedDataRewriter.DELETE_FILE_THRESHOLD, String.valueOf(Integer.MAX_VALUE));
    rewriter.init(options);

    Iterable<List<FileScanTask>> groups = rewriter.planFileGroups(tasks);
    Assert.assertEquals("Must have 1 group", 1, Iterables.size(groups));
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    Assert.assertEquals("Must rewrite 4 files", 4, group.size());
  }

  private void checkDataFileGroupWithEnoughData(SizeBasedDataRewriter rewriter) {
    List<FileScanTask> tasks =
        ImmutableList.of(
            new MockFileScanTask(100L), new MockFileScanTask(100L), new MockFileScanTask(100L));

    Map<String, String> options =
        ImmutableMap.of(
            SizeBasedDataRewriter.MIN_INPUT_FILES, "5",
            SizeBasedDataRewriter.MIN_FILE_SIZE_BYTES, "200",
            SizeBasedDataRewriter.TARGET_FILE_SIZE_BYTES, "250",
            SizeBasedDataRewriter.MAX_FILE_SIZE_BYTES, "500",
            SizeBasedDataRewriter.DELETE_FILE_THRESHOLD, String.valueOf(Integer.MAX_VALUE));
    rewriter.init(options);

    Iterable<List<FileScanTask>> groups = rewriter.planFileGroups(tasks);
    Assert.assertEquals("Must have 1 group", 1, Iterables.size(groups));
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    Assert.assertEquals("Must rewrite 3 files", 3, group.size());
  }

  private void checkDataFileGroupWithTooMuchData(SizeBasedDataRewriter rewriter) {
    List<FileScanTask> tasks = ImmutableList.of(new MockFileScanTask(2000L));

    Map<String, String> options =
        ImmutableMap.of(
            SizeBasedDataRewriter.MIN_INPUT_FILES, "5",
            SizeBasedDataRewriter.MIN_FILE_SIZE_BYTES, "200",
            SizeBasedDataRewriter.TARGET_FILE_SIZE_BYTES, "250",
            SizeBasedDataRewriter.MAX_FILE_SIZE_BYTES, "500",
            SizeBasedDataRewriter.DELETE_FILE_THRESHOLD, String.valueOf(Integer.MAX_VALUE));
    rewriter.init(options);

    Iterable<List<FileScanTask>> groups = rewriter.planFileGroups(tasks);
    Assert.assertEquals("Must have 1 group", 1, Iterables.size(groups));
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    Assert.assertEquals("Must rewrite big file", 1, group.size());
  }

  @Test
  public void testInvalidConstructorUsagesSortData() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);

    Assertions.assertThatThrownBy(() -> new SparkSortDataRewriter(spark, table))
        .hasMessageContaining("Cannot sort data without a valid sort order")
        .hasMessageContaining("is unsorted and no sort order is provided");

    Assertions.assertThatThrownBy(() -> new SparkSortDataRewriter(spark, table, null))
        .hasMessageContaining("Cannot sort data without a valid sort order")
        .hasMessageContaining("the provided sort order is null or empty");

    Assertions.assertThatThrownBy(
            () -> new SparkSortDataRewriter(spark, table, SortOrder.unsorted()))
        .hasMessageContaining("Cannot sort data without a valid sort order")
        .hasMessageContaining("the provided sort order is null or empty");
  }

  @Test
  public void testInvalidConstructorUsagesZOrderData() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA, SPEC);

    Assertions.assertThatThrownBy(() -> new SparkZOrderDataRewriter(spark, table, null))
        .hasMessageContaining("Cannot ZOrder when no columns are specified");

    Assertions.assertThatThrownBy(
            () -> new SparkZOrderDataRewriter(spark, table, ImmutableList.of()))
        .hasMessageContaining("Cannot ZOrder when no columns are specified");

    Assertions.assertThatThrownBy(
            () -> new SparkZOrderDataRewriter(spark, table, ImmutableList.of("dep")))
        .hasMessageContaining("Cannot ZOrder")
        .hasMessageContaining("all columns provided were identity partition columns");

    Assertions.assertThatThrownBy(
            () -> new SparkZOrderDataRewriter(spark, table, ImmutableList.of("DeP")))
        .hasMessageContaining("Cannot ZOrder")
        .hasMessageContaining("all columns provided were identity partition columns");
  }

  @Test
  public void testBinPackDataValidOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkBinPackDataRewriter rewriter = new SparkBinPackDataRewriter(spark, table);

    Assert.assertEquals(
        "Rewriter must report all supported options",
        ImmutableSet.of(
            SparkBinPackDataRewriter.TARGET_FILE_SIZE_BYTES,
            SparkBinPackDataRewriter.MIN_FILE_SIZE_BYTES,
            SparkBinPackDataRewriter.MAX_FILE_SIZE_BYTES,
            SparkBinPackDataRewriter.MIN_INPUT_FILES,
            SparkBinPackDataRewriter.REWRITE_ALL,
            SparkBinPackDataRewriter.MAX_FILE_GROUP_SIZE_BYTES,
            SparkBinPackDataRewriter.DELETE_FILE_THRESHOLD),
        rewriter.validOptions());
  }

  @Test
  public void testSortDataValidOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkSortDataRewriter rewriter = new SparkSortDataRewriter(spark, table, SORT_ORDER);

    Assert.assertEquals(
        "Rewriter must report all supported options",
        ImmutableSet.of(
            SparkSortDataRewriter.TARGET_FILE_SIZE_BYTES,
            SparkSortDataRewriter.MIN_FILE_SIZE_BYTES,
            SparkSortDataRewriter.MAX_FILE_SIZE_BYTES,
            SparkSortDataRewriter.MIN_INPUT_FILES,
            SparkSortDataRewriter.REWRITE_ALL,
            SparkSortDataRewriter.MAX_FILE_GROUP_SIZE_BYTES,
            SparkSortDataRewriter.DELETE_FILE_THRESHOLD,
            SparkSortDataRewriter.COMPRESSION_FACTOR),
        rewriter.validOptions());
  }

  @Test
  public void testZOrderDataValidOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    ImmutableList<String> zOrderCols = ImmutableList.of("id");
    SparkZOrderDataRewriter rewriter = new SparkZOrderDataRewriter(spark, table, zOrderCols);

    Assert.assertEquals(
        "Rewriter must report all supported options",
        ImmutableSet.of(
            SparkZOrderDataRewriter.TARGET_FILE_SIZE_BYTES,
            SparkZOrderDataRewriter.MIN_FILE_SIZE_BYTES,
            SparkZOrderDataRewriter.MAX_FILE_SIZE_BYTES,
            SparkZOrderDataRewriter.MIN_INPUT_FILES,
            SparkZOrderDataRewriter.REWRITE_ALL,
            SparkZOrderDataRewriter.MAX_FILE_GROUP_SIZE_BYTES,
            SparkZOrderDataRewriter.DELETE_FILE_THRESHOLD,
            SparkZOrderDataRewriter.COMPRESSION_FACTOR,
            SparkZOrderDataRewriter.MAX_OUTPUT_SIZE,
            SparkZOrderDataRewriter.VAR_LENGTH_CONTRIBUTION),
        rewriter.validOptions());
  }

  @Test
  public void testInvalidValuesForBinPackDataOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkBinPackDataRewriter rewriter = new SparkBinPackDataRewriter(spark, table);

    validateSizeBasedRewriterOptions(rewriter);

    Map<String, String> invalidDeleteThresholdOptions =
        ImmutableMap.of(SizeBasedDataRewriter.DELETE_FILE_THRESHOLD, "-1");
    Assertions.assertThatThrownBy(() -> rewriter.init(invalidDeleteThresholdOptions))
        .hasMessageContaining("'delete-file-threshold' is set to -1 but must be >= 0");
  }

  @Test
  public void testInvalidValuesForSortDataOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkSortDataRewriter rewriter = new SparkSortDataRewriter(spark, table, SORT_ORDER);

    validateSizeBasedRewriterOptions(rewriter);

    Map<String, String> invalidDeleteThresholdOptions =
        ImmutableMap.of(SizeBasedDataRewriter.DELETE_FILE_THRESHOLD, "-1");
    Assertions.assertThatThrownBy(() -> rewriter.init(invalidDeleteThresholdOptions))
        .hasMessageContaining("'delete-file-threshold' is set to -1 but must be >= 0");

    Map<String, String> invalidCompressionFactorOptions =
        ImmutableMap.of(SparkShufflingDataRewriter.COMPRESSION_FACTOR, "0");
    Assertions.assertThatThrownBy(() -> rewriter.init(invalidCompressionFactorOptions))
        .hasMessageContaining("'compression-factor' is set to 0.0 but must be > 0");
  }

  @Test
  public void testInvalidValuesForZOrderDataOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    ImmutableList<String> zOrderCols = ImmutableList.of("id");
    SparkZOrderDataRewriter rewriter = new SparkZOrderDataRewriter(spark, table, zOrderCols);

    validateSizeBasedRewriterOptions(rewriter);

    Map<String, String> invalidDeleteThresholdOptions =
        ImmutableMap.of(SizeBasedDataRewriter.DELETE_FILE_THRESHOLD, "-1");
    Assertions.assertThatThrownBy(() -> rewriter.init(invalidDeleteThresholdOptions))
        .hasMessageContaining("'delete-file-threshold' is set to -1 but must be >= 0");

    Map<String, String> invalidCompressionFactorOptions =
        ImmutableMap.of(SparkShufflingDataRewriter.COMPRESSION_FACTOR, "0");
    Assertions.assertThatThrownBy(() -> rewriter.init(invalidCompressionFactorOptions))
        .hasMessageContaining("'compression-factor' is set to 0.0 but must be > 0");

    Map<String, String> invalidMaxOutputOptions =
        ImmutableMap.of(SparkZOrderDataRewriter.MAX_OUTPUT_SIZE, "0");
    Assertions.assertThatThrownBy(() -> rewriter.init(invalidMaxOutputOptions))
        .hasMessageContaining("Cannot have the interleaved ZOrder value use less than 1 byte")
        .hasMessageContaining("'max-output-size' was set to 0");

    Map<String, String> invalidVarLengthContributionOptions =
        ImmutableMap.of(SparkZOrderDataRewriter.VAR_LENGTH_CONTRIBUTION, "0");
    Assertions.assertThatThrownBy(() -> rewriter.init(invalidVarLengthContributionOptions))
        .hasMessageContaining("Cannot use less than 1 byte for variable length types with ZOrder")
        .hasMessageContaining("'var-length-contribution' was set to 0");
  }

  private void validateSizeBasedRewriterOptions(SizeBasedFileRewriter<?, ?> rewriter) {
    Map<String, String> invalidTargetSizeOptions =
        ImmutableMap.of(SizeBasedFileRewriter.TARGET_FILE_SIZE_BYTES, "0");
    Assertions.assertThatThrownBy(() -> rewriter.init(invalidTargetSizeOptions))
        .hasMessageContaining("'target-file-size-bytes' is set to 0 but must be > 0");

    Map<String, String> invalidMinSizeOptions =
        ImmutableMap.of(SizeBasedFileRewriter.MIN_FILE_SIZE_BYTES, "-1");
    Assertions.assertThatThrownBy(() -> rewriter.init(invalidMinSizeOptions))
        .hasMessageContaining("'min-file-size-bytes' is set to -1 but must be >= 0");

    Map<String, String> invalidTargetMinSizeOptions =
        ImmutableMap.of(
            SizeBasedFileRewriter.TARGET_FILE_SIZE_BYTES, "3",
            SizeBasedFileRewriter.MIN_FILE_SIZE_BYTES, "5");
    Assertions.assertThatThrownBy(() -> rewriter.init(invalidTargetMinSizeOptions))
        .hasMessageContaining("'target-file-size-bytes' (3) must be > 'min-file-size-bytes' (5)")
        .hasMessageContaining("all new files will be smaller than the min threshold");

    Map<String, String> invalidTargetMaxSizeOptions =
        ImmutableMap.of(
            SizeBasedFileRewriter.TARGET_FILE_SIZE_BYTES, "5",
            SizeBasedFileRewriter.MAX_FILE_SIZE_BYTES, "3");
    Assertions.assertThatThrownBy(() -> rewriter.init(invalidTargetMaxSizeOptions))
        .hasMessageContaining("'target-file-size-bytes' (5) must be < 'max-file-size-bytes' (3)")
        .hasMessageContaining("all new files will be larger than the max threshold");

    Map<String, String> invalidMinInputFilesOptions =
        ImmutableMap.of(SizeBasedFileRewriter.MIN_INPUT_FILES, "0");
    Assertions.assertThatThrownBy(() -> rewriter.init(invalidMinInputFilesOptions))
        .hasMessageContaining("'min-input-files' is set to 0 but must be > 0");

    Map<String, String> invalidMaxFileGroupSizeOptions =
        ImmutableMap.of(SizeBasedFileRewriter.MAX_FILE_GROUP_SIZE_BYTES, "0");
    Assertions.assertThatThrownBy(() -> rewriter.init(invalidMaxFileGroupSizeOptions))
        .hasMessageContaining("'max-file-group-size-bytes' is set to 0 but must be > 0");
  }
}
