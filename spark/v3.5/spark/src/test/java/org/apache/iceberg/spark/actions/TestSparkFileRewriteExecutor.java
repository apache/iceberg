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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteFileGroupPlanner;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TestSparkFileRewriteExecutor extends TestBase {

  private static final TableIdentifier TABLE_IDENT = TableIdentifier.of("default", "tbl");
  private static final Schema SCHEMA =
      new Schema(
          NestedField.required(1, "id", IntegerType.get()),
          NestedField.required(2, "dep", StringType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("dep").build();
  private static final SortOrder SORT_ORDER = SortOrder.builderFor(SCHEMA).asc("id").build();

  @AfterEach
  public void removeTable() {
    catalog.dropTable(TABLE_IDENT);
  }

  @Test
  public void testBinPackDataSelectFiles() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    RewriteFileGroupPlanner rewriter =
        new RewriteFileGroupPlanner(table, Expressions.alwaysTrue(), 1, false);

    checkDataFileSizeFiltering(rewriter);
    checkDataFilesDeleteThreshold(rewriter);
    checkDataFileGroupWithEnoughFiles(rewriter);
    checkDataFileGroupWithEnoughData(rewriter);
    checkDataFileGroupWithTooMuchData(rewriter);
  }

  private void checkDataFileSizeFiltering(RewriteFileGroupPlanner rewriter) {
    FileScanTask tooSmallTask = new MockFileScanTask(100L);
    FileScanTask optimal = new MockFileScanTask(450);
    FileScanTask tooBigTask = new MockFileScanTask(1000L);
    List<FileScanTask> tasks = ImmutableList.of(tooSmallTask, optimal, tooBigTask);

    Map<String, String> options =
        ImmutableMap.of(
            RewriteFileGroupPlanner.MIN_FILE_SIZE_BYTES, "250",
            RewriteFileGroupPlanner.TARGET_FILE_SIZE_BYTES, "500",
            RewriteFileGroupPlanner.MAX_FILE_SIZE_BYTES, "750",
            RewriteFileGroupPlanner.DELETE_FILE_THRESHOLD, String.valueOf(Integer.MAX_VALUE));
    rewriter.init(options);

    Iterable<List<FileScanTask>> groups = rewriter.planFileGroups(tasks);
    assertThat(groups).as("Must have 1 group").hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).as("Must rewrite 2 files").hasSize(2);
  }

  private void checkDataFilesDeleteThreshold(RewriteFileGroupPlanner rewriter) {
    FileScanTask tooManyDeletesTask = MockFileScanTask.mockTaskWithDeletes(1000L, 3);
    FileScanTask optimalTask = MockFileScanTask.mockTaskWithDeletes(1000L, 1);
    List<FileScanTask> tasks = ImmutableList.of(tooManyDeletesTask, optimalTask);

    Map<String, String> options =
        ImmutableMap.of(
            RewriteFileGroupPlanner.MIN_FILE_SIZE_BYTES, "1",
            RewriteFileGroupPlanner.TARGET_FILE_SIZE_BYTES, "2000",
            RewriteFileGroupPlanner.MAX_FILE_SIZE_BYTES, "5000",
            RewriteFileGroupPlanner.DELETE_FILE_THRESHOLD, "2");
    rewriter.init(options);

    Iterable<List<FileScanTask>> groups = rewriter.planFileGroups(tasks);
    assertThat(groups).as("Must have 1 group").hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).as("Must rewrite 1 file").hasSize(1);
  }

  private void checkDataFileGroupWithEnoughFiles(RewriteFileGroupPlanner rewriter) {
    List<FileScanTask> tasks =
        ImmutableList.of(
            new MockFileScanTask(100L),
            new MockFileScanTask(100L),
            new MockFileScanTask(100L),
            new MockFileScanTask(100L));

    Map<String, String> options =
        ImmutableMap.of(
            RewriteFileGroupPlanner.MIN_INPUT_FILES, "3",
            RewriteFileGroupPlanner.MIN_FILE_SIZE_BYTES, "150",
            RewriteFileGroupPlanner.TARGET_FILE_SIZE_BYTES, "1000",
            RewriteFileGroupPlanner.MAX_FILE_SIZE_BYTES, "5000",
            RewriteFileGroupPlanner.DELETE_FILE_THRESHOLD, String.valueOf(Integer.MAX_VALUE));
    rewriter.init(options);

    Iterable<List<FileScanTask>> groups = rewriter.planFileGroups(tasks);
    assertThat(groups).as("Must have 1 group").hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).as("Must rewrite 4 files").hasSize(4);
  }

  private void checkDataFileGroupWithEnoughData(RewriteFileGroupPlanner rewriter) {
    List<FileScanTask> tasks =
        ImmutableList.of(
            new MockFileScanTask(100L), new MockFileScanTask(100L), new MockFileScanTask(100L));

    Map<String, String> options =
        ImmutableMap.of(
            RewriteFileGroupPlanner.MIN_INPUT_FILES, "5",
            RewriteFileGroupPlanner.MIN_FILE_SIZE_BYTES, "200",
            RewriteFileGroupPlanner.TARGET_FILE_SIZE_BYTES, "250",
            RewriteFileGroupPlanner.MAX_FILE_SIZE_BYTES, "500",
            RewriteFileGroupPlanner.DELETE_FILE_THRESHOLD, String.valueOf(Integer.MAX_VALUE));
    rewriter.init(options);

    Iterable<List<FileScanTask>> groups = rewriter.planFileGroups(tasks);
    assertThat(groups).as("Must have 1 group").hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).as("Must rewrite 3 files").hasSize(3);
  }

  private void checkDataFileGroupWithTooMuchData(RewriteFileGroupPlanner rewriter) {
    List<FileScanTask> tasks = ImmutableList.of(new MockFileScanTask(2000L));

    Map<String, String> options =
        ImmutableMap.of(
            RewriteFileGroupPlanner.MIN_INPUT_FILES, "5",
            RewriteFileGroupPlanner.MIN_FILE_SIZE_BYTES, "200",
            RewriteFileGroupPlanner.TARGET_FILE_SIZE_BYTES, "250",
            RewriteFileGroupPlanner.MAX_FILE_SIZE_BYTES, "500",
            RewriteFileGroupPlanner.DELETE_FILE_THRESHOLD, String.valueOf(Integer.MAX_VALUE));
    rewriter.init(options);

    Iterable<List<FileScanTask>> groups = rewriter.planFileGroups(tasks);
    assertThat(groups).as("Must have 1 group").hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).as("Must rewrite big file").hasSize(1);
  }

  @Test
  public void testInvalidConstructorUsagesSortData() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);

    assertThatThrownBy(() -> new SparkSortDataRewriteExecutor(spark, table))
        .hasMessageContaining("Cannot sort data without a valid sort order")
        .hasMessageContaining("is unsorted and no sort order is provided");

    assertThatThrownBy(() -> new SparkSortDataRewriteExecutor(spark, table, null))
        .hasMessageContaining("Cannot sort data without a valid sort order")
        .hasMessageContaining("the provided sort order is null or empty");

    assertThatThrownBy(() -> new SparkSortDataRewriteExecutor(spark, table, SortOrder.unsorted()))
        .hasMessageContaining("Cannot sort data without a valid sort order")
        .hasMessageContaining("the provided sort order is null or empty");
  }

  @Test
  public void testInvalidConstructorUsagesZOrderData() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA, SPEC);

    assertThatThrownBy(() -> new SparkZOrderDataRewriteExecutor(spark, table, null))
        .hasMessageContaining("Cannot ZOrder when no columns are specified");

    assertThatThrownBy(() -> new SparkZOrderDataRewriteExecutor(spark, table, ImmutableList.of()))
        .hasMessageContaining("Cannot ZOrder when no columns are specified");

    assertThatThrownBy(
            () -> new SparkZOrderDataRewriteExecutor(spark, table, ImmutableList.of("dep")))
        .hasMessageContaining("Cannot ZOrder")
        .hasMessageContaining("all columns provided were identity partition columns");

    assertThatThrownBy(
            () -> new SparkZOrderDataRewriteExecutor(spark, table, ImmutableList.of("DeP")))
        .hasMessageContaining("Cannot ZOrder")
        .hasMessageContaining("all columns provided were identity partition columns");
  }

  @Test
  public void testBinPackDataValidOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkBinPackDataRewriteExecutor rewriter = new SparkBinPackDataRewriteExecutor(spark, table);
    RewriteFileGroupPlanner planner =
        new RewriteFileGroupPlanner(table, Expressions.alwaysTrue(), 1, false);

    assertThat(rewriter.validOptions())
        .as("Rewriter must report all supported options")
        .isEqualTo(ImmutableSet.of());

    assertThat(planner.validOptions())
        .as("Planner must report all supported options")
        .isEqualTo(
            ImmutableSet.of(
                RewriteFileGroupPlanner.TARGET_FILE_SIZE_BYTES,
                RewriteFileGroupPlanner.MIN_FILE_SIZE_BYTES,
                RewriteFileGroupPlanner.MAX_FILE_SIZE_BYTES,
                RewriteFileGroupPlanner.MIN_INPUT_FILES,
                RewriteFileGroupPlanner.REWRITE_ALL,
                RewriteFileGroupPlanner.MAX_FILE_GROUP_SIZE_BYTES,
                RewriteFileGroupPlanner.DELETE_FILE_THRESHOLD,
                RewriteDataFiles.REWRITE_JOB_ORDER));
  }

  @Test
  public void testSortDataValidOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkSortDataRewriteExecutor rewriter =
        new SparkSortDataRewriteExecutor(spark, table, SORT_ORDER);
    RewriteFileGroupPlanner planner =
        new RewriteFileGroupPlanner(table, Expressions.alwaysTrue(), 1, false);

    assertThat(rewriter.validOptions())
        .as("Rewriter must report all supported options")
        .isEqualTo(
            ImmutableSet.of(
                SparkSortDataRewriteExecutor.SHUFFLE_PARTITIONS_PER_FILE,
                SparkSortDataRewriteExecutor.COMPRESSION_FACTOR));

    assertThat(planner.validOptions())
        .as("Planner must report all supported options")
        .isEqualTo(
            ImmutableSet.of(
                RewriteFileGroupPlanner.TARGET_FILE_SIZE_BYTES,
                RewriteFileGroupPlanner.MIN_FILE_SIZE_BYTES,
                RewriteFileGroupPlanner.MAX_FILE_SIZE_BYTES,
                RewriteFileGroupPlanner.MIN_INPUT_FILES,
                RewriteFileGroupPlanner.REWRITE_ALL,
                RewriteFileGroupPlanner.MAX_FILE_GROUP_SIZE_BYTES,
                RewriteFileGroupPlanner.DELETE_FILE_THRESHOLD,
                RewriteDataFiles.REWRITE_JOB_ORDER));
  }

  @Test
  public void testZOrderDataValidOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    ImmutableList<String> zOrderCols = ImmutableList.of("id");
    SparkZOrderDataRewriteExecutor rewriter =
        new SparkZOrderDataRewriteExecutor(spark, table, zOrderCols);
    RewriteFileGroupPlanner planner =
        new RewriteFileGroupPlanner(table, Expressions.alwaysTrue(), 1, false);

    assertThat(rewriter.validOptions())
        .as("Rewriter must report all supported options")
        .isEqualTo(
            ImmutableSet.of(
                SparkZOrderDataRewriteExecutor.SHUFFLE_PARTITIONS_PER_FILE,
                SparkZOrderDataRewriteExecutor.COMPRESSION_FACTOR,
                SparkZOrderDataRewriteExecutor.MAX_OUTPUT_SIZE,
                SparkZOrderDataRewriteExecutor.VAR_LENGTH_CONTRIBUTION));
    assertThat(planner.validOptions())
        .as("Planner must report all supported options")
        .isEqualTo(
            ImmutableSet.of(
                RewriteFileGroupPlanner.TARGET_FILE_SIZE_BYTES,
                RewriteFileGroupPlanner.MIN_FILE_SIZE_BYTES,
                RewriteFileGroupPlanner.MAX_FILE_SIZE_BYTES,
                RewriteFileGroupPlanner.MIN_INPUT_FILES,
                RewriteFileGroupPlanner.REWRITE_ALL,
                RewriteFileGroupPlanner.MAX_FILE_GROUP_SIZE_BYTES,
                RewriteFileGroupPlanner.DELETE_FILE_THRESHOLD,
                RewriteDataFiles.REWRITE_JOB_ORDER));
  }

  @Test
  public void testInvalidValuesForBinPackDataOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    RewriteFileGroupPlanner planner =
        new RewriteFileGroupPlanner(table, Expressions.alwaysTrue(), 1, false);

    validateSizeBasedRewriterOptions(planner);

    Map<String, String> invalidDeleteThresholdOptions =
        ImmutableMap.of(RewriteFileGroupPlanner.DELETE_FILE_THRESHOLD, "-1");
    assertThatThrownBy(() -> planner.init(invalidDeleteThresholdOptions))
        .hasMessageContaining("'delete-file-threshold' is set to -1 but must be >= 0");
  }

  @Test
  public void testInvalidValuesForSortDataOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkSortDataRewriteExecutor rewriter =
        new SparkSortDataRewriteExecutor(spark, table, SORT_ORDER);
    RewriteFileGroupPlanner planner =
        new RewriteFileGroupPlanner(table, Expressions.alwaysTrue(), 1, false);

    validateSizeBasedRewriterOptions(planner);

    Map<String, String> invalidDeleteThresholdOptions =
        ImmutableMap.of(RewriteFileGroupPlanner.DELETE_FILE_THRESHOLD, "-1");
    assertThatThrownBy(() -> planner.init(invalidDeleteThresholdOptions))
        .hasMessageContaining("'delete-file-threshold' is set to -1 but must be >= 0");

    Map<String, String> invalidCompressionFactorOptions =
        ImmutableMap.of(SparkShufflingDataRewriteExecutor.COMPRESSION_FACTOR, "0");
    assertThatThrownBy(() -> rewriter.init(invalidCompressionFactorOptions))
        .hasMessageContaining("'compression-factor' is set to 0.0 but must be > 0");
  }

  @Test
  public void testInvalidValuesForZOrderDataOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    ImmutableList<String> zOrderCols = ImmutableList.of("id");
    SparkZOrderDataRewriteExecutor rewriter =
        new SparkZOrderDataRewriteExecutor(spark, table, zOrderCols);
    RewriteFileGroupPlanner planner =
        new RewriteFileGroupPlanner(table, Expressions.alwaysTrue(), 1, false);

    validateSizeBasedRewriterOptions(planner);

    Map<String, String> invalidDeleteThresholdOptions =
        ImmutableMap.of(RewriteFileGroupPlanner.DELETE_FILE_THRESHOLD, "-1");
    assertThatThrownBy(() -> planner.init(invalidDeleteThresholdOptions))
        .hasMessageContaining("'delete-file-threshold' is set to -1 but must be >= 0");

    Map<String, String> invalidCompressionFactorOptions =
        ImmutableMap.of(SparkShufflingDataRewriteExecutor.COMPRESSION_FACTOR, "0");
    assertThatThrownBy(() -> rewriter.init(invalidCompressionFactorOptions))
        .hasMessageContaining("'compression-factor' is set to 0.0 but must be > 0");

    Map<String, String> invalidMaxOutputOptions =
        ImmutableMap.of(SparkZOrderDataRewriteExecutor.MAX_OUTPUT_SIZE, "0");
    assertThatThrownBy(() -> rewriter.init(invalidMaxOutputOptions))
        .hasMessageContaining("Cannot have the interleaved ZOrder value use less than 1 byte")
        .hasMessageContaining("'max-output-size' was set to 0");

    Map<String, String> invalidVarLengthContributionOptions =
        ImmutableMap.of(SparkZOrderDataRewriteExecutor.VAR_LENGTH_CONTRIBUTION, "0");
    assertThatThrownBy(() -> rewriter.init(invalidVarLengthContributionOptions))
        .hasMessageContaining("Cannot use less than 1 byte for variable length types with ZOrder")
        .hasMessageContaining("'var-length-contribution' was set to 0");
  }

  private void validateSizeBasedRewriterOptions(SizeBasedFileRewritePlanner<?, ?, ?, ?> rewriter) {
    Map<String, String> invalidTargetSizeOptions =
        ImmutableMap.of(SizeBasedFileRewritePlanner.TARGET_FILE_SIZE_BYTES, "0");
    assertThatThrownBy(() -> rewriter.init(invalidTargetSizeOptions))
        .hasMessageContaining("'target-file-size-bytes' is set to 0 but must be > 0");

    Map<String, String> invalidMinSizeOptions =
        ImmutableMap.of(SizeBasedFileRewritePlanner.MIN_FILE_SIZE_BYTES, "-1");
    assertThatThrownBy(() -> rewriter.init(invalidMinSizeOptions))
        .hasMessageContaining("'min-file-size-bytes' is set to -1 but must be >= 0");

    Map<String, String> invalidTargetMinSizeOptions =
        ImmutableMap.of(
            SizeBasedFileRewritePlanner.TARGET_FILE_SIZE_BYTES, "3",
            SizeBasedFileRewritePlanner.MIN_FILE_SIZE_BYTES, "5");
    assertThatThrownBy(() -> rewriter.init(invalidTargetMinSizeOptions))
        .hasMessageContaining("'target-file-size-bytes' (3) must be > 'min-file-size-bytes' (5)")
        .hasMessageContaining("all new files will be smaller than the min threshold");

    Map<String, String> invalidTargetMaxSizeOptions =
        ImmutableMap.of(
            SizeBasedFileRewritePlanner.TARGET_FILE_SIZE_BYTES, "5",
            SizeBasedFileRewritePlanner.MAX_FILE_SIZE_BYTES, "3");
    assertThatThrownBy(() -> rewriter.init(invalidTargetMaxSizeOptions))
        .hasMessageContaining("'target-file-size-bytes' (5) must be < 'max-file-size-bytes' (3)")
        .hasMessageContaining("all new files will be larger than the max threshold");

    Map<String, String> invalidMinInputFilesOptions =
        ImmutableMap.of(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "0");
    assertThatThrownBy(() -> rewriter.init(invalidMinInputFilesOptions))
        .hasMessageContaining("'min-input-files' is set to 0 but must be > 0");

    Map<String, String> invalidMaxFileGroupSizeOptions =
        ImmutableMap.of(SizeBasedFileRewritePlanner.MAX_FILE_GROUP_SIZE_BYTES, "0");
    assertThatThrownBy(() -> rewriter.init(invalidMaxFileGroupSizeOptions))
        .hasMessageContaining("'max-file-group-size-bytes' is set to 0 but must be > 0");
  }
}
