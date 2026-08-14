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
import java.util.Set;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SortOrderStatsHandler;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;

/**
 * Produces plans for shuffling rewrites. Since shuffle and sort could considerably improve the
 * compression ratio, the planner introduces an additional {@link #COMPRESSION_FACTOR} option which
 * is used when calculating the {@link #expectedOutputFiles(long)}.
 */
class SparkShufflingDataRewritePlanner extends BinPackRewriteFilePlanner {
  /**
   * The number of shuffle partitions and consequently the number of output files created by the
   * Spark sort is based on the size of the input data files used in this file rewriter. Due to
   * compression, the disk file sizes may not accurately represent the size of files in the output.
   * This parameter lets the user adjust the file size used for estimating actual output data size.
   * A factor greater than 1.0 would generate more files than we would expect based on the on-disk
   * file size. A value less than 1.0 would create fewer files than we would expect based on the
   * on-disk size.
   */
  public static final String COMPRESSION_FACTOR = "compression-factor";

  public static final double COMPRESSION_FACTOR_DEFAULT = 1.0;

  /**
   * The overlap depth at which files are selected for rewrite regardless of their size.
   *
   * <p>Size based selection cannot detect files that are large enough but whose sort key ranges sit
   * on top of each other, so a sorted rewrite may report success without improving clustering. When
   * this option is set, files that intersect a region of the table sort key covered by at least
   * this many files of the same partition are added to the rewrite, on top of the files selected by
   * size and by deletes.
   *
   * <p>The overlap is measured on the table sort order using data file bounds only, as reported by
   * the {@code compute_sort_order_stats} procedure. Because column bounds may be truncated, the
   * measured depth is an upper-bound estimate. Unset by default, which leaves selection unchanged.
   */
  public static final String MIN_OVERLAP_DEPTH = "min-overlap-depth";

  private double compressionFactor;
  private Integer minOverlapDepth;
  private Set<String> overlappingFiles = ImmutableSet.of();

  SparkShufflingDataRewritePlanner(
      Table table, Expression filter, Long snapshotId, boolean caseSensitive) {
    super(table, filter, snapshotId, caseSensitive);
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(COMPRESSION_FACTOR)
        .add(MIN_OVERLAP_DEPTH)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.compressionFactor = compressionFactor(options);
    this.minOverlapDepth = minOverlapDepth(options);
  }

  @Override
  protected Set<String> columnsToIncludeStats() {
    if (minOverlapDepth == null || !table().sortOrder().isSorted()) {
      // an unsorted table fails with a clear message when the overlap is computed
      return ImmutableSet.of();
    }

    int sourceId = table().sortOrder().fields().get(0).sourceId();
    return ImmutableSet.of(table().schema().findColumnName(sourceId));
  }

  @Override
  protected int expectedOutputFiles(long inputSize) {
    return Math.max(1, super.expectedOutputFiles((long) (inputSize * compressionFactor)));
  }

  /**
   * Adds the files sitting in overlapping regions of the sort key to those selected by size and by
   * deletes. The overlapping files are remembered so that {@link #filterFileGroups(List)} can keep
   * the groups holding them, which planning always calls after this method.
   */
  @Override
  protected Iterable<FileScanTask> filterFiles(Iterable<FileScanTask> tasks) {
    if (minOverlapDepth == null) {
      return super.filterFiles(tasks);
    }

    List<FileScanTask> allTasks = Lists.newArrayList(tasks);
    this.overlappingFiles =
        SortOrderStatsHandler.highOverlapFiles(
            Lists.transform(allTasks, FileScanTask::file),
            table().specs(),
            table().sortOrder(),
            table().name(),
            minOverlapDepth);

    Set<String> selected = Sets.newHashSet(overlappingFiles);
    for (FileScanTask task : super.filterFiles(allTasks)) {
      selected.add(task.file().location());
    }

    return Iterables.filter(allTasks, task -> selected.contains(task.file().location()));
  }

  @Override
  protected Iterable<List<FileScanTask>> filterFileGroups(List<List<FileScanTask>> groups) {
    if (minOverlapDepth == null) {
      return super.filterFileGroups(groups);
    }

    Set<List<FileScanTask>> kept = Sets.newIdentityHashSet();
    Iterables.addAll(kept, super.filterFileGroups(groups));

    return Iterables.filter(
        groups, group -> kept.contains(group) || group.stream().anyMatch(this::isOverlapping));
  }

  private boolean isOverlapping(FileScanTask task) {
    return overlappingFiles.contains(task.file().location());
  }

  private double compressionFactor(Map<String, String> options) {
    double value =
        PropertyUtil.propertyAsDouble(options, COMPRESSION_FACTOR, COMPRESSION_FACTOR_DEFAULT);
    Preconditions.checkArgument(
        value > 0, "'%s' is set to %s but must be > 0", COMPRESSION_FACTOR, value);
    return value;
  }

  private Integer minOverlapDepth(Map<String, String> options) {
    Integer value = PropertyUtil.propertyAsNullableInt(options, MIN_OVERLAP_DEPTH);
    Preconditions.checkArgument(
        value == null || value > 1, "'%s' is set to %s but must be > 1", MIN_OVERLAP_DEPTH, value);
    return value;
  }
}
