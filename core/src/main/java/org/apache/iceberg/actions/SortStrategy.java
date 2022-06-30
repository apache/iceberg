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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.BinPacking;
import org.apache.iceberg.util.BinPacking.ListPacker;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SortStrategyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A rewrite strategy for data files which aims to reorder data with data files to optimally lay them out
 * in relation to a column. For example, if the Sort strategy is used on a set of files which is ordered
 * by column x and original has files File A (x: 0 - 50), File B ( x: 10 - 40), File C ( x: 30 - 60),
 * File D ( x: 61 - 80), and File E ( x: 81 - 100), this Strategy will attempt to rewrite files A, B and C
 * into File A' (x: 0-20), File B' (x: 21 - 40), File C' (x: 41 - 60) and keep File D and E unchanged.
 * <p>
 * When the {@link BinPackStrategy#REWRITE_ALL} flag is, all files are selected for rewrite. Otherwise,
 * only unsorted files are selected. Rewrite will be applied to those selected files if
 * 1. There are a certain number of mis-sized data files or
 * 2. Those data files do not have sortedness score good enough.
 * <p>
 */
public abstract class SortStrategy extends BinPackStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(SortStrategy.class);
  /**
   * Rewrites if the ratio of mis-sized files to total files is over this threshold.
   * The value should be between 0.0 and 1.0
   */
  public static final String MIS_SIZED_RATIO_THRESHOLD = "mis-sized-ratio-threshold";
  public static final double MIS_SIZED_RATIO_THRESHOLD_DEFAULT = 0.05;

  /**
   * Rewrites if the sortedness score of given files is below this threshold.
   * The value should be between 0.0 and 1.0
   */
  public static final String SORTEDNESS_SCORE_THRESHOLD = "sortedness-score-threshold";
  public static final double SORTEDNESS_SCORE__THRESHOLD_DEFAULT = 0.95;

  private double misSizedRatioThreshold;
  private double sortednessScoreThreshold;
  private SortOrder sortOrder;

  /**
   * Sets the sort order to be used in this strategy when rewriting files
   * @param order the order to use
   * @return this for method chaining
   */
  public SortStrategy sortOrder(SortOrder order) {
    this.sortOrder = order;
    return this;
  }

  protected SortOrder sortOrder() {
    return sortOrder;
  }

  @Override
  public String name() {
    return "SORT";
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .build();
  }

  @Override
  public RewriteStrategy options(Map<String, String> options) {
    super.options(options); // Also checks validity of BinPack options

    misSizedRatioThreshold = PropertyUtil.propertyAsDouble(options,
        MIS_SIZED_RATIO_THRESHOLD,
        MIS_SIZED_RATIO_THRESHOLD_DEFAULT);

    sortednessScoreThreshold = PropertyUtil.propertyAsDouble(options,
        SORTEDNESS_SCORE_THRESHOLD,
        SORTEDNESS_SCORE__THRESHOLD_DEFAULT);

    if (sortOrder == null) {
      sortOrder = table().sortOrder();
    }

    validateOptions();
    return this;
  }

  @Override
  public Iterable<FileScanTask> selectFilesToRewrite(Iterable<FileScanTask> dataFiles) {
    if (rewriteAll()) {
      LOG.info("Table {} set to rewrite all data files", table().name());
      return dataFiles;
    } else {
      // Remove files that are completely sorted.
      // Example: File_A(1, 10), File_B(11, 25), File_C(15, 30), File_D(31, 40)
      // Then only File_B and File_C are selected
      Iterable<FileScanTask> selectedFiles = SortStrategyUtil.removeSortedFiles(dataFiles, sortOrder);
      if (!haveGoodFileSizes(selectedFiles) || !areFilesSorted(selectedFiles)) {
        // Rewrite all selected files if they are mis-sized or have bad sortedness score
        return selectedFiles;
      }
      return ImmutableList.of();
    }
  }

  @Override
  public Iterable<List<FileScanTask>> planFileGroups(Iterable<FileScanTask> dataFiles) {
    ListPacker<FileScanTask> packer = new BinPacking.ListPacker<>(maxGroupSize(), 1, false);
    return packer.pack(dataFiles, FileScanTask::length);
  }

  protected void validateOptions() {
    Preconditions.checkArgument(misSizedRatioThreshold >= 0 && misSizedRatioThreshold <= 1,
        "mis-sized-ratio-threshold value must be between 0.0 and 1.0");

    Preconditions.checkArgument(sortednessScoreThreshold >= 0 && sortednessScoreThreshold <= 1,
        "sortedness-score-threshold value must be between 0.0 and 1.0");

    Preconditions.checkArgument(!sortOrder.isUnsorted(),
        "Can't use %s when there is no sort order, either define table %s's sort order or set sort" +
            "order in the action",
        name(), table().name());

    SortOrder.checkCompatibility(sortOrder, table().schema());
  }

  boolean haveGoodFileSizes(Iterable<FileScanTask> dataFiles) {
    int totalDataFiles = Iterables.size(dataFiles);
    int misSizedDataFiles = Iterables.size(super.selectFilesToRewrite(dataFiles));
    return (0.0 + misSizedDataFiles) / totalDataFiles <= misSizedRatioThreshold;
  }

  boolean areFilesSorted(Iterable<FileScanTask> dataFiles) {
    List<DataFile> files = FluentIterable.from(dataFiles).stream()
        .map(FileScanTask::file)
        .collect(Collectors.toList());
    return SortStrategyUtil.sortednessScore(files, sortOrder) >= sortednessScoreThreshold;
  }
}
