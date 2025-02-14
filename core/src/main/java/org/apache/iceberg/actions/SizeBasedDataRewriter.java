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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class SizeBasedDataRewriter extends SizeBasedFileRewriter<FileScanTask, DataFile> {
    private static final Logger LOG = LoggerFactory.getLogger(SizeBasedDataRewriter.class);

  /**
   * The minimum number of deletes that needs to be associated with a data file for it to be
   * considered for rewriting. If a data file has this number of deletes or more, it will be
   * rewritten regardless of its file size determined by {@link #MIN_FILE_SIZE_BYTES} and {@link
   * #MAX_FILE_SIZE_BYTES}. If a file group contains a file that satisfies this condition, the file
   * group will be rewritten regardless of the number of files in the file group determined by
   * {@link #MIN_INPUT_FILES}.
   *
   * <p>Defaults to Integer.MAX_VALUE, which means this feature is not enabled by default.
   */
  public static final String DELETE_FILE_THRESHOLD = "delete-file-threshold";

  public static final int DELETE_FILE_THRESHOLD_DEFAULT = Integer.MAX_VALUE;

  private int deleteFileThreshold;


  /**
 * The threshold for the minimum number of deleted rows that a data file must have to be considered for rewriting.
 * If a data file has a row count greater than or equal to the specified threshold, the file will be considered
 * for rewriting regardless of other size or delete-based criteria.
 *
 * <p>This property allows for controlling the rewrite of files based on deleted row count, which can be useful when
 * the size of a file isn't the sole criterion, and large files with excessive rows should also be rewritten
 * for performance reasons or other optimization goals.
 *
 * <p>Defaults to 0, meaning this feature is disabled unless a specific threshold is set.
 */
  public static final String DELETED_ROW_COUNT_THRESHOLD = "deleted-row-count-threshold";

  public static final int DELETED_ROW_COUNT_THRESHOLD_DEFAULT = Integer.MAX_VALUE;

  private int deletedRowCountThreshold;


  protected SizeBasedDataRewriter(Table table) {
    super(table);
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(DELETE_FILE_THRESHOLD)
        .add(DELETED_ROW_COUNT_THRESHOLD)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.deleteFileThreshold = deleteFileThreshold(options);
    this.deletedRowCountThreshold = deletedRowCountThreshold(options);
  }

  @Override
  protected Iterable<FileScanTask> filterFiles(Iterable<FileScanTask> tasks) {
    return Iterables.filter(tasks, task -> wronglySized(task)
        || tooManyDeletes(task)
        || tooManyDeletedRows(task));
  }

  private boolean tooManyDeletedRows(FileScanTask task) {
      long positionDeletes = task.positionDeleteCount();
      long equalityDeletes = task.equalityDeleteCount();

      // Equality deletes are 10x more resource-intensive
      long totalWeightedDeletes = positionDeletes + (equalityDeletes * 10);
      boolean exceedsThreshold = totalWeightedDeletes >= deletedRowCountThreshold;

      LOG.debug("Task delete counts: file={}, position_deletes={}, equality_deletes={}, weighted_total={}, threshold={}, exceeds={}",
          task.file().path(), positionDeletes, equalityDeletes, totalWeightedDeletes, deletedRowCountThreshold, exceedsThreshold);

      return exceedsThreshold;
  }

  private boolean tooManyDeletes(FileScanTask task) {
    return task.deletes() != null && task.deletes().size() >= deleteFileThreshold;
  }

  @Override
  protected Iterable<List<FileScanTask>> filterFileGroups(List<List<FileScanTask>> groups) {
    return Iterables.filter(groups, this::shouldRewrite);
  }

  private boolean shouldRewrite(List<FileScanTask> group) {
    return enoughInputFiles(group)
        || enoughContent(group)
        || tooMuchContent(group)
        || anyTaskHasTooManyDeletes(group)
        || combinedTasksHasTooManyDeletedRows(group);
  }

  private boolean combinedTasksHasTooManyDeletedRows(List<FileScanTask> group) {
      long totalPositionDeletes = group.stream()
          .mapToLong(FileScanTask::positionDeleteCount)
          .sum();

      long totalEqualityDeletes = group.stream()
          .mapToLong(FileScanTask::equalityDeleteCount)
          .sum();

      // Equality deletes are 10x more resource-intensive
      long totalWeightedDeletes = totalPositionDeletes + (totalEqualityDeletes * 10);
      boolean exceedsThreshold = totalWeightedDeletes >= deletedRowCountThreshold;

      LOG.debug("Group delete counts: position_deletes={}, equality_deletes={}, weighted_total={}, threshold={}, exceeds={}",
          totalPositionDeletes, totalEqualityDeletes, totalWeightedDeletes, deletedRowCountThreshold, exceedsThreshold);

      return exceedsThreshold;
  }

  private boolean anyTaskHasTooManyDeletes(List<FileScanTask> group) {
    return group.stream().anyMatch(this::tooManyDeletes);
  }

  @Override
  protected long defaultTargetFileSize() {
    return PropertyUtil.propertyAsLong(
        table().properties(),
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
  }

  private int deleteFileThreshold(Map<String, String> options) {
    int value =
        PropertyUtil.propertyAsInt(options, DELETE_FILE_THRESHOLD, DELETE_FILE_THRESHOLD_DEFAULT);
    Preconditions.checkArgument(
        value >= 0, "'%s' is set to %s but must be >= 0", DELETE_FILE_THRESHOLD, value);
    return value;
  }

  private int deletedRowCountThreshold(Map<String, String> options) {
    int value =
        PropertyUtil.propertyAsInt(options, DELETED_ROW_COUNT_THRESHOLD, DELETED_ROW_COUNT_THRESHOLD_DEFAULT);
    Preconditions.checkArgument(
        value >= 0, "'%s' is set to %s but must be >= 0", DELETED_ROW_COUNT_THRESHOLD, value);
    return value;
  }
}
