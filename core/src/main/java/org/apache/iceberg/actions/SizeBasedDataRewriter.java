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
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.PropertyUtil;

/**
 * Deprecated {@link SizeBasedDataRewriter} abstract class.
 *
 * @deprecated since 1.9.0, will be removed in 1.10.0; use {@link BinPackRewriteFilePlanner} and
 *     {@link FileRewriteRunner}
 */
@Deprecated
public abstract class SizeBasedDataRewriter extends SizeBasedFileRewriter<FileScanTask, DataFile> {

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

  /**
   * The ratio of the deleted rows in a data file for it to be considered for rewriting. If the
   * deletion ratio of a data file is greater than or equal to this value, it will be rewritten
   * regardless of its file size determined by {@link #MIN_FILE_SIZE_BYTES} and {@link
   * #MAX_FILE_SIZE_BYTES}. If a file group contains a file that satisfies this condition, the file
   * group will be rewritten regardless of the number of files in the file group determined by
   * {@link #MIN_INPUT_FILES}.
   *
   * <p>Defaults to 0.3, which means that if the number of deleted records in a file reaches or
   * exceeds 30%, it will trigger the rewriting operation.
   */
  public static final String DELETE_RATIO_THRESHOLD = "delete-ratio-threshold";

  public static final double DELETE_RATIO_THRESHOLD_DEFAULT = 0.3;

  private int deleteFileThreshold;

  private double deleteRatioThreshold;

  protected SizeBasedDataRewriter(Table table) {
    super(table);
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(DELETE_FILE_THRESHOLD)
        .add(DELETE_RATIO_THRESHOLD)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.deleteFileThreshold = deleteFileThreshold(options);
    this.deleteRatioThreshold = deleteRatioThreshold(options);
  }

  private double deleteRatioThreshold(Map<String, String> options) {
    double value =
        PropertyUtil.propertyAsDouble(
            options, DELETE_RATIO_THRESHOLD, DELETE_RATIO_THRESHOLD_DEFAULT);
    Preconditions.checkArgument(
        value > 0, "'%s' is set to %s but must be > 0", DELETE_RATIO_THRESHOLD, value);
    Preconditions.checkArgument(
        value <= 1, "'%s' is set to %s but must be <= 1", DELETE_RATIO_THRESHOLD, value);
    return value;
  }

  @Override
  protected Iterable<FileScanTask> filterFiles(Iterable<FileScanTask> tasks) {
    return Iterables.filter(tasks, this::shouldRewrite);
  }

  private boolean shouldRewrite(FileScanTask task) {
    return wronglySized(task) || tooManyDeletes(task) || tooHighDeleteRatio(task);
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
        || anyTaskHasTooHighDeleteRatio(group);
  }

  private boolean anyTaskHasTooManyDeletes(List<FileScanTask> group) {
    return group.stream().anyMatch(this::tooManyDeletes);
  }

  private boolean anyTaskHasTooHighDeleteRatio(List<FileScanTask> group) {
    return group.stream().anyMatch(this::tooHighDeleteRatio);
  }

  private boolean tooHighDeleteRatio(FileScanTask task) {
    if (task.deletes() == null || task.deletes().isEmpty()) {
      return false;
    }

    long knownDeletedRecordCount =
        task.deletes().stream()
            .filter(ContentFileUtil::isFileScoped)
            .mapToLong(ContentFile::recordCount)
            .sum();

    double deletedRecords = (double) Math.min(knownDeletedRecordCount, task.file().recordCount());
    double deleteRatio = deletedRecords / task.file().recordCount();
    return deleteRatio >= deleteRatioThreshold;
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
}
