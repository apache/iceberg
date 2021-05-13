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
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteDataFiles.Strategy;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.util.BinPacking;
import org.apache.iceberg.util.BinPacking.ListPacker;
import org.apache.iceberg.util.PropertyUtil;

/**
 * A rewrite strategy for data files which determines which files to rewrite
 * based on their size. If files are either smaller than the {@link MIN_FILE_SIZE_BYTES} threshold or
 * larger than the {@link MAX_FILE_SIZE_BYTES} threshold, they are considered targets for being rewritten.
 * <p>
 * Once selected files are grouped based on a {@link BinPacking} into groups defined
 * by {@link RewriteDataFiles#MAX_FILE_GROUP_SIZE_BYTES}. Groups will be considered for rewriting if they contain
 * more files than {@link MIN_INPUT_FILES} and would produce more files than {@link MIN_OUTPUT_FILES}.
 */
abstract class BinPackStrategy implements RewriteStrategy {

  /**
   * Minimum number of files that need to be in a file group to be considered
   * for rewriting. This is considered in conjunction with {@link MIN_OUTPUT_FILES}, both
   * conditions must pass to consider a group of files to be rewritten.
   */
  public static final String MIN_INPUT_FILES = "min-input-files";
  public static final int MIN_INPUT_FILES_DEFAULT = 1;

  /**
   * Minimum number of files we want to be created by file group when being
   * rewritten. This is considered in conjunction with {@link MIN_INPUT_FILES}, both
   * conditions must pass to consider a group of files to be rewritten.
   */
  public static final String MIN_OUTPUT_FILES = "min-output-files";
  public static final int MIN_OUTPUT_FILES_DEFAULT = 1;

  /**
   * Adjusts files which will be considered for rewriting. Files smaller than
   * {@link MIN_FILE_SIZE_BYTES} will be considered for rewriting. This functions independently
   * of {@link MAX_FILE_SIZE_BYTES}.
   * <p>
   * Defaults to 75% of the target file size
   */
  public static final String MIN_FILE_SIZE_BYTES = "min-file-size-bytes";
  public static final double MIN_FILE_SIZE_DEFAULT_RATIO = 0.75d;

  /**
   * Adjusts files which will be considered for rewriting. Files larger than
   * {@link MAX_FILE_SIZE_BYTES} will be considered for rewriting. This functions independently
   * of {@link MIN_FILE_SIZE_BYTES}.
   * <p>
   * Defaults to 180% of the target file size
   */
  public static final String MAX_FILE_SIZE_BYTES = "max-file-size-bytes";
  public static final double MAX_FILE_SIZE_DEFAULT_RATIO = 1.80d;

  private int minInputFiles;
  private int minOutputFiles;
  private long minFileSize;
  private long maxFileSize;
  private long targetFileSize;
  private long maxGroupSize;

  @Override
  public String name() {
    return Strategy.BINPACK.name();
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.of(
        MIN_INPUT_FILES,
        MIN_OUTPUT_FILES,
        MIN_FILE_SIZE_BYTES,
        MAX_FILE_SIZE_BYTES
    );
  }

  @Override
  public RewriteStrategy options(Map<String, String> options) {
    targetFileSize = PropertyUtil.propertyAsLong(options,
        RewriteDataFiles.TARGET_FILE_SIZE_BYTES,
        PropertyUtil.propertyAsLong(
            table().properties(),
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT));

    minFileSize = PropertyUtil.propertyAsLong(options,
        MIN_FILE_SIZE_BYTES,
        (long) (targetFileSize * MIN_FILE_SIZE_DEFAULT_RATIO));

    maxFileSize = PropertyUtil.propertyAsLong(options,
        MAX_FILE_SIZE_BYTES,
        (long) (targetFileSize * MAX_FILE_SIZE_DEFAULT_RATIO));

    maxGroupSize = PropertyUtil.propertyAsLong(options,
        RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES,
        RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES_DEFAULT);

    minOutputFiles = PropertyUtil.propertyAsInt(options,
        MIN_OUTPUT_FILES,
        MIN_OUTPUT_FILES_DEFAULT);

    minInputFiles = PropertyUtil.propertyAsInt(options,
        MIN_INPUT_FILES,
        MIN_INPUT_FILES_DEFAULT);

    validateOptions();
    return this;
  }

  @Override
  public Iterable<FileScanTask> selectFilesToRewrite(Iterable<FileScanTask> dataFiles) {
    return FluentIterable.from(dataFiles)
        .filter(scanTask -> scanTask.length() < minFileSize || scanTask.length() > maxFileSize);
  }

  @Override
  public Iterable<List<FileScanTask>> planFileGroups(Iterable<FileScanTask> dataFiles) {
    ListPacker<FileScanTask> packer = new BinPacking.ListPacker<>(maxGroupSize, 1, false);
    List<List<FileScanTask>> potentialGroups = packer.pack(dataFiles, FileScanTask::length);
    return potentialGroups.stream().filter(group ->
        numOutputFiles(group) >= minOutputFiles && group.size() >= minInputFiles
    ).collect(Collectors.toList());
  }

  private long numOutputFiles(List<FileScanTask> group) {
    long groupSize = group.stream().mapToLong(FileScanTask::length).sum();
    return (long) Math.ceil((double) groupSize / targetFileSize);
  }

  private void validateOptions() {
    Preconditions.checkArgument(minFileSize >= 0,
        "Cannot set %s to a negative number, %d < 0",
        MIN_FILE_SIZE_BYTES, minFileSize);

    Preconditions.checkArgument(maxFileSize > minFileSize,
        "Cannot set %s greater than or equal to %s, %d >= %d",
        MIN_FILE_SIZE_BYTES, MAX_FILE_SIZE_BYTES, minFileSize, maxFileSize);

    Preconditions.checkArgument(targetFileSize > minFileSize,
        "Cannot set %s greater than or equal to %s, all files written will be smaller than the threshold, %d >= %d",
        MIN_FILE_SIZE_BYTES, RewriteDataFiles.TARGET_FILE_SIZE_BYTES, minFileSize, targetFileSize);

    Preconditions.checkArgument(targetFileSize < maxFileSize,
        "Cannot set %s is greater than or equal to %s, all files written will be larger than the threshold, %d >= %d",
        MAX_FILE_SIZE_BYTES, RewriteDataFiles.TARGET_FILE_SIZE_BYTES, maxFileSize, targetFileSize);

    Preconditions.checkArgument(minInputFiles > 0,
        "Cannot set %s is less than 1. All values less than 1 have the same effect as 1. %d < 1",
        MIN_INPUT_FILES, minInputFiles);

    Preconditions.checkArgument(minOutputFiles > 0,
        "Cannot set %s to less than 1. All values less than 1 have the same effect as 1. %d < 1",
        MIN_OUTPUT_FILES, minOutputFiles);
  }
}
