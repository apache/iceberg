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

import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.math.LongMath;
import org.apache.iceberg.util.BinPacking;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A file rewriter that determines which files to rewrite based on their size.
 *
 * <p>If files are smaller than the {@link #MIN_FILE_SIZE_BYTES} threshold or larger than the {@link
 * #MAX_FILE_SIZE_BYTES} threshold, they are considered targets for being rewritten.
 *
 * <p>Once selected, files are grouped based on the {@link BinPacking bin-packing algorithm} into
 * groups of no more than {@link #MAX_FILE_GROUP_SIZE_BYTES}. Groups will be actually rewritten if
 * they contain more than {@link #MIN_INPUT_FILES} or if they would produce at least one file of
 * {@link #TARGET_FILE_SIZE_BYTES}.
 *
 * <p>Note that implementations may add extra conditions for selecting files or filtering groups.
 */
public abstract class SizeBasedFileRewriter<T extends ContentScanTask<F>, F extends ContentFile<F>>
    implements FileRewriter<T, F> {

  private static final Logger LOG = LoggerFactory.getLogger(SizeBasedFileRewriter.class);

  /** The target output file size that this file rewriter will attempt to generate. */
  public static final String TARGET_FILE_SIZE_BYTES = "target-file-size-bytes";

  /**
   * Controls which files will be considered for rewriting. Files with sizes under this threshold
   * will be considered for rewriting regardless of any other criteria.
   *
   * <p>Defaults to 75% of the target file size.
   */
  public static final String MIN_FILE_SIZE_BYTES = "min-file-size-bytes";

  public static final double MIN_FILE_SIZE_DEFAULT_RATIO = 0.75;

  /**
   * Controls which files will be considered for rewriting. Files with sizes above this threshold
   * will be considered for rewriting regardless of any other criteria.
   *
   * <p>Defaults to 180% of the target file size.
   */
  public static final String MAX_FILE_SIZE_BYTES = "max-file-size-bytes";

  public static final double MAX_FILE_SIZE_DEFAULT_RATIO = 1.80;

  /**
   * Any file group exceeding this number of files will be rewritten regardless of other criteria.
   * This config ensures file groups that contain many files are compacted even if the total size of
   * that group is less than the target file size. This can also be thought of as the maximum number
   * of wrongly sized files that could remain in a partition after rewriting.
   */
  public static final String MIN_INPUT_FILES = "min-input-files";

  public static final int MIN_INPUT_FILES_DEFAULT = 5;

  /** Overrides other options and forces rewriting of all provided files. */
  public static final String REWRITE_ALL = "rewrite-all";

  public static final boolean REWRITE_ALL_DEFAULT = false;

  /**
   * This option controls the largest amount of data that should be rewritten in a single file
   * group. It helps with breaking down the rewriting of very large partitions which may not be
   * rewritable otherwise due to the resource constraints of the cluster. For example, a sort-based
   * rewrite may not scale to TB-sized partitions, and those partitions need to be worked on in
   * small subsections to avoid exhaustion of resources.
   */
  public static final String MAX_FILE_GROUP_SIZE_BYTES = "max-file-group-size-bytes";

  public static final long MAX_FILE_GROUP_SIZE_BYTES_DEFAULT = 100L * 1024 * 1024 * 1024; // 100 GB

  private static final long SPLIT_OVERHEAD = 5 * 1024;

  private final Table table;
  private long targetFileSize;
  private long minFileSize;
  private long maxFileSize;
  private int minInputFiles;
  private boolean rewriteAll;
  private long maxGroupSize;

  protected SizeBasedFileRewriter(Table table) {
    this.table = table;
  }

  protected abstract long defaultTargetFileSize();

  protected abstract Iterable<T> filterFiles(Iterable<T> tasks);

  protected abstract Iterable<List<T>> filterFileGroups(List<List<T>> groups);

  protected Table table() {
    return table;
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.of(
        TARGET_FILE_SIZE_BYTES,
        MIN_FILE_SIZE_BYTES,
        MAX_FILE_SIZE_BYTES,
        MIN_INPUT_FILES,
        REWRITE_ALL,
        MAX_FILE_GROUP_SIZE_BYTES);
  }

  @Override
  public void init(Map<String, String> options) {
    Map<String, Long> sizeThresholds = sizeThresholds(options);
    this.targetFileSize = sizeThresholds.get(TARGET_FILE_SIZE_BYTES);
    this.minFileSize = sizeThresholds.get(MIN_FILE_SIZE_BYTES);
    this.maxFileSize = sizeThresholds.get(MAX_FILE_SIZE_BYTES);

    this.minInputFiles = minInputFiles(options);
    this.rewriteAll = rewriteAll(options);
    this.maxGroupSize = maxGroupSize(options);

    if (rewriteAll) {
      LOG.info("Configured to rewrite all provided files in table {}", table.name());
    }
  }

  protected boolean wronglySized(T task) {
    return task.length() < minFileSize || task.length() > maxFileSize;
  }

  @Override
  public Iterable<List<T>> planFileGroups(Iterable<T> tasks) {
    Iterable<T> filteredTasks = rewriteAll ? tasks : filterFiles(tasks);
    BinPacking.ListPacker<T> packer = new BinPacking.ListPacker<>(maxGroupSize, 1, false);
    List<List<T>> groups = packer.pack(filteredTasks, ContentScanTask::length);
    return rewriteAll ? groups : filterFileGroups(groups);
  }

  protected boolean enoughInputFiles(List<T> group) {
    return group.size() > 1 && group.size() >= minInputFiles;
  }

  protected boolean enoughContent(List<T> group) {
    return group.size() > 1 && inputSize(group) > targetFileSize;
  }

  protected boolean tooMuchContent(List<T> group) {
    return inputSize(group) > maxFileSize;
  }

  protected long inputSize(List<T> group) {
    return group.stream().mapToLong(ContentScanTask::length).sum();
  }

  /**
   * Calculates the split size to use in bin-packing rewrites.
   *
   * <p>This method determines the target split size as the input size divided by the desired number
   * of output files. The final split size is adjusted to be at least as big as the target file size
   * but less than the max write file size.
   */
  protected long splitSize(long inputSize) {
    long estimatedSplitSize = (inputSize / numOutputFiles(inputSize)) + SPLIT_OVERHEAD;
    if (estimatedSplitSize < targetFileSize) {
      return targetFileSize;
    } else if (estimatedSplitSize > writeMaxFileSize()) {
      return writeMaxFileSize();
    } else {
      return estimatedSplitSize;
    }
  }

  /**
   * Determines the preferable number of output files when rewriting a particular file group.
   *
   * <p>If the rewriter is handling 10.1 GB of data with a target file size of 1 GB, it could
   * produce 11 files, one of which would only have 0.1 GB. This would most likely be less
   * preferable to 10 files with 1.01 GB each. So this method decides whether to round up or round
   * down based on what the estimated average file size will be if the remainder (0.1 GB) is
   * distributed amongst other files. If the new average file size is no more than 10% greater than
   * the target file size, then this method will round down when determining the number of output
   * files. Otherwise, the remainder will be written into a separate file.
   *
   * @param inputSize a total input size for a file group
   * @return the number of files this rewriter should create
   */
  protected long numOutputFiles(long inputSize) {
    if (inputSize < targetFileSize) {
      return 1;
    }

    long numFilesWithRemainder = LongMath.divide(inputSize, targetFileSize, RoundingMode.CEILING);
    long numFilesWithoutRemainder = LongMath.divide(inputSize, targetFileSize, RoundingMode.FLOOR);
    long avgFileSizeWithoutRemainder = inputSize / numFilesWithoutRemainder;

    if (LongMath.mod(inputSize, targetFileSize) > minFileSize) {
      // the remainder file is of a valid size for this rewrite so keep it
      return numFilesWithRemainder;

    } else if (avgFileSizeWithoutRemainder < Math.min(1.1 * targetFileSize, writeMaxFileSize())) {
      // if the reminder is distributed amongst other files,
      // the average file size will be no more than 10% bigger than the target file size
      // so round down and distribute remainder amongst other files
      return numFilesWithoutRemainder;

    } else {
      // keep the remainder file as it is not OK to distribute it amongst other files
      return numFilesWithRemainder;
    }
  }

  /**
   * Estimates a larger max target file size than the target size used in task creation to avoid
   * creating tiny remainder files.
   *
   * <p>While we create tasks that should all be smaller than our target size, there is a chance
   * that the actual data will end up being larger than our target size due to various factors of
   * compression, serialization, which are outside our control. If this occurs, instead of making a
   * single file that is close in size to our target, we would end up producing one file of the
   * target size, and then a small extra file with the remaining data.
   *
   * <p>For example, if our target is 512 MB, we may generate a rewrite task that should be 500 MB.
   * When we write the data we may find we actually have to write out 530 MB. If we use the target
   * size while writing, we would produce a 512 MB file and an 18 MB file. If instead we use a
   * larger size estimated by this method, then we end up writing a single file.
   *
   * @return the target size plus one half of the distance between max and target
   */
  protected long writeMaxFileSize() {
    return (long) (targetFileSize + ((maxFileSize - targetFileSize) * 0.5));
  }

  private Map<String, Long> sizeThresholds(Map<String, String> options) {
    long target =
        PropertyUtil.propertyAsLong(options, TARGET_FILE_SIZE_BYTES, defaultTargetFileSize());

    long defaultMin = (long) (target * MIN_FILE_SIZE_DEFAULT_RATIO);
    long min = PropertyUtil.propertyAsLong(options, MIN_FILE_SIZE_BYTES, defaultMin);

    long defaultMax = (long) (target * MAX_FILE_SIZE_DEFAULT_RATIO);
    long max = PropertyUtil.propertyAsLong(options, MAX_FILE_SIZE_BYTES, defaultMax);

    Preconditions.checkArgument(
        target > 0, "'%s' is set to %s but must be > 0", TARGET_FILE_SIZE_BYTES, target);

    Preconditions.checkArgument(
        min >= 0, "'%s' is set to %s but must be >= 0", MIN_FILE_SIZE_BYTES, min);

    Preconditions.checkArgument(
        target > min,
        "'%s' (%s) must be > '%s' (%s), all new files will be smaller than the min threshold",
        TARGET_FILE_SIZE_BYTES,
        target,
        MIN_FILE_SIZE_BYTES,
        min);

    Preconditions.checkArgument(
        target < max,
        "'%s' (%s) must be < '%s' (%s), all new files will be larger than the max threshold",
        TARGET_FILE_SIZE_BYTES,
        target,
        MAX_FILE_SIZE_BYTES,
        max);

    Map<String, Long> values = Maps.newHashMap();

    values.put(TARGET_FILE_SIZE_BYTES, target);
    values.put(MIN_FILE_SIZE_BYTES, min);
    values.put(MAX_FILE_SIZE_BYTES, max);

    return values;
  }

  private int minInputFiles(Map<String, String> options) {
    int value = PropertyUtil.propertyAsInt(options, MIN_INPUT_FILES, MIN_INPUT_FILES_DEFAULT);
    Preconditions.checkArgument(
        value > 0, "'%s' is set to %s but must be > 0", MIN_INPUT_FILES, value);
    return value;
  }

  private long maxGroupSize(Map<String, String> options) {
    long value =
        PropertyUtil.propertyAsLong(
            options, MAX_FILE_GROUP_SIZE_BYTES, MAX_FILE_GROUP_SIZE_BYTES_DEFAULT);
    Preconditions.checkArgument(
        value > 0, "'%s' is set to %s but must be > 0", MAX_FILE_GROUP_SIZE_BYTES, value);
    return value;
  }

  private boolean rewriteAll(Map<String, String> options) {
    return PropertyUtil.propertyAsBoolean(options, REWRITE_ALL, REWRITE_ALL_DEFAULT);
  }
}
