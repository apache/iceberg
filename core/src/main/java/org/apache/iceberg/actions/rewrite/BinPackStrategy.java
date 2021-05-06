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

package org.apache.iceberg.actions.rewrite;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFiles.Strategy;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.util.BinPacking;
import org.apache.iceberg.util.BinPacking.ListPacker;
import org.apache.iceberg.util.PropertyUtil;

abstract class BinPackStrategy implements RewriteStrategy {

  /**
   * Minimum number of files that need to be in a file group to be considered
   * for rewriting.
   */
  public static final String MIN_NUM_INPUT_FILES = "min-num-input-files";
  public static final int MIN_NUM_INPUT_FILES_DEFAULT = 1;
  private int minNumInputFiles;

  /**
   * Minimum number of files we want to be created by file group when being
   * rewritten.
   */
  public static final String MIN_NUM_OUTPUT_FILES = "min-num-output-file";
  public static final int MIN_NUM_OUTPUT_FILES_DEFAULT = 1;
  private int minNumOutputFiles;

  /**
   * Adjusts files which will be considered for rewriting. Files smaller than
   * MIN_FILE_SIZE_BYTES will be considered for rewriting.
   * <p>
   * Defaults to 75% of the target file size
   */
  public static final String MIN_FILE_SIZE_BYTES = "min-file-size-bytes";
  public static final double MIN_FILE_SIZE_DEFAULT_RATIO = 0.75d;
  private long minFileSize;

  /**
   * Adjusts files which will be considered for rewriting. Files larger than
   * MAX_FILE_SIZE_BYTES will be considered for rewriting.
   * <p>
   * Defaults to 180% of the target file size
   */
  public static final String MAX_FILE_SIZE_BYTES = "max-file-size-bytes";
  public static final double MAX_FILE_SIZE_DEFAULT_RATIO = 1.80d;
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
        MIN_NUM_INPUT_FILES,
        MIN_NUM_OUTPUT_FILES,
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

    minNumOutputFiles = PropertyUtil.propertyAsInt(options,
        MIN_NUM_OUTPUT_FILES,
        MIN_NUM_OUTPUT_FILES_DEFAULT);

    minNumInputFiles = PropertyUtil.propertyAsInt(options,
        MIN_NUM_INPUT_FILES,
        MIN_NUM_INPUT_FILES_DEFAULT);

    return this;
  }


  @Override
  public Iterable<FileScanTask> selectFilesToRewrite(Iterable<FileScanTask> dataFiles) {
    return StreamSupport.stream(dataFiles.spliterator(), false)
        .filter(scanTask ->
          scanTask.length() < minFileSize || scanTask.length() > maxFileSize
        ).collect(Collectors.toList());
  }

  @Override
  public Iterable<List<FileScanTask>> planFileGroups(Iterable<FileScanTask> dataFiles) {
    ListPacker<FileScanTask> packer = new BinPacking.ListPacker<>(maxGroupSize, 1, false);
    List<List<FileScanTask>> potentialGroups = packer.pack(dataFiles, FileScanTask::length);
    return potentialGroups.stream().filter(group ->
        numOutputFiles(group) >= minNumOutputFiles && group.size() >= minNumInputFiles
    ).collect(Collectors.toList());
  }

  private long numOutputFiles(List<FileScanTask> group) {
    long groupSize = group.stream().mapToLong(scanTask -> scanTask.length()).sum();
    long numOutputFiles = (long) Math.ceil((double) groupSize / targetFileSize);
    return numOutputFiles;
  }
}
