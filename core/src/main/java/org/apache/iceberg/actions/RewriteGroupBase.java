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
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;

/**
 * Container class representing a set of files to be rewritten by a {@link FileRewriteRunner}.
 *
 * @param <I> the Java type of the plan info member like {@link RewriteDataFiles.FileGroupInfo} or
 *     {@link RewritePositionDeleteFiles.FileGroupInfo}
 * @param <T> the Java type of the input scan tasks (input)
 * @param <F> the Java type of the content files (input and output)
 */
public abstract class RewriteGroupBase<I, T extends ContentScanTask<F>, F extends ContentFile<F>> {
  private final I info;
  private final List<T> fileScanTasks;
  private final long maxOutputFileSize;
  private final long inputSplitSize;
  private final int expectedOutputFiles;

  RewriteGroupBase(
      I info,
      List<T> fileScanTasks,
      long maxOutputFileSize,
      long inputSplitSize,
      int expectedOutputFiles) {
    this.info = info;
    this.fileScanTasks = fileScanTasks;
    this.maxOutputFileSize = maxOutputFileSize;
    this.inputSplitSize = inputSplitSize;
    this.expectedOutputFiles = expectedOutputFiles;
  }

  /** Identifiers and partition information about the group. */
  public I info() {
    return info;
  }

  /** Scan tasks for input files. */
  public List<T> fileScanTasks() {
    return fileScanTasks;
  }

  /** Accumulated size for the input files. */
  public long inputFilesSizeInBytes() {
    return fileScanTasks.stream().mapToLong(T::length).sum();
  }

  /** Number of the input files. */
  public int inputFileNum() {
    return fileScanTasks.size();
  }

  /**
   * The target file size which should be used by the {@link FileRewriteRunner}. The {@link
   * FileRewritePlanner} could chose different values than defined by the table properties.
   *
   * @return the target size should be used by the runner
   */
  public long maxOutputFileSize() {
    return maxOutputFileSize;
  }

  /**
   * The amount of bytes of data the {@link FileRewriteRunner} should read from a single group in a
   * single read task. The {@link FileRewritePlanner} chooses a value to allow parallelization for
   * the runners, but prevent fragmentation of the output caused by too many readers.
   */
  public long inputSplitSize() {
    return inputSplitSize;
  }

  /** The total number of files that should be produced by the rewrite of this entire file group. */
  public int expectedOutputFiles() {
    return expectedOutputFiles;
  }
}
