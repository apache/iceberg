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
public abstract class FileRewriteGroup<I, T extends ContentScanTask<F>, F extends ContentFile<F>> {
  private final I info;
  private final List<T> fileScanTasks;
  private final long maxOutputFileSize;
  private final long inputSplitSize;
  private final int expectedOutputFiles;

  FileRewriteGroup(
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
  public List<T> fileScans() {
    return fileScanTasks;
  }

  /**
   * While we create tasks that should all be smaller than our target size, there is a chance that
   * the actual data will end up being larger than our target size due to various factors of
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
  public long maxOutputFileSize() {
    return maxOutputFileSize;
  }

  /**
   * Determines the reader split size as the input size divided by the desired number of output
   * files. The final split size is adjusted to be at least as big as the target file size but less
   * than the max write file size.
   */
  public long inputSplitSize() {
    return inputSplitSize;
  }

  /** Expected number of the output files. */
  public int expectedOutputFiles() {
    return expectedOutputFiles;
  }

  /** Accumulated size for the input files. */
  public long sizeInBytes() {
    return fileScanTasks.stream().mapToLong(T::length).sum();
  }

  /** Number of the input files. */
  public int numFiles() {
    return fileScanTasks.size();
  }
}
