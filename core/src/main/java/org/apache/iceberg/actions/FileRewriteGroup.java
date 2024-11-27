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

import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.RewriteJobOrder;

/**
 * Container class representing a set of files to be rewritten by a {@link FileRewriteExecutor}.
 *
 * @param <I> the Java type of the plan info
 * @param <T> the Java type of the tasks to read content files
 * @param <F> the Java type of the content files
 */
public abstract class FileRewriteGroup<I, T extends ContentScanTask<F>, F extends ContentFile<F>> {
  private final I info;
  private final List<T> fileScanTasks;
  private final long splitSize;
  private final int expectedOutputFiles;

  protected FileRewriteGroup(
      I info, List<T> fileScanTasks, long splitSize, int expectedOutputFiles) {
    this.info = info;
    this.fileScanTasks = fileScanTasks;
    this.splitSize = splitSize;
    this.expectedOutputFiles = expectedOutputFiles;
  }

  public I info() {
    return info;
  }

  public List<T> fileScans() {
    return fileScanTasks;
  }

  public long splitSize() {
    return splitSize;
  }

  public int expectedOutputFiles() {
    return expectedOutputFiles;
  }

  public long sizeInBytes() {
    return fileScanTasks.stream().mapToLong(T::length).sum();
  }

  public int numInputFiles() {
    return fileScanTasks.size();
  }

  public static <I, T extends ContentScanTask<F>, F extends ContentFile<F>>
      Comparator<FileRewriteGroup<I, T, F>> taskComparator(RewriteJobOrder rewriteJobOrder) {
    switch (rewriteJobOrder) {
      case BYTES_ASC:
        return Comparator.comparing(FileRewriteGroup::sizeInBytes);
      case BYTES_DESC:
        return Comparator.comparing(FileRewriteGroup::sizeInBytes, Comparator.reverseOrder());
      case FILES_ASC:
        return Comparator.comparing(FileRewriteGroup::numInputFiles);
      case FILES_DESC:
        return Comparator.comparing(FileRewriteGroup::numInputFiles, Comparator.reverseOrder());
      default:
        return (unused, unused2) -> 0;
    }
  }
}
