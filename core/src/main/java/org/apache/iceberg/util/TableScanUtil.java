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

package org.apache.iceberg.util;

import java.util.function.Function;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;

public class TableScanUtil {

  private TableScanUtil() {
  }

  public static boolean hasDeletes(CombinedScanTask task) {
    return task.files().stream().anyMatch(TableScanUtil::hasDeletes);
  }

  /**
   * This is temporarily introduced since we plan to support pos-delete vectorized read first, then get to the
   * equality-delete support. We will remove this method once both are supported.
   */
  public static boolean hasEqDeletes(CombinedScanTask task) {
    return task.files().stream().anyMatch(
        t -> t.deletes().stream().anyMatch(deleteFile -> deleteFile.content().equals(FileContent.EQUALITY_DELETES)));
  }

  public static boolean hasDeletes(FileScanTask task) {
    return !task.deletes().isEmpty();
  }

  public static CloseableIterable<FileScanTask> splitFiles(CloseableIterable<FileScanTask> tasks, long splitSize) {
    Preconditions.checkArgument(splitSize > 0, "Invalid split size (negative or 0): %s", splitSize);

    Iterable<FileScanTask> splitTasks = FluentIterable
        .from(tasks)
        .transformAndConcat(input -> input.split(splitSize));
    // Capture manifests which can be closed after scan planning
    return CloseableIterable.combine(splitTasks, tasks);
  }

  /**
   * Split files into FileScanTasks which only contain a single offset (rowGroup). For files which do not
   * expose the offsets, use the normal split code.
   * @param tasks Scan tasks, one per whole file to be split
   * @param fallbackSplitSize the splitSize to use when the file does not contain explicit offsets to use
   * @return Scan tasks, one per offset
   */
  public static CloseableIterable<FileScanTask> splitOnOffsets(CloseableIterable<FileScanTask> tasks,
                                                               long fallbackSplitSize) {
    Preconditions.checkArgument(fallbackSplitSize > 0,
        "Invalid fallback split size (negative or 0): %s", fallbackSplitSize);

    Iterable<FileScanTask> splitTasks = FluentIterable
        .from(tasks)
        .transformAndConcat(input -> {
          DataFile file = input.file();
          if (file.format().hasOffsets()) {
            if (file.splitOffsets() != null) {
              // Split on offsets, size 0 means 1 task per offset
              return input.split(0);
            } else {
              // File too small to have offsets, take the entire file as a task
              return CloseableIterable.withNoopClose(input);
            }
          } else {
            // File cannot have offsets, split normally
            return input.split(fallbackSplitSize);
          }
        });
    // Capture manifests which can be closed after scan planning
    return CloseableIterable.combine(splitTasks, tasks);
  }

  public static CloseableIterable<CombinedScanTask> planTasks(CloseableIterable<FileScanTask> splitFiles,
                                                              long splitSize, int lookback, long openFileCost) {
    Preconditions.checkArgument(splitSize > 0, "Invalid split size (negative or 0): %s", splitSize);
    Preconditions.checkArgument(lookback > 0, "Invalid split planning lookback (negative or 0): %s", lookback);
    Preconditions.checkArgument(openFileCost >= 0, "Invalid file open cost (negative): %s", openFileCost);

    // Check the size of delete file as well to avoid unbalanced bin-packing
    Function<FileScanTask, Long> weightFunc = file -> Math.max(
        file.length() + file.deletes().stream().mapToLong(ContentFile::fileSizeInBytes).sum(),
        (1 + file.deletes().size()) * openFileCost);

    return CloseableIterable.transform(
        CloseableIterable.combine(
            new BinPacking.PackingIterable<>(splitFiles, splitSize, lookback, weightFunc, true),
            splitFiles),
        BaseCombinedScanTask::new);
  }
}
