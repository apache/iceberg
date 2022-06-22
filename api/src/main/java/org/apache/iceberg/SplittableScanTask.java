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

package org.apache.iceberg;

/**
 * A scan task that can be split into smaller scan tasks.
 *
 * @param <ThisT> the child Java API class, returned by method chaining
 */
public interface SplittableScanTask<ThisT> extends ScanTask {
  /**
   * Attempts to split this scan task into several smaller scan tasks, each close to {@code targetSplitSizeBytes} size.
   * <p>
   * Note the target split size is just a guidance and the actual split size may be either smaller or larger.
   * File formats like Parquet may leverage the row group offset information while splitting tasks.
   *
   * @param targetSplitSizeBytes the target size of each new scan task in bytes
   * @return an Iterable of smaller tasks
   */
  Iterable<ThisT> split(long targetSplitSizeBytes);

  /**
   * Checks if this task is adjacent to another.
   * <p>
   * Two tasks are adjacent if they are scanning the same file and one task begins where exactly the other ends.
   *
   * @param other another task
   * @return whether this task is adjacent to another
   */
  boolean isAdjacent(ThisT other);

  /**
   * Combines this task with an adjacent task into one.
   *
   * @param other another task
   * @return a new combined task
   */
  ThisT combineWithAdjacentTask(ThisT other);

  /**
   * The total number of bytes that should be read by this scan task.
   *
   * @return the total number of bytes to read
   */
  long totalSizeBytes();

  /**
   * The total number of files that will be opened by this scan task.
   *
   * @return the total number of files to open
   */
  long totalFilesCount();
}
