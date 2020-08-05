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

import java.util.List;
import org.apache.iceberg.expressions.Expression;

/**
 * A scan task over a range of a single file.
 */
public interface FileScanTask extends ScanTask {
  /**
   * The {@link DataFile file} to scan.
   *
   * @return the file to scan
   */
  DataFile file();

  /**
   * A list of {@link DeleteFile delete files} to apply when reading the task's data file.
   *
   * @return a list of delete files to apply
   */
  List<DeleteFile> deletes();

  /**
   * The {@link PartitionSpec spec} used to store this file.
   *
   * @return the partition spec from this file's manifest
   */
  PartitionSpec spec();

  /**
   * The starting position of this scan range in the file.
   *
   * @return the start position of this scan range
   */
  long start();

  /**
   * The number of bytes to scan from the {@link #start()} position in the file.
   *
   * @return the length of this scan range in bytes
   */
  long length();

  /**
   * Returns the residual expression that should be applied to rows in this file scan.
   * <p>
   * The residual expression for a file is a filter expression created from the scan's filter, inclusive
   * any predicates that are true or false for the entire file removed, based on the file's
   * partition data.
   *
   * @return a residual expression to apply to rows from this scan
   */
  Expression residual();

  /**
   * Splits this scan task into component {@link FileScanTask scan tasks}, each of {@code splitSize} size
   * @param splitSize The size of a component scan task
   * @return an Iterable of {@link FileScanTask scan tasks}
   */
  Iterable<FileScanTask> split(long splitSize);

  @Override
  default boolean isFileScanTask() {
    return true;
  }

  @Override
  default FileScanTask asFileScanTask() {
    return this;
  }
}
