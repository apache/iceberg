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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.expressions.Expression;

/**
 * An action that rewrites data files.
 */
public interface RewriteDataFiles extends SnapshotUpdate<RewriteDataFiles, RewriteDataFiles.Result> {
  /**
   * Pass a row filter to filter {@link DataFile}s to be rewritten.
   * <p>
   * Note that all files that may contain data matching the filter may be rewritten.
   * <p>
   * If not set, all files will be rewritten.
   *
   * @param expr a row filter to filter out data files
   * @return this for method chaining
   */
  RewriteDataFiles filter(Expression expr);

  /**
   * Enables or disables case sensitive expression binding.
   * <p>
   * If not set, defaults to false.
   *
   * @param caseSensitive caseSensitive
   * @return this for method chaining
   */
  RewriteDataFiles caseSensitive(boolean caseSensitive);

  /**
   * Pass a PartitionSpec id to specify which PartitionSpec should be used in DataFile rewrite
   * <p>
   * If not set, defaults to the table's default spec ID.
   *
   * @param specId PartitionSpec id to rewrite
   * @return this for method chaining
   */
  RewriteDataFiles outputSpecId(int specId);

  /**
   * Specify the target data file size in bytes.
   * <p>
   * If not set, defaults to the table's target file size.
   *
   * @param targetSizeInBytes size in bytes of rewrite data file
   * @return this for method chaining
   */
  RewriteDataFiles targetSizeInBytes(long targetSizeInBytes);

  /**
   * Specify the number of "bins" considered when trying to pack the next file split into a task. Increasing this
   * usually makes tasks a bit more even by considering more ways to pack file regions into a single task with extra
   * planning cost.
   * <p>
   * This configuration can reorder the incoming file regions, to preserve order for lower/upper bounds in file
   * metadata, user can use a lookback of 1.
   *
   * @param splitLookback number of "bins" considered when trying to pack the next file split into a task.
   * @return this for method chaining
   */
  RewriteDataFiles splitLookback(int splitLookback);

  /**
   * Specify the cost of opening a file that will be taken into account during packing files into
   * bins. If the size of the file is smaller than the cost of opening, then this value will be used
   * instead of the actual file size.
   * <p>
   * If not set, defaults to the table's open file cost.
   *
   * @param splitOpenFileCost minimum file size to count to pack into one "bin".
   * @return this for method chaining
   */
  RewriteDataFiles splitOpenFileCost(long splitOpenFileCost);

  /**
   * The action result that contains a summary of the execution.
   */
  interface Result {
    /**
     * Returns rewritten data files.
     */
    Iterable<DataFile> rewrittenDataFiles();

    /**
     * Returns added data files.
     */
    Iterable<DataFile> addedDataFiles();
  }
}
