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

import org.apache.iceberg.expressions.Expression;

/**
 * A scan task over a range of bytes in a content file.
 *
 * @param <F> the Java class of the content file
 */
public interface ContentScanTask<F extends ContentFile<F>> extends ScanTask, PartitionScanTask {
  /**
   * The {@link ContentFile file} to scan.
   *
   * @return the file to scan
   */
  F file();

  @Override
  default StructLike partition() {
    return file().partition();
  }

  @Override
  default long sizeBytes() {
    return length();
  }

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
   *
   * <p>The residual expression for a file is a filter expression created by partially evaluating
   * the scan's filter using the file's partition data.
   *
   * @return a residual expression to apply to rows from this scan
   */
  Expression residual();

  @Override
  default long estimatedRowsCount() {
    long splitOffset = (file().splitOffsets() != null) ? file().splitOffsets().get(0) : 0L;
    double scannedFileFraction = ((double) length()) / (file().fileSizeInBytes() - splitOffset);
    return (long) (scannedFileFraction * file().recordCount());
  }
}
