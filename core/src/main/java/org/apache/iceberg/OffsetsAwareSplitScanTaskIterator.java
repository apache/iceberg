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
import java.util.NoSuchElementException;
import org.apache.iceberg.util.ArrayUtil;

/**
 * An iterator that splits tasks using split offsets such as row group offsets in Parquet.
 *
 * @param <T> the Java type of tasks produced by this iterator
 */
class OffsetsAwareSplitScanTaskIterator<T extends ScanTask> implements SplitScanTaskIterator<T> {
  private final T parentTask;
  private final SplitScanTaskCreator<T> splitTaskCreator;
  private final long[] offsets;
  private final long[] splitSizes;
  private int splitIndex = 0;

  OffsetsAwareSplitScanTaskIterator(
      T parentTask,
      long parentTaskLength,
      List<Long> offsets,
      SplitScanTaskCreator<T> splitTaskCreator) {
    this(parentTask, parentTaskLength, ArrayUtil.toLongArray(offsets), splitTaskCreator);
  }

  OffsetsAwareSplitScanTaskIterator(
      T parentTask,
      long parentTaskLength,
      long[] offsets,
      SplitScanTaskCreator<T> splitTaskCreator) {
    this.parentTask = parentTask;
    this.splitTaskCreator = splitTaskCreator;
    this.offsets = offsets;
    this.splitSizes = new long[offsets.length];
    if (offsets.length > 0) {
      int lastIndex = offsets.length - 1;
      for (int index = 0; index < lastIndex; index++) {
        splitSizes[index] = offsets[index + 1] - offsets[index];
      }
      splitSizes[lastIndex] = parentTaskLength - offsets[lastIndex];
    }
  }

  @Override
  public boolean hasNext() {
    return splitIndex < splitSizes.length;
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    long offset = offsets[splitIndex];
    long splitSize = splitSizes[splitIndex];
    splitIndex += 1; // create 1 split per offset
    return splitTaskCreator.create(parentTask, offset, splitSize);
  }
}
