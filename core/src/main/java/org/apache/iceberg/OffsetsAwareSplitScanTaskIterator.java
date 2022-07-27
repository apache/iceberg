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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Ordering;

/**
 * An iterator that splits tasks using split offsets such as row group offsets in Parquet.
 *
 * @param <T> the Java type of tasks produced by this iterator
 */
class OffsetsAwareSplitScanTaskIterator<T extends ScanTask> implements SplitScanTaskIterator<T> {
  private static final Ordering<Comparable<Long>> OFFSET_ORDERING = Ordering.natural();

  private final T parentTask;
  private final SplitScanTaskCreator<T> splitTaskCreator;
  private final List<Long> offsets;
  private final List<Long> splitSizes;
  private int splitIndex = 0;

  OffsetsAwareSplitScanTaskIterator(
      T parentTask,
      long parentTaskLength,
      List<Long> offsetList,
      SplitScanTaskCreator<T> splitTaskCreator) {
    Preconditions.checkArgument(
        OFFSET_ORDERING.isStrictlyOrdered(offsetList), "Offsets must be sorted in asc order");

    this.parentTask = parentTask;
    this.splitTaskCreator = splitTaskCreator;
    this.offsets = ImmutableList.copyOf(offsetList);
    this.splitSizes = Lists.newArrayListWithCapacity(offsets.size());
    if (offsets.size() > 0) {
      int lastIndex = offsets.size() - 1;
      for (int index = 0; index < lastIndex; index++) {
        splitSizes.add(offsets.get(index + 1) - offsets.get(index));
      }
      splitSizes.add(parentTaskLength - offsets.get(lastIndex));
    }
  }

  @Override
  public boolean hasNext() {
    return splitIndex < splitSizes.size();
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    long offset = offsets.get(splitIndex);
    long splitSize = splitSizes.get(splitIndex);
    splitIndex += 1; // create 1 split per offset
    return splitTaskCreator.create(parentTask, offset, splitSize);
  }
}
