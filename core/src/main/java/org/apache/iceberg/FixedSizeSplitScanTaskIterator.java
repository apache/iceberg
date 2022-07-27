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
 * An iterator that splits tasks using a fixed target split size.
 *
 * @param <T> the Java type of tasks produced by this iterator
 */
class FixedSizeSplitScanTaskIterator<T extends ScanTask> implements SplitScanTaskIterator<T> {
  private final T parentTask;
  private final long splitSize;
  private final SplitScanTaskCreator<T> splitTaskCreator;
  private long offset;
  private long remainingLength;

  FixedSizeSplitScanTaskIterator(
      T parentTask,
      long parentTaskLength,
      long splitSize,
      SplitScanTaskCreator<T> splitTaskCreator) {
    this.parentTask = parentTask;
    this.splitSize = splitSize;
    this.splitTaskCreator = splitTaskCreator;
    this.offset = 0;
    this.remainingLength = parentTaskLength;
  }

  @Override
  public boolean hasNext() {
    return remainingLength > 0;
  }

  @Override
  public T next() {
    long length = Math.min(splitSize, remainingLength);
    T splitTask = splitTaskCreator.create(parentTask, offset, length);
    offset += length;
    remainingLength -= length;
    return splitTask;
  }
}
