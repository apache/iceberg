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
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/** A split of a {@link PositionDeletesScanTask} that is mergeable. */
class SplitPositionDeletesScanTask
    implements PositionDeletesScanTask, MergeableScanTask<PositionDeletesScanTask> {

  private final PositionDeletesScanTask parentTask;
  private final long offset;
  private final long length;

  protected SplitPositionDeletesScanTask(
      PositionDeletesScanTask parentTask, long offset, long length) {
    this.parentTask = parentTask;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public DeleteFile file() {
    return parentTask.file();
  }

  @Override
  public PartitionSpec spec() {
    return parentTask.spec();
  }

  @Override
  public long start() {
    return offset;
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public Expression residual() {
    return parentTask.residual();
  }

  @Override
  public boolean canMerge(org.apache.iceberg.ScanTask other) {
    if (other instanceof SplitPositionDeletesScanTask) {
      SplitPositionDeletesScanTask that = (SplitPositionDeletesScanTask) other;
      return file().equals(that.file()) && offset + length == that.start();
    } else {
      return false;
    }
  }

  @Override
  public SplitPositionDeletesScanTask merge(org.apache.iceberg.ScanTask other) {
    SplitPositionDeletesScanTask that = (SplitPositionDeletesScanTask) other;
    return new SplitPositionDeletesScanTask(parentTask, offset, length + that.length());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("file", file().path())
        .add("partition_data", file().partition())
        .add("offset", offset)
        .add("length", length)
        .add("residual", residual())
        .toString();
  }
}
