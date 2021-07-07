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

import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class BaseRewriteDataFilesFileGroupInfo implements RewriteDataFiles.FileGroupInfo {
  private final int globalIndex;
  private final int partitionIndex;
  private final StructLike partition;

  public BaseRewriteDataFilesFileGroupInfo(int globalIndex, int partitionIndex, StructLike partition) {
    this.globalIndex = globalIndex;
    this.partitionIndex = partitionIndex;
    this.partition = partition;
  }

  @Override
  public int globalIndex() {
    return globalIndex;
  }

  @Override
  public int partitionIndex() {
    return partitionIndex;
  }

  @Override
  public StructLike partition() {
    return partition;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("globalIndex", globalIndex)
        .add("partitionIndex", partitionIndex)
        .add("partition", partition)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BaseRewriteDataFilesFileGroupInfo that = (BaseRewriteDataFilesFileGroupInfo) o;

    if (globalIndex != that.globalIndex) {
      return false;
    }
    if (partitionIndex != that.partitionIndex) {
      return false;
    }
    return partition.equals(that.partition);
  }

  @Override
  public int hashCode() {
    int result = globalIndex;
    result = 31 * result + partitionIndex;
    result = 31 * result + partition.hashCode();
    return result;
  }
}
