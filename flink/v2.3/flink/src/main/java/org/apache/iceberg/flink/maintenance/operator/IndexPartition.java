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
package org.apache.iceberg.flink.maintenance.operator;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * A partition of the PK index: the partition spec id and the partition tuple encoded by {@link
 * StructLikeSerializer#encodePartition}, compared by content.
 */
@Internal
public record IndexPartition(int specId, byte[] partition) implements Serializable {

  static IndexPartition of(DVPosition position) {
    return new IndexPartition(position.specId(), position.partition());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof IndexPartition other)) {
      return false;
    }

    return specId == other.specId && Arrays.equals(partition, other.partition);
  }

  @Override
  public int hashCode() {
    return 31 * specId + Arrays.hashCode(partition);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("specId", specId)
        .add("partition", Arrays.toString(partition))
        .toString();
  }
}
