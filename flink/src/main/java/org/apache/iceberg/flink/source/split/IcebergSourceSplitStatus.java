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

package org.apache.iceberg.flink.source.split;

import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Objects;

public class IcebergSourceSplitStatus implements Serializable {

  public enum Status {
    UNASSIGNED,
    ASSIGNED,
    COMPLETED
  }

  private final Status status;
  @Nullable
  private final Integer assignedSubtaskId;

  /**
   * The splits are frequently serialized into checkpoints.
   * Caching the byte representation makes repeated serialization cheap.
   */
  @Nullable private transient byte[] serializedFormCache;

  public IcebergSourceSplitStatus(Status status) {
    this(status, null);
  }

  public IcebergSourceSplitStatus(Status status, @Nullable Integer assignedSubtaskId) {
    this.status = status;
    this.assignedSubtaskId = assignedSubtaskId;
  }

  public Status status() {
    return status;
  }

  public Integer assignedSubtaskId() {
    return assignedSubtaskId;
  }

  public byte[] serializedFormCache() {
    return serializedFormCache;
  }

  public void serializedFormCache(byte[] cachedBytes) {
    this.serializedFormCache = cachedBytes;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        status,
        assignedSubtaskId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IcebergSourceSplitStatus other = (IcebergSourceSplitStatus) o;
    return Objects.equal(status, other.status()) &&
        Objects.equal(assignedSubtaskId, other.assignedSubtaskId());
  }
}
