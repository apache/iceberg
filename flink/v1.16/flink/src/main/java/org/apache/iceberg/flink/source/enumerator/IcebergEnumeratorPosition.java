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
package org.apache.iceberg.flink.source.enumerator;

import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;

class IcebergEnumeratorPosition {
  private final Long snapshotId;
  // Track snapshot timestamp mainly for info logging
  private final Long snapshotTimestampMs;

  static IcebergEnumeratorPosition empty() {
    return new IcebergEnumeratorPosition(null, null);
  }

  static IcebergEnumeratorPosition of(long snapshotId, Long snapshotTimestampMs) {
    return new IcebergEnumeratorPosition(snapshotId, snapshotTimestampMs);
  }

  private IcebergEnumeratorPosition(Long snapshotId, Long snapshotTimestampMs) {
    this.snapshotId = snapshotId;
    this.snapshotTimestampMs = snapshotTimestampMs;
  }

  boolean isEmpty() {
    return snapshotId == null;
  }

  Long snapshotId() {
    return snapshotId;
  }

  Long snapshotTimestampMs() {
    return snapshotTimestampMs;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("snapshotId", snapshotId)
        .add("snapshotTimestampMs", snapshotTimestampMs)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(snapshotId, snapshotTimestampMs);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IcebergEnumeratorPosition other = (IcebergEnumeratorPosition) o;
    return Objects.equal(snapshotId, other.snapshotId())
        && Objects.equal(snapshotTimestampMs, other.snapshotTimestampMs());
  }
}
