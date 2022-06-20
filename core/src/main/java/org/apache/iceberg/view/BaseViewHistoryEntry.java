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
package org.apache.iceberg.view;

import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class BaseViewHistoryEntry implements ViewHistoryEntry {
  private final long timestampMillis;
  private final long versionId;

  static ViewHistoryEntry of(long timestampMillis, long versionId) {
    return new BaseViewHistoryEntry(timestampMillis, versionId);
  }

  private BaseViewHistoryEntry(long timestampMillis, long versionId) {
    this.timestampMillis = timestampMillis;
    this.versionId = versionId;
  }

  @Override
  public long timestampMillis() {
    return timestampMillis;
  }

  @Override
  public long versionId() {
    return versionId;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    BaseViewHistoryEntry that = (BaseViewHistoryEntry) other;
    return timestampMillis == that.timestampMillis && versionId == that.versionId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestampMillis, versionId);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("timestampMillis", timestampMillis)
        .add("versionId", versionId)
        .toString();
  }
}
