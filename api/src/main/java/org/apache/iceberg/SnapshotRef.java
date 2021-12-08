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

import java.io.Serializable;
import java.util.Objects;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class SnapshotRef implements Serializable {

  public static final String MAIN_BRANCH = "main";

  private final long snapshotId;
  private final SnapshotRefType type;
  private final Integer minSnapshotsToKeep;
  private final Long maxSnapshotAgeMs;
  private final Long maxRefAgeMs;

  private SnapshotRef(
      long snapshotId,
      SnapshotRefType type,
      Integer minSnapshotsToKeep,
      Long maxSnapshotAgeMs,
      Long maxRefAgeMs) {
    this.snapshotId = snapshotId;
    this.type = type;
    this.minSnapshotsToKeep = minSnapshotsToKeep;
    this.maxSnapshotAgeMs = maxSnapshotAgeMs;
    this.maxRefAgeMs = maxRefAgeMs;
  }

  public long snapshotId() {
    return snapshotId;
  }

  public SnapshotRefType type() {
    return type;
  }

  public Integer minSnapshotsToKeep() {
    return minSnapshotsToKeep;
  }

  public Long maxSnapshotAgeMs() {
    return maxSnapshotAgeMs;
  }

  public Long maxRefAgeMs() {
    return maxRefAgeMs;
  }

  public static Builder builderForTag(long snapshotId) {
    return builderFor(snapshotId, SnapshotRefType.TAG);
  }

  public static Builder builderForBranch(long snapshotId) {
    return builderFor(snapshotId, SnapshotRefType.BRANCH);
  }

  public static Builder builderFrom(SnapshotRef ref) {
    return new Builder(ref.type())
        .snapshotId(ref.snapshotId())
        .minSnapshotsToKeep(ref.minSnapshotsToKeep())
        .maxSnapshotAgeMs(ref.maxSnapshotAgeMs())
        .maxRefAgeMs(ref.maxRefAgeMs());
  }

  public static Builder builderFor(long snapshotId, SnapshotRefType type) {
    return new Builder(type).snapshotId(snapshotId);
  }

  public static class Builder {

    private final SnapshotRefType type;

    private Long snapshotId;
    private Integer minSnapshotsToKeep;
    private Long maxSnapshotAgeMs;
    private Long maxRefAgeMs;

    Builder(SnapshotRefType type) {
      ValidationException.check(type != null, "Snapshot reference type must not be null");
      this.type = type;
    }

    public Builder snapshotId(long id) {
      this.snapshotId = id;
      return this;
    }

    public Builder minSnapshotsToKeep(Integer value) {
      this.minSnapshotsToKeep = value;
      return this;
    }

    public Builder maxSnapshotAgeMs(Long value) {
      this.maxSnapshotAgeMs = value;
      return this;
    }

    public Builder maxRefAgeMs(Long value) {
      this.maxRefAgeMs = value;
      return this;
    }

    public SnapshotRef build() {
      if (type.equals(SnapshotRefType.TAG)) {
        ValidationException.check(minSnapshotsToKeep == null,
            "TAG type snapshot reference does not support setting minSnapshotsToKeep");
        ValidationException.check(maxSnapshotAgeMs == null,
            "TAG type snapshot reference does not support setting maxSnapshotAgeMs");
      } else {
        if (minSnapshotsToKeep != null) {
          ValidationException.check(minSnapshotsToKeep > 0,
              "Min snapshots to keep must be greater than 0");
        }

        if (maxSnapshotAgeMs != null) {
          ValidationException.check(maxSnapshotAgeMs > 0, "Max snapshot age must be greater than 0");
        }
      }

      if (maxRefAgeMs != null) {
        ValidationException.check(maxRefAgeMs > 0, "Max reference age must be greater than 0");
      }

      return new SnapshotRef(snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)  {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SnapshotRef that = (SnapshotRef) o;
    return snapshotId == that.snapshotId &&
        type == that.type &&
        Objects.equals(minSnapshotsToKeep, that.minSnapshotsToKeep) &&
        Objects.equals(maxSnapshotAgeMs, that.maxSnapshotAgeMs) &&
        Objects.equals(maxRefAgeMs, that.maxRefAgeMs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("snapshotId", snapshotId)
        .add("type", type)
        .add("minSnapshotsToKeep", minSnapshotsToKeep)
        .add("maxSnapshotAgeMs", maxSnapshotAgeMs)
        .add("maxRefAgeMs", maxRefAgeMs)
        .toString();
  }
}
