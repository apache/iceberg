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
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

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

  public boolean isBranch() {
    return type == SnapshotRefType.BRANCH;
  }

  public boolean isTag() {
    return type == SnapshotRefType.TAG;
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

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof SnapshotRef)) {
      return false;
    }

    SnapshotRef ref = (SnapshotRef) other;
    return ref.snapshotId == snapshotId
        && Objects.equals(ref.type(), type)
        && Objects.equals(ref.maxRefAgeMs(), maxRefAgeMs)
        && Objects.equals(ref.minSnapshotsToKeep(), minSnapshotsToKeep)
        && Objects.equals(ref.maxSnapshotAgeMs(), maxSnapshotAgeMs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.snapshotId,
        this.type,
        this.maxRefAgeMs,
        this.maxSnapshotAgeMs,
        this.minSnapshotsToKeep);
  }

  public static Builder tagBuilder(long snapshotId) {
    return builderFor(snapshotId, SnapshotRefType.TAG);
  }

  public static Builder branchBuilder(long snapshotId) {
    return builderFor(snapshotId, SnapshotRefType.BRANCH);
  }

  public static Builder builderFrom(SnapshotRef ref) {
    return new Builder(ref.type(), ref.snapshotId())
        .minSnapshotsToKeep(ref.minSnapshotsToKeep())
        .maxSnapshotAgeMs(ref.maxSnapshotAgeMs())
        .maxRefAgeMs(ref.maxRefAgeMs());
  }

  /**
   * Creates a ref builder from the given ref and its properties but the ref will now point to the
   * given snapshotId.
   *
   * @param ref Ref to build from
   * @param snapshotId snapshotID to use.
   * @return ref builder with the same retention properties as given ref, but the ref will point to
   *     the passed in id
   */
  public static Builder builderFrom(SnapshotRef ref, long snapshotId) {
    return new Builder(ref.type(), snapshotId)
        .minSnapshotsToKeep(ref.minSnapshotsToKeep())
        .maxSnapshotAgeMs(ref.maxSnapshotAgeMs())
        .maxRefAgeMs(ref.maxRefAgeMs());
  }

  public static Builder builderFor(long snapshotId, SnapshotRefType type) {
    return new Builder(type, snapshotId);
  }

  public static class Builder {

    private final SnapshotRefType type;
    private final long snapshotId;
    private Integer minSnapshotsToKeep;
    private Long maxSnapshotAgeMs;
    private Long maxRefAgeMs;

    Builder(SnapshotRefType type, long snapshotId) {
      Preconditions.checkArgument(type != null, "Snapshot reference type must not be null");
      this.type = type;
      this.snapshotId = snapshotId;
    }

    public Builder minSnapshotsToKeep(Integer value) {
      Preconditions.checkArgument(
          value == null || !type.equals(SnapshotRefType.TAG),
          "Tags do not support setting minSnapshotsToKeep");
      Preconditions.checkArgument(
          value == null || value > 0, "Min snapshots to keep must be greater than 0");
      this.minSnapshotsToKeep = value;
      return this;
    }

    public Builder maxSnapshotAgeMs(Long value) {
      Preconditions.checkArgument(
          value == null || !type.equals(SnapshotRefType.TAG),
          "Tags do not support setting maxSnapshotAgeMs");
      Preconditions.checkArgument(
          value == null || value > 0, "Max snapshot age must be greater than 0 ms");
      this.maxSnapshotAgeMs = value;
      return this;
    }

    public Builder maxRefAgeMs(Long value) {
      Preconditions.checkArgument(
          value == null || value > 0, "Max reference age must be greater than 0");
      this.maxRefAgeMs = value;
      return this;
    }

    public SnapshotRef build() {
      return new SnapshotRef(snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs);
    }
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
