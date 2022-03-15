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

import java.io.Serializable;
import java.time.Duration;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * Settings for continuous split enumeration
 */
public class IcebergEnumeratorConfig implements Serializable {
  private static final long serialVersionUID = 1L;

  public enum StartingStrategy {
    /**
     * First do a regular table scan.
     * then switch to incremental mode.
     */
    TABLE_SCAN_THEN_INCREMENTAL,

    /**
     * Start incremental mode from the latest snapshot
     */
    LATEST_SNAPSHOT,

    /**
     * Start incremental mode from the earliest snapshot
     */
    EARLIEST_SNAPSHOT,

    /**
     * Start incremental mode from a specific startSnapshotId
     */
    SPECIFIC_START_SNAPSHOT_ID,

    /**
     * Start incremental mode from a specific startTimestamp.
     * Starting snapshot has a timestamp lower than or equal to the specified timestamp.
     */
    SPECIFIC_START_SNAPSHOT_TIMESTAMP
  }

  // for continuous enumerator
  private final Duration splitDiscoveryInterval;
  private final StartingStrategy startingStrategy;
  private final Long startSnapshotId;
  private final Long startSnapshotTimeMs;

  private IcebergEnumeratorConfig(Builder builder) {
    this.splitDiscoveryInterval = builder.splitDiscoveryInterval;
    this.startingStrategy = builder.startingStrategy;
    this.startSnapshotId = builder.startSnapshotId;
    this.startSnapshotTimeMs = builder.startSnapshotTimeMs;
  }

  public Duration splitDiscoveryInterval() {
    return splitDiscoveryInterval;
  }

  public StartingStrategy startingStrategy() {
    return startingStrategy;
  }

  public Long startSnapshotId() {
    return startSnapshotId;
  }

  public Long startSnapshotTimeMs() {
    return startSnapshotTimeMs;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("splitDiscoveryInterval", splitDiscoveryInterval)
        .add("startingStrategy", startingStrategy)
        .add("startSnapshotId", startSnapshotId)
        .add("startSnapshotTimeMs", startSnapshotTimeMs)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Duration splitDiscoveryInterval;
    private StartingStrategy startingStrategy = StartingStrategy.LATEST_SNAPSHOT;
    private Long startSnapshotId;
    private Long startSnapshotTimeMs;

    private Builder() {
    }

    /**
     * This is required for continuous enumerator in streaming read
     */
    public Builder splitDiscoveryInterval(Duration interval) {
      this.splitDiscoveryInterval = interval;
      return this;
    }

    public Builder startingStrategy(StartingStrategy strategy) {
      this.startingStrategy = strategy;
      return this;
    }

    public Builder startSnapshotId(long startId) {
      this.startSnapshotId = startId;
      return this;
    }

    public Builder startSnapshotTimeMs(long startTimeMs) {
      this.startSnapshotTimeMs = startTimeMs;
      return this;
    }

    public IcebergEnumeratorConfig build() {
      checkRequired();
      return new IcebergEnumeratorConfig(this);
    }

    private void checkRequired() {
      // continuous enumerator
      if (splitDiscoveryInterval != null) {
        switch (startingStrategy) {
          case SPECIFIC_START_SNAPSHOT_ID:
            Preconditions.checkNotNull(startSnapshotId,
                "Must set startSnapshotId with starting strategy: " + startingStrategy);
            break;
          case SPECIFIC_START_SNAPSHOT_TIMESTAMP:
            Preconditions.checkNotNull(startSnapshotTimeMs,
                "Must set startSnapshotTimeMs with starting strategy: " + startingStrategy);
            break;
          default:
            break;
        }
      }
    }
  }
}
