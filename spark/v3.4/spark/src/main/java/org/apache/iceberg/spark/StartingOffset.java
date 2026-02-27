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
package org.apache.iceberg.spark;

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Controls where a Spark structured streaming query begins reading when there is no existing
 * checkpoint.
 */
public enum StartingOffset {
  /** Start from the oldest snapshot (default behaviour). */
  EARLIEST("earliest"),
  /** Skip all existing snapshots; only process new data added after the stream starts. */
  LATEST("latest"),
  /**
   * Read all files in the oldest snapshot as a single micro-batch, then switch to added-files-only
   * incremental processing.
   */
  EARLIEST_SNAPSHOT("earliest-snapshot"),
  /**
   * Read all files in the current snapshot as a single micro-batch, then switch to added-files-only
   * incremental processing.
   */
  LATEST_SNAPSHOT("latest-snapshot");

  private final String modeName;

  StartingOffset(String modeName) {
    this.modeName = modeName;
  }

  public static StartingOffset fromName(String name) {
    Preconditions.checkArgument(name != null, "Invalid starting offset: null");
    for (StartingOffset offset : StartingOffset.values()) {
      if (offset.modeName.equalsIgnoreCase(name)) {
        return offset;
      }
    }

    throw new IllegalArgumentException(
        "Invalid streaming-start-from value: "
            + name
            + ". Expected one of: earliest, latest, earliest-snapshot, latest-snapshot");
  }

  /** Returns true if this mode should scan all files in the starting snapshot. */
  public boolean scanAllFiles() {
    return this == EARLIEST_SNAPSHOT || this == LATEST_SNAPSHOT;
  }

  /** Returns true if this mode should start from the latest (current) snapshot. */
  public boolean useLatest() {
    return this == LATEST || this == LATEST_SNAPSHOT;
  }
}
