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
import java.util.Locale;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * Checkpointable cursor for {@link LazyContinuousSplitPlanner}'s bulk-scan phase.
 *
 * <p>Only present while the lazy planner is paging through the initial {@code
 * TABLE_SCAN_THEN_INCREMENTAL} scan. {@code null} when an eager planner is used, or after the lazy
 * planner has completed the bulk phase and transitioned to incremental discovery.
 *
 * <p>The {@code rollingHash} field is a 64-bit FNV-1a hash folding in, for each emitted file scan
 * task in iteration order: the file's location (UTF-8 bytes), its {@code start} offset, its {@code
 * length}, and the location of every attached delete file. On restart the planner re-iterates the
 * snapshot's scan, recomputes the hash during the skip, and verifies it matches. Mismatch (an
 * Iceberg version upgrade that changes internal iteration order, or a scan-context option like
 * {@code splitSize} differing between incarnations) triggers a loud abort rather than silent data
 * loss. The hash is a drift detector, not a tampering defence.
 */
@Internal
public class LazyBulkScanCursor implements Serializable {

  private static final long serialVersionUID = 1L;

  private final long bulkSnapshotId;
  private final long combinedTasksEnumerated;
  private final long rollingHash;

  public LazyBulkScanCursor(long bulkSnapshotId, long combinedTasksEnumerated, long rollingHash) {
    this.bulkSnapshotId = bulkSnapshotId;
    this.combinedTasksEnumerated = combinedTasksEnumerated;
    this.rollingHash = rollingHash;
  }

  public long bulkSnapshotId() {
    return bulkSnapshotId;
  }

  public long combinedTasksEnumerated() {
    return combinedTasksEnumerated;
  }

  public long rollingHash() {
    return rollingHash;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("bulkSnapshotId", bulkSnapshotId)
        .add("combinedTasksEnumerated", combinedTasksEnumerated)
        .add("rollingHash", String.format(Locale.ROOT, "0x%016x", rollingHash))
        .toString();
  }
}
