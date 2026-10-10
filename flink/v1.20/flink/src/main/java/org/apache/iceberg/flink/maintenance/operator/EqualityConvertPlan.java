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
import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * Result of equality convert planning. Produced by {@code EqualityConvertPlanner} and consumed by
 * both {@code EqualityConvertDVWriter} (for partition info and DV merge gating) and {@code
 * EqualityConvertCommitter} (for data files and staging deletes to commit).
 *
 * <p>A no-op cycle is encoded by {@link #stagingSnapshotId()} == {@link
 * #NO_OP_STAGING_SNAPSHOT_ID}. Use {@link #noOp(Long, long, long)} to construct one and {@link
 * #noOp()} to check.
 *
 * @param dataFiles new staging data files committed in the cycle
 * @param stagingDVFiles staging DVs passed through to main, used by DVWriter to merge with
 *     newly-created DVs
 * @param eqDeleteFiles equality delete files resolved this cycle. Removed by the committer when
 *     staging and target are the same branch, so readers stop applying them once the equivalent DVs
 *     commit. Empty on a separate target branch, where the eq deletes remain on staging.
 * @param stagingSnapshotId staging snapshot the cycle resolved against, or {@link
 *     #NO_OP_STAGING_SNAPSHOT_ID} for a no-op cycle
 * @param mainSnapshotId main branch snapshot ID the index was resolved against, used for commit
 *     validation
 * @param triggerTimestamp original trigger timestamp, forwarded by the Committer to the Aggregator
 * @param doneTimestamp timestamp after which all phase watermarks have been emitted; the DVWriter
 *     should only process the result when the watermark reaches or exceeds this value
 */
@Internal
public record EqualityConvertPlan(
    List<DataFile> dataFiles,
    List<DeleteFile> stagingDVFiles,
    List<DeleteFile> eqDeleteFiles,
    long stagingSnapshotId,
    Long mainSnapshotId,
    long triggerTimestamp,
    long doneTimestamp)
    implements Serializable {

  public static final long NO_OP_STAGING_SNAPSHOT_ID = -1L;

  /** Returns {@code true} if this cycle has nothing to commit. */
  public boolean noOp() {
    return stagingSnapshotId == NO_OP_STAGING_SNAPSHOT_ID;
  }

  /** No-op cycle: empty file lists, sentinel staging snapshot id. */
  public static EqualityConvertPlan noOp(
      Long mainSnapshotId, long triggerTimestamp, long doneTimestamp) {
    return new EqualityConvertPlan(
        ImmutableList.of(),
        ImmutableList.of(),
        ImmutableList.of(),
        NO_OP_STAGING_SNAPSHOT_ID,
        mainSnapshotId,
        triggerTimestamp,
        doneTimestamp);
  }
}
