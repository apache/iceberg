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
package org.apache.iceberg.flink.sink;

import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotAncestryValidator;
import org.apache.iceberg.exceptions.ValidationException;

/**
 * Validates, inside the commit transaction, that the branch does not already contain a commit for
 * the staged checkpoint. This closes the race window between the up-front {@link
 * SinkUtil#getMaxCommittedCheckpointId} read and the commit itself: if a previous commit attempt
 * for the same checkpoint reached the catalog after the committer gave up (e.g. a client timeout on
 * a slow catalog commit) and the commit request is redelivered on recovery, the refreshed base
 * ancestry already contains a snapshot whose max-committed-checkpoint-id is &gt;= the staged
 * checkpoint, and the commit is rejected rather than duplicating the checkpoint's data.
 *
 * <p>Shared by both the non-dynamic {@link IcebergCommitter} and the {@code DynamicCommitter}.
 */
@Internal
public class MaxCommittedCheckpointIdValidator implements SnapshotAncestryValidator {

  private final long stagedCheckpointId;
  private final String flinkJobId;
  private final String flinkOperatorId;

  public MaxCommittedCheckpointIdValidator(
      long stagedCheckpointId, String flinkJobId, String flinkOperatorId) {
    this.stagedCheckpointId = stagedCheckpointId;
    this.flinkJobId = flinkJobId;
    this.flinkOperatorId = flinkOperatorId;
  }

  @Override
  public boolean validate(Iterable<Snapshot> baseSnapshots) {
    long maxCommittedCheckpointId = SinkUtil.INITIAL_CHECKPOINT_ID;
    for (Snapshot ancestor : baseSnapshots) {
      Map<String, String> summary = ancestor.summary();
      String snapshotFlinkJobId = summary.get(SinkUtil.FLINK_JOB_ID);
      String snapshotOperatorId = summary.get(SinkUtil.OPERATOR_ID);
      if (flinkJobId.equals(snapshotFlinkJobId)
          && (snapshotOperatorId == null || snapshotOperatorId.equals(flinkOperatorId))) {
        String value = summary.get(SinkUtil.MAX_COMMITTED_CHECKPOINT_ID);
        if (value != null) {
          maxCommittedCheckpointId = Long.parseLong(value);
          break;
        }
      }
    }

    if (maxCommittedCheckpointId >= stagedCheckpointId) {
      throw new MaxCommittedCheckpointMismatchException();
    }

    return true;
  }

  /**
   * Thrown from {@link #validate(Iterable)} when the branch already contains a commit for the
   * staged checkpoint. Committers catch this to skip the redelivered commit.
   */
  public static class MaxCommittedCheckpointMismatchException extends ValidationException {
    public MaxCommittedCheckpointMismatchException() {
      super("Table already contains staged changes.");
    }
  }
}
