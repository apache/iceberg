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
package org.apache.iceberg.flink.sink.committer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.NavigableMap;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the Flink SinkV2 {@link Committer} interface to implement the Iceberg
 * commits. The implementation builds on the following assumptions:
 *
 * <ul>
 *   <li>There is a single {@link SinkV2Committable} for every checkpoint
 *   <li>There is no late checkpoint - if checkpoint 'x' has received in one call, then after a
 *       successful run only checkpoints &gt; x will arrive
 *   <li>There is no other writer which would generate another commit to the same branch with the
 *       same jobId-operatorId-checkpointId triplet
 * </ul>
 *
 * The implementation delegates most of the commit responsibilities to the {@link CommonCommitter},
 * so the implementation could be shared between {@link org.apache.iceberg.flink.sink.FlinkSink}
 * (SinkV1) and the {@link org.apache.iceberg.flink.sink.IcebergSink} (SinkV2).
 *
 * <p>Missing features from the Flink Sink V2 API:
 *
 * <ul>
 *   <li>No metrics access - The SinkV2 implementation will not emit committer metrics for now
 *   <li>No init method - The {@link CommonCommitter} object init called on every commit
 *   <li>Committer parallelism could not be set - There will be running instances of the Committer
 *       which does not receive any events
 * </ul>
 */
public class SinkV2Committer implements Committer<SinkV2Committable>, Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(SinkV2Committer.class);
  private final CommonCommitter commonCommitter;

  public SinkV2Committer(CommonCommitter commonCommitter) {
    this.commonCommitter = commonCommitter;
  }

  @Override
  public void commit(Collection<CommitRequest<SinkV2Committable>> commitRequests)
      throws IOException, InterruptedException {
    if (commitRequests.isEmpty()) {
      LOG.info("Noting to commit");
      return;
    }

    // Make sure that the committer is initialized
    commonCommitter.init(null);

    NavigableMap<Long, byte[]> manifestMap = Maps.newTreeMap();
    NavigableMap<Long, CommitRequest<SinkV2Committable>> commitRequestMap = Maps.newTreeMap();

    for (CommitRequest<SinkV2Committable> request : commitRequests) {
      manifestMap.put(request.getCommittable().checkpointId(), request.getCommittable().manifest());
      commitRequestMap.put(request.getCommittable().checkpointId(), request);
    }

    long maxCommittedCheckpointId = commonCommitter.getMaxCommittedCheckpointId(commitRequestMap);

    // Mark the already committed FilesCommittable(s) as finished
    commitRequestMap
        .headMap(maxCommittedCheckpointId, true)
        .values()
        .forEach(CommitRequest::signalAlreadyCommitted);

    // Commit the remaining
    SinkV2Committable last = commitRequestMap.lastEntry().getValue().getCommittable();

    NavigableMap<Long, byte[]> uncommitted =
        Maps.newTreeMap(manifestMap).tailMap(maxCommittedCheckpointId, false);
    if (!uncommitted.isEmpty()) {
      commonCommitter.commitUpToCheckpoint(
          uncommitted, last.jobId(), last.operatorId(), last.checkpointId());
    }
  }

  @Override
  public void close() {
    // Nothing to do
  }
}
