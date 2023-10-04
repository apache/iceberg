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
package io.tabular.iceberg.connect.channel;

import static java.util.stream.Collectors.groupingBy;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.events.CommitReadyPayload;
import io.tabular.iceberg.connect.events.CommitResponsePayload;
import io.tabular.iceberg.connect.events.TopicPartitionOffset;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitState {
  private static final Logger LOG = LoggerFactory.getLogger(CommitState.class);

  private final List<Envelope> commitBuffer = new LinkedList<>();
  private final List<CommitReadyPayload> readyBuffer = new LinkedList<>();
  private long startTime;
  private UUID currentCommitId;
  private final IcebergSinkConfig config;

  public CommitState(IcebergSinkConfig config) {
    this.config = config;
  }

  public void addResponse(Envelope envelope) {
    commitBuffer.add(envelope);
    if (!isCommitInProgress()) {
      LOG.warn(
          "Received commit response when no commit in progress, this can happen during recovery");
    }
  }

  public void addReady(Envelope envelope) {
    readyBuffer.add((CommitReadyPayload) envelope.event().payload());
    if (!isCommitInProgress()) {
      LOG.warn("Received commit ready when no commit in progress, this can happen during recovery");
    }
  }

  public UUID currentCommitId() {
    return currentCommitId;
  }

  public boolean isCommitInProgress() {
    return currentCommitId != null;
  }

  public boolean isCommitIntervalReached() {
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }

    return (!isCommitInProgress()
        && System.currentTimeMillis() - startTime >= config.commitIntervalMs());
  }

  public void startNewCommit() {
    currentCommitId = UUID.randomUUID();
    startTime = System.currentTimeMillis();
  }

  public void endCurrentCommit() {
    readyBuffer.clear();
    currentCommitId = null;
  }

  public void clearResponses() {
    commitBuffer.clear();
  }

  public boolean isCommitTimedOut() {
    if (!isCommitInProgress()) {
      return false;
    }

    if (System.currentTimeMillis() - startTime > config.commitTimeoutMs()) {
      LOG.info("Commit timeout reached");
      return true;
    }
    return false;
  }

  public boolean isCommitReady(int expectedPartitionCount) {
    if (!isCommitInProgress()) {
      return false;
    }

    int receivedPartitionCount =
        readyBuffer.stream()
            .filter(payload -> payload.commitId().equals(currentCommitId))
            .mapToInt(payload -> payload.assignments().size())
            .sum();

    if (receivedPartitionCount >= expectedPartitionCount) {
      LOG.info(
          "Commit {} ready, received responses for all {} partitions",
          currentCommitId,
          receivedPartitionCount);
      return true;
    }

    LOG.info(
        "Commit {} not ready, received responses for {} of {} partitions, waiting for more",
        currentCommitId,
        receivedPartitionCount,
        expectedPartitionCount);

    return false;
  }

  public Map<TableIdentifier, List<Envelope>> tableCommitMap() {
    return commitBuffer.stream()
        .collect(
            groupingBy(
                envelope ->
                    ((CommitResponsePayload) envelope.event().payload())
                        .tableName()
                        .toIdentifier()));
  }

  public Long vtts(boolean partialCommit) {
    boolean validVtts =
        !partialCommit
            && readyBuffer.stream()
                .flatMap(event -> event.assignments().stream())
                .allMatch(offset -> offset.timestamp() != null);

    Long result;
    if (validVtts) {
      result =
          readyBuffer.stream()
              .flatMap(event -> event.assignments().stream())
              .mapToLong(TopicPartitionOffset::timestamp)
              .min()
              .getAsLong();
    } else {
      result = null;
    }
    return result;
  }
}
