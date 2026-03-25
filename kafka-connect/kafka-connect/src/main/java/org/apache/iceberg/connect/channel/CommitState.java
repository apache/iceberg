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
package org.apache.iceberg.connect.channel;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommitState implements CommitStateMXBean {
  private static final Logger LOG = LoggerFactory.getLogger(CommitState.class);

  private final List<Envelope> commitBuffer = Lists.newArrayList();
  private final List<DataComplete> readyBuffer = Lists.newArrayList();
  private final Map<UUID, Integer> groupRetryCount = Maps.newConcurrentMap();
  private long startTime;
  private UUID currentCommitId;
  private final IcebergSinkConfig config;

  CommitState(IcebergSinkConfig config) {
    this.config = config;
  }

  void addResponse(Envelope envelope) {
    commitBuffer.add(envelope);
    DataWritten dataWritten = (DataWritten) envelope.event().payload();
    if (!isCommitInProgress()) {
      LOG.warn(
          "Received commit response when no commit in progress, this can happen during recovery. Commit ID: {}",
          dataWritten.commitId());
    }
  }

  void addReady(Envelope envelope) {
    DataComplete dataComplete = (DataComplete) envelope.event().payload();
    readyBuffer.add(dataComplete);
    if (!isCommitInProgress()) {
      LOG.warn(
          "Received commit ready when no commit in progress, this can happen during recovery. Commit ID: {}",
          dataComplete.commitId());
    }
  }

  UUID currentCommitId() {
    return currentCommitId;
  }

  boolean isCommitInProgress() {
    return currentCommitId != null;
  }

  boolean isCommitIntervalReached() {
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }

    return (!isCommitInProgress()
        && System.currentTimeMillis() - startTime >= config.commitIntervalMs());
  }

  void startNewCommit() {
    // Do NOT clear commitBuffer. Stale events from prior failed or timed-out cycles are
    // retained for retry. Successfully committed groups are removed selectively by
    // removeEnvelopes() after each group's RowDelta commit succeeds.
    currentCommitId = UUID.randomUUID();
    startTime = System.currentTimeMillis();
  }

  void endCurrentCommit() {
    readyBuffer.clear();
    currentCommitId = null;
  }

  void clearResponses() {
    commitBuffer.clear();
  }

  /**
   * Removes only the specified envelopes from the commit buffer. Used after per-group RowDelta
   * commits to selectively drain successfully committed events while retaining events from failed
   * or skipped groups for retry next cycle.
   */
  void removeEnvelopes(Collection<Envelope> committed) {
    commitBuffer.removeAll(Sets.newHashSet(committed));

    // Clean up tracking maps for commitIds no longer in the buffer.
    Set<UUID> remainingIds =
        commitBuffer.stream()
            .map(env -> ((DataWritten) env.event().payload()).commitId())
            .collect(Collectors.toSet());
    groupRetryCount.keySet().retainAll(remainingIds);
  }

  void recordGroupFailure(UUID commitId) {
    groupRetryCount.merge(commitId, 1, Integer::sum);
  }

  void recordGroupSuccess(UUID commitId) {
    groupRetryCount.remove(commitId);
  }

  boolean isGroupBlocking(UUID commitId) {
    return groupRetryCount.getOrDefault(commitId, 0) <= config.commitStaleMaxBlockingRetries();
  }

  int getRetryCount(UUID commitId) {
    return groupRetryCount.getOrDefault(commitId, 0);
  }

  boolean isBufferEmpty() {
    return commitBuffer.isEmpty();
  }

  boolean isCommitTimedOut() {
    if (!isCommitInProgress()) {
      return false;
    }

    if (System.currentTimeMillis() - startTime > config.commitTimeoutMs()) {
      LOG.info("Commit timeout reached. Commit ID: {}", currentCommitId);
      return true;
    }
    return false;
  }

  boolean isCommitReady(int expectedPartitionCount) {
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

  // ── MXBean interface methods ──

  @Override
  public int getStaleGroupCount() {
    return staleGroupCount();
  }

  @Override
  public int getBufferSize() {
    return bufferSize();
  }

  // ── Internal accessors ──

  int staleGroupCount() {
    if (currentCommitId == null) {
      return 0;
    }
    return (int)
        commitBuffer.stream()
            .map(env -> ((DataWritten) env.event().payload()).commitId())
            .filter(cid -> !cid.equals(currentCommitId))
            .distinct()
            .count();
  }

  int bufferSize() {
    return commitBuffer.size();
  }

  /**
   * Returns the minimum control topic offset per partition among uncommitted envelopes remaining in
   * the buffer. Used for partial consumer offset advancement: advancing to these offsets ensures
   * uncommitted events survive a restart while already-committed events are not re-consumed.
   */
  Map<Integer, Long> remainingEnvelopeMinOffsets() {
    Map<Integer, Long> minOffsets = Maps.newHashMap();
    for (Envelope env : commitBuffer) {
      minOffsets.merge(env.partition(), env.offset(), Long::min);
    }
    return minOffsets;
  }

  /**
   * Returns commit buffer entries grouped by table, separated by commitId.
   *
   * <p>After a partial commit (timeout), late-arriving DataWritten events from the previous cycle
   * may be present in the buffer alongside current cycle events. Merging them into a single
   * RowDelta would assign the same sequence number to both, breaking equality delete semantics
   * (which require {@code data_sequence_number < delete_sequence_number}).
   *
   * <p>This method separates entries by commitId and returns them as an ordered list: stale
   * commitIds first (in control topic consumption order), current commitId last. Each map in the
   * list should be committed in a separate RowDelta to preserve sequence number ordering.
   */
  List<Map<TableReference, List<Envelope>>> tableCommitMaps() {
    // LinkedHashMap preserves insertion order from control topic consumption
    Map<UUID, List<Envelope>> byCommitId = new LinkedHashMap<>();
    for (Envelope envelope : commitBuffer) {
      UUID commitId = ((DataWritten) envelope.event().payload()).commitId();
      byCommitId.computeIfAbsent(commitId, k -> Lists.newArrayList()).add(envelope);
    }

    List<Map<TableReference, List<Envelope>>> result = Lists.newArrayList();

    // Stale commitIds first (in control topic consumption order)
    for (Map.Entry<UUID, List<Envelope>> entry : byCommitId.entrySet()) {
      UUID commitId = entry.getKey();
      if (currentCommitId != null && commitId.equals(currentCommitId)) {
        continue;
      }
      LOG.warn(
          "Stale DataWritten detected: commitId={} (current={}), envelopes={}, "
              + "will commit in separate RowDelta to preserve sequence number ordering",
          commitId,
          currentCommitId,
          entry.getValue().size());
      result.add(toTableMap(entry.getValue()));
    }

    // Current commitId last — ensures highest sequence number
    if (currentCommitId != null) {
      List<Envelope> currentEnvelopes =
          byCommitId.getOrDefault(currentCommitId, Lists.newArrayList());
      if (!currentEnvelopes.isEmpty()) {
        result.add(toTableMap(currentEnvelopes));
      }
    }

    return result;
  }

  /**
   * Groups commit buffer entries by table, then by commitId within each table. CommitId groups are
   * ordered by first appearance in the buffer (insertion order), which matches control topic
   * consumption order. This ensures stale groups from prior failed or timed-out cycles sort before
   * the current cycle's group.
   *
   * <p>Each group becomes a separate RowDelta commit with its own Iceberg sequence number, allowing
   * equality deletes from newer groups to apply to data files from older groups.
   */
  Map<TableReference, List<CommitGroup>> tableCommitGroups() {
    Map<TableReference, List<Envelope>> byTable =
        commitBuffer.stream()
            .collect(
                Collectors.groupingBy(
                    envelope -> ((DataWritten) envelope.event().payload()).tableReference()));

    Map<TableReference, List<CommitGroup>> result = new LinkedHashMap<>();
    for (Map.Entry<TableReference, List<Envelope>> entry : byTable.entrySet()) {
      Map<UUID, List<Envelope>> byCommitId = new LinkedHashMap<>();
      for (Envelope env : entry.getValue()) {
        UUID cid = ((DataWritten) env.event().payload()).commitId();
        byCommitId.computeIfAbsent(cid, k -> Lists.newArrayList()).add(env);
      }
      List<CommitGroup> groups =
          byCommitId.entrySet().stream()
              .map(e -> new CommitGroup(e.getKey(), e.getValue()))
              .collect(Collectors.toList());
      result.put(entry.getKey(), groups);
    }
    return result;
  }

  static class CommitGroup {
    private final UUID commitId;
    private final List<Envelope> envelopes;

    CommitGroup(UUID commitId, List<Envelope> envelopes) {
      this.commitId = commitId;
      this.envelopes = envelopes;
    }

    UUID commitId() {
      return commitId;
    }

    List<Envelope> envelopes() {
      return envelopes;
    }
  }

  private Map<TableReference, List<Envelope>> toTableMap(List<Envelope> envelopes) {
    return envelopes.stream()
        .collect(
            Collectors.groupingBy(
                envelope -> ((DataWritten) envelope.event().payload()).tableReference()));
  }

  OffsetDateTime validThroughTs(boolean partialCommit) {
    boolean hasValidThroughTs =
        !partialCommit
            && readyBuffer.stream()
                .flatMap(event -> event.assignments().stream())
                .allMatch(offset -> offset.timestamp() != null);

    OffsetDateTime result;
    if (hasValidThroughTs) {
      result =
          readyBuffer.stream()
              .flatMap(event -> event.assignments().stream())
              .map(TopicPartitionOffset::timestamp)
              .min(Comparator.naturalOrder())
              .orElse(null);
    } else {
      result = null;
    }
    return result;
  }
}
