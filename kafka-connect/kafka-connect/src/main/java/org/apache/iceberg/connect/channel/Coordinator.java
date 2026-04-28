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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.management.ObjectName;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotAncestryValidator;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Coordinator extends Channel {

  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
  private static final String TASK_ID_SNAPSHOT_PROP = "kafka.connect.task-id";
  private static final String VALID_THROUGH_TS_SNAPSHOT_PROP = "kafka.connect.valid-through-ts";
  private static final Duration POLL_DURATION = Duration.ofSeconds(1);

  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final int totalPartitionCount;
  private final String snapshotOffsetsProp;
  private final ExecutorService exec;
  private final CommitState commitState;
  private final AtomicLong partialCommitFailures = new AtomicLong();
  private final ObjectName commitStateMBeanName;
  private volatile boolean terminated;
  private final String taskId;

  Coordinator(
      Catalog catalog,
      IcebergSinkConfig config,
      Collection<MemberDescription> members,
      KafkaClientFactory clientFactory,
      SinkTaskContext context) {
    // pass consumer group ID to which we commit low watermark offsets
    super("coordinator", config.connectGroupId() + "-coord", config, clientFactory, context);

    this.catalog = catalog;
    this.config = config;
    this.totalPartitionCount =
        members.stream().mapToInt(desc -> desc.assignment().topicPartitions().size()).sum();
    this.snapshotOffsetsProp =
        String.format(
            "kafka.connect.offsets.%s.%s", config.controlTopic(), config.connectGroupId());
    this.exec =
        new ThreadPoolExecutor(
            config.commitThreads(),
            config.commitThreads(),
            config.keepAliveTimeoutInMs(),
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("iceberg-committer" + "-%d")
                .build());
    this.commitState = new CommitState(config);
    this.taskId = config.connectorName() + "-" + config.taskId();
    this.commitStateMBeanName = registerCommitStateMBean(taskId);
  }

  private ObjectName registerCommitStateMBean(String connectorName) {
    try {
      ObjectName name =
          new ObjectName(
              String.format(
                  "org.apache.iceberg.connect:type=CommitState,connector=%s,id=%s",
                  connectorName, System.identityHashCode(this)));
      ManagementFactory.getPlatformMBeanServer().registerMBean(commitState, name);
      return name;
    } catch (Exception e) {
      LOG.warn("Failed to register CommitState MBean, metrics will not be available via JMX", e);
      return null;
    }
  }

  void process() {
    if (commitState.isCommitIntervalReached()) {
      // send out begin commit
      commitState.startNewCommit();
      Event event =
          new Event(config.connectGroupId(), new StartCommit(commitState.currentCommitId()));
      send(event);
      LOG.info("Coordinator {} initiated commit {}", taskId, commitState.currentCommitId());
    }

    consumeAvailable(POLL_DURATION);

    if (commitState.isCommitTimedOut()) {
      commit(true);
    }
  }

  @Override
  protected boolean receive(Envelope envelope) {
    switch (envelope.event().payload().type()) {
      case DATA_WRITTEN:
        commitState.addResponse(envelope);
        return true;
      case DATA_COMPLETE:
        commitState.addReady(envelope);
        if (commitState.isCommitReady(totalPartitionCount)) {
          commit(false);
        }
        return true;
    }
    return false;
  }

  private void commit(boolean partialCommit) {
    try {
      doCommit(partialCommit);
    } catch (ConnectException e) {
      // Deliberate failure (e.g., stale group retries exhausted) — must propagate
      // to stop the connector. CoordinatorThread catches this and terminates.
      throw e;
    } catch (RuntimeException e) {
      if (partialCommit) {
        partialCommitFailures.incrementAndGet();
        LOG.warn(
            "Partial commit {} failed for task {}",
            commitState.currentCommitId(),
            taskId,
            e);
      } else {
        LOG.warn(
            "Commit {} failed for task {}",
            commitState.currentCommitId(),
            taskId,
            e);
      }
    } catch (Exception e) {
      LOG.warn("Commit failed, will try again next cycle", e);
    } finally {
      commitState.endCurrentCommit();
    }
  }

  private void doCommit(boolean partialCommit) {
    Map<TableReference, List<CommitState.CommitGroup>> commitGroups =
        commitState.tableCommitGroups();
    OffsetDateTime validThroughTs = commitState.validThroughTs(partialCommit);

    if (commitGroups.isEmpty()) {
      handleEmptyCommit(validThroughTs);
      return;
    }

    Map<Integer, Long> ctlOffsets = controlTopicOffsets();
    CommitCycleResult result = runTableCommits(commitGroups, ctlOffsets, validThroughTs);
    finalizeCommitCycle(commitGroups, result, validThroughTs);
  }

  private void handleEmptyCommit(OffsetDateTime validThroughTs) {
    LOG.info(
        "Coordinator {} found nothing to commit for commit {}",
        taskId,
        commitState.currentCommitId());
    commitConsumerOffsets();
    commitState.clearResponses();
    Event event =
        new Event(
            config.connectGroupId(),
            new CommitComplete(commitState.currentCommitId(), validThroughTs));
    send(event);
  }

  private record CommitCycleResult(
      List<Envelope> committedEnvelopes, RuntimeException taskException) {}

  private CommitCycleResult runTableCommits(
      Map<TableReference, List<CommitState.CommitGroup>> commitGroups,
      Map<Integer, Long> ctlOffsets,
      OffsetDateTime validThroughTs) {
    // Track successfully committed envelopes for selective removal.
    // Synchronized because table commits run in parallel via the exec thread pool.
    List<Envelope> committedEnvelopes = Collections.synchronizedList(Lists.newArrayList());

    // Outer: tables in parallel (via exec thread pool).
    // Inner: commitId groups sequentially per table, oldest first.
    // If a group fails for a table, remaining groups for that table are skipped
    // to preserve sequence number ordering. Other tables are unaffected.
    // Capture exception from parallel table commits so cleanup always runs.
    // Without this, a ConnectException from one table's exhausted retries would
    // skip removeEnvelopes(), leaving successfully committed envelopes in the buffer
    // indefinitely (memory leak + wasted retry work on subsequent cycles).
    RuntimeException taskException = null;
    try {
      Tasks.foreach(commitGroups.entrySet())
          .executeWith(exec)
          .run(
              entry -> {
                TableReference tableRef = entry.getKey();
                List<CommitState.CommitGroup> groups = entry.getValue();

                for (int i = 0; i < groups.size(); i++) {
                  CommitState.CommitGroup group = groups.get(i);
                  boolean isCurrentGroup = group.commitId().equals(commitState.currentCommitId());

                  // Stale groups must only write their own envelope offsets to the snapshot,
                  // not the global consumer position. Otherwise the global offsets "poison"
                  // subsequent groups: their envelopes appear already-committed and get
                  // filtered out. The current (last) group writes the global offsets.
                  Map<Integer, Long> groupOffsets;
                  if (isCurrentGroup) {
                    groupOffsets = ctlOffsets;
                  } else {
                    groupOffsets = Maps.newHashMap();
                    for (Envelope env : group.envelopes()) {
                      groupOffsets.merge(env.partition(), env.offset() + 1, Long::max);
                    }
                  }

                  try {
                    commitToTable(
                        tableRef,
                        group.envelopes(),
                        group.commitId(),
                        groupOffsets,
                        isCurrentGroup ? validThroughTs : null);
                    committedEnvelopes.addAll(group.envelopes());
                    commitState.recordGroupSuccess(group.commitId());
                  } catch (Exception e) {
                    commitState.recordGroupFailure(group.commitId());
                    int remaining = groups.size() - i - 1;

                    if (!isCurrentGroup && !commitState.isRetryAllowed(group.commitId())) {
                      // Stale group exceeded max blocking retries - fail the connector.
                      throw new ConnectException(
                          "Stale group "
                              + group.commitId()
                              + " for table "
                              + tableRef.identifier()
                              + " failed after "
                              + commitState.getRetryCount(group.commitId())
                              + " retries. Connector stopping.",
                          e);
                    }

                    // Blocking: skip remaining groups to preserve ordering.
                    LOG.warn(
                        "Commit failed for table {} group {} ({}, attempt {}), "
                            + "skipping {} remaining group(s) for this table",
                        tableRef.identifier(),
                        group.commitId(),
                        isCurrentGroup ? "current" : "stale",
                        commitState.getRetryCount(group.commitId()),
                        remaining,
                        e);
                    break;
                  }
                }
              });
    } catch (RuntimeException e) {
      taskException = e;
    }
    return new CommitCycleResult(committedEnvelopes, taskException);
  }

  private void finalizeCommitCycle(
      Map<TableReference, List<CommitState.CommitGroup>> commitGroups,
      CommitCycleResult result,
      OffsetDateTime validThroughTs) {
    List<Envelope> committedEnvelopes = result.committedEnvelopes();

    // Remove only the envelopes whose groups committed successfully.
    if (!committedEnvelopes.isEmpty()) {
      commitState.removeEnvelopes(committedEnvelopes);
    }

    if (committedEnvelopes.isEmpty() && !commitGroups.isEmpty()) {
      LOG.error(
          "Commit {} failed: all groups across all tables failed to commit. "
              + "{} group(s) remain in buffer for retry.",
          commitState.currentCommitId(),
          commitGroups.values().stream().mapToInt(List::size).sum());
    }

    // Advance consumer offsets and send CommitComplete only when the buffer is fully
    // drained. If any groups remain (failed + their skipped successors), consumer
    // offsets must NOT advance — those envelopes need to survive a restart.
    if (commitState.isBufferEmpty()) {
      commitConsumerOffsets();
      Event event =
          new Event(
              config.connectGroupId(),
              new CommitComplete(commitState.currentCommitId(), validThroughTs));
      send(event);

      LOG.info(
        "Coordinator {} completed commit {} complete, committed to {} table(s) in {} batch(es), valid-through {}",
        taskId,
        commitState.currentCommitId(),
        tableCount,
        commitMaps.size(),
        validThroughTs);

    } else {
      // Advance consumer offsets to the minimum uncommitted envelope offset per partition.
      // This bounds re-consumption on restart to only uncommitted events, while ensuring
      // those events survive the restart.
      Map<Integer, Long> minUncommitted = commitState.remainingEnvelopeMinOffsets();
      Map<Integer, Long> safeOffsets = Maps.newHashMap(controlTopicOffsets());
      minUncommitted.forEach(safeOffsets::put);
      commitConsumerOffsetsTo(safeOffsets);

      LOG.warn(
          "Commit {} partially complete, {} envelopes remain for retry. "
              + "Consumer offsets advanced to min uncommitted: {}",
          commitState.currentCommitId(),
          commitState.bufferSize(),
          minUncommitted);
    }

    // Re-throw after cleanup so the commit() wrapper can log it.
    if (result.taskException() != null) {
      throw result.taskException();
    }
  }

  private String offsetsToJson(Map<Integer, Long> offsets) {
    try {
      return MAPPER.writeValueAsString(offsets);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private void commitToTable(
      TableReference tableReference,
      List<Envelope> envelopeList,
      UUID commitId,
      Map<Integer, Long> controlTopicOffsets,
      OffsetDateTime validThroughTs) {
    TableIdentifier tableIdentifier = tableReference.identifier();
    Table table;
    try {
      table = catalog.loadTable(tableIdentifier);
    } catch (NoSuchTableException e) {
      LOG.warn("Table not found, skipping commit: {}", tableIdentifier, e);
      return;
    }

    if (tableReference.uuid() != null && !tableReference.uuid().equals(table.uuid())) {
      LOG.warn(
          "Skipping commits to table {} due to target table mismatch.  Expected: {} Received: {}",
          tableIdentifier,
          table.uuid(),
          tableReference.uuid());
      return;
    }

    String branch = config.tableConfig(tableIdentifier.toString()).commitBranch();

    // Control topic partition offsets may include a subset of partition ids if there were no
    // records for other partitions.  Merge the updated topic partitions with the last committed
    // offsets.
    Map<Integer, Long> committedOffsets = lastCommittedOffsetsForTable(table, branch);
    Map<Integer, Long> mergedOffsets =
        Stream.of(committedOffsets, controlTopicOffsets)
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::max));
    String offsetsJson = offsetsToJson(mergedOffsets);

    List<DataWritten> payloads =
        envelopeList.stream()
            .filter(
                envelope -> {
                  Long minOffset = committedOffsets.get(envelope.partition());
                  return minOffset == null || envelope.offset() >= minOffset;
                })
            .map(envelope -> (DataWritten) envelope.event().payload())
            .collect(Collectors.toList());

    List<DataFile> dataFiles =
        payloads.stream()
            .filter(payload -> payload.dataFiles() != null)
            .flatMap(payload -> payload.dataFiles().stream())
            .filter(dataFile -> dataFile.recordCount() > 0)
            .filter(distinctByKey(ContentFile::location))
            .collect(Collectors.toList());

    List<DeleteFile> deleteFiles =
        payloads.stream()
            .filter(payload -> payload.deleteFiles() != null)
            .flatMap(payload -> payload.deleteFiles().stream())
            .filter(deleteFile -> deleteFile.recordCount() > 0)
            .filter(distinctByKey(ContentFile::location))
            .collect(Collectors.toList());

    if (terminated) {
      throw new ConnectException(
          String.format("Coordinator %s is terminated, commit aborted", taskId));
    }

    if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
      LOG.info(
          "Coordinator {} found nothing to commit to table {}, skipping", taskId, tableIdentifier);
    } else {
      if (deleteFiles.isEmpty()) {
        AppendFiles appendOp =
            table.newAppend().validateWith(offsetValidator(tableIdentifier, committedOffsets));
        if (branch != null) {
          appendOp.toBranch(branch);
        }
        appendOp.set(snapshotOffsetsProp, offsetsJson);
        appendOp.set(COMMIT_ID_SNAPSHOT_PROP, commitId.toString());
        appendOp.set(TASK_ID_SNAPSHOT_PROP, taskId);
        if (validThroughTs != null) {
          appendOp.set(VALID_THROUGH_TS_SNAPSHOT_PROP, validThroughTs.toString());
        }
        dataFiles.forEach(appendOp::appendFile);
        appendOp.commit();
      } else {
        RowDelta deltaOp =
            table.newRowDelta().validateWith(offsetValidator(tableIdentifier, committedOffsets));
        if (branch != null) {
          deltaOp.toBranch(branch);
        }
        deltaOp.set(snapshotOffsetsProp, offsetsJson);
        deltaOp.set(COMMIT_ID_SNAPSHOT_PROP, commitId.toString());
        deltaOp.set(TASK_ID_SNAPSHOT_PROP, taskId);
        if (validThroughTs != null) {
          deltaOp.set(VALID_THROUGH_TS_SNAPSHOT_PROP, validThroughTs.toString());
        }
        dataFiles.forEach(deltaOp::addRows);
        deleteFiles.forEach(deltaOp::addDeletes);
        deltaOp.commit();
      }

      Long snapshotId = latestSnapshot(table, branch).snapshotId();
      Event event =
          new Event(
              config.connectGroupId(),
              new CommitToTable(commitId, tableReference, snapshotId, validThroughTs));
      send(event);

      LOG.info(
          "Coordinator {} completed commit to table {}, snapshot {}, commit ID {}, valid-through {}",
          taskId,
          tableIdentifier,
          snapshotId,
          commitId,
          validThroughTs);
    }
  }

  private SnapshotAncestryValidator offsetValidator(
      TableIdentifier tableIdentifier, Map<Integer, Long> expectedOffsets) {

    return new SnapshotAncestryValidator() {
      private Map<Integer, Long> lastCommittedOffsets;

      @Override
      public boolean validate(Iterable<Snapshot> baseSnapshots) {
        lastCommittedOffsets = lastCommittedOffsets(baseSnapshots);

        return expectedOffsets.equals(lastCommittedOffsets);
      }

      @Override
      public String errorMessage() {
        return String.format(
            "Cannot commit to %s, stale offsets: Expected: %s Committed: %s",
            tableIdentifier, expectedOffsets, lastCommittedOffsets);
      }
    };
  }

  private <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
    Map<Object, Boolean> seen = Maps.newConcurrentMap();
    return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
  }

  private Snapshot latestSnapshot(Table table, String branch) {
    if (branch == null) {
      return table.currentSnapshot();
    }
    return table.snapshot(branch);
  }

  private Map<Integer, Long> lastCommittedOffsetsForTable(Table table, String branch) {
    Snapshot snapshot = latestSnapshot(table, branch);

    if (snapshot == null) {
      return Map.of();
    }

    Iterable<Snapshot> branchAncestry =
        SnapshotUtil.ancestorsOf(snapshot.snapshotId(), table::snapshot);
    return lastCommittedOffsets(branchAncestry);
  }

  private Map<Integer, Long> lastCommittedOffsets(Iterable<Snapshot> snapshots) {
    return Streams.stream(snapshots)
        .filter(Objects::nonNull)
        .filter(snapshot -> snapshot.summary().containsKey(snapshotOffsetsProp))
        .map(snapshot -> snapshot.summary().get(snapshotOffsetsProp))
        .map(this::parseOffsets)
        .findFirst()
        .orElseGet(Map::of);
  }

  private Map<Integer, Long> parseOffsets(String value) {
    if (value == null) {
      return Map.of();
    }

    TypeReference<Map<Integer, Long>> typeRef = new TypeReference<>() {};
    try {
      return MAPPER.readValue(value, typeRef);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  long partialCommitFailureCount() {
    return partialCommitFailures.get();
  }

  void terminate() {
    this.terminated = true;

    if (commitStateMBeanName != null) {
      try {
        ManagementFactory.getPlatformMBeanServer().unregisterMBean(commitStateMBeanName);
      } catch (Exception e) {
        LOG.warn("Failed to unregister CommitState MBean", e);
      }
    }

    exec.shutdownNow();

    // wait for coordinator termination, else cause the sink task to fail
    try {
      if (!exec.awaitTermination(1, TimeUnit.MINUTES)) {
        throw new ConnectException("Timed out waiting for coordinator shutdown");
      }
    } catch (InterruptedException e) {
      throw new ConnectException("Interrupted while waiting for coordinator shutdown", e);
    }
  }
}
