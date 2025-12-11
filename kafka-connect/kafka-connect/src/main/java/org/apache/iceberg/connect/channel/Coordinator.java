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
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  private volatile boolean terminated;

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
  }

  void process() {
    if (commitState.isCommitIntervalReached()) {
      // send out begin commit
      commitState.startNewCommit();
      Event event =
          new Event(config.connectGroupId(), new StartCommit(commitState.currentCommitId()));
      send(event);
      LOG.info("Commit {} initiated", commitState.currentCommitId());
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
    } catch (Exception e) {
      LOG.warn("Commit failed, will try again next cycle", e);
    } finally {
      commitState.endCurrentCommit();
    }
  }

  private void doCommit(boolean partialCommit) {
    Map<TableReference, List<Envelope>> commitMap = commitState.tableCommitMap();
    OffsetDateTime validThroughTs = commitState.validThroughTs(partialCommit);

    Tasks.foreach(commitMap.entrySet())
        .executeWith(exec)
        .stopOnFailure()
        .run(
            entry -> {
              commitToTable(
                  entry.getKey(), entry.getValue(), controlTopicOffsets(), validThroughTs);
            });

    // we should only get here if all tables committed successfully...
    commitConsumerOffsets();
    commitState.clearResponses();

    Event event =
        new Event(
            config.connectGroupId(),
            new CommitComplete(commitState.currentCommitId(), validThroughTs));
    send(event);

    LOG.info(
        "Commit {} complete, committed to {} table(s), valid-through {}",
        commitState.currentCommitId(),
        commitMap.size(),
        validThroughTs);
  }

  private String offsetsToJson(Map<Integer, Long> offsets) {
    try {
      return MAPPER.writeValueAsString(offsets);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void commitToTable(
      TableReference tableReference,
      List<Envelope> envelopeList,
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
      throw new ConnectException("Coordinator is terminated, commit aborted");
    }

    if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
      LOG.info("Nothing to commit to table {}, skipping", tableIdentifier);
    } else {
      String taskId = String.format("%s-%s", config.connectorName(), config.taskId());
      if (deleteFiles.isEmpty()) {
        AppendFiles appendOp =
            table.newAppend().validateWith(offsetValidator(tableIdentifier, committedOffsets));
        if (branch != null) {
          appendOp.toBranch(branch);
        }
        appendOp.set(snapshotOffsetsProp, offsetsJson);
        appendOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
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
        deltaOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
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
              new CommitToTable(
                  commitState.currentCommitId(), tableReference, snapshotId, validThroughTs));
      send(event);

      LOG.info(
          "Commit complete to table {}, snapshot {}, commit ID {}, valid-through {}",
          tableIdentifier,
          snapshotId,
          commitState.currentCommitId(),
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

    TypeReference<Map<Integer, Long>> typeRef = new TypeReference<Map<Integer, Long>>() {};
    try {
      return MAPPER.readValue(value, typeRef);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  void terminate() {
    this.terminated = true;

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
