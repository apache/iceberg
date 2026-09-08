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

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.Committer;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitterImpl implements Committer {

  private static final Logger LOG = LoggerFactory.getLogger(CommitterImpl.class);

  // Bounded wait for a coordinator thread to fully exit (release its producer/consumer/admin) when
  // stopping it, so a newly elected coordinator does not overlap the old one's clients.
  private static final long COORDINATOR_STOP_TIMEOUT_MS = 60_000L;

  private CoordinatorThread coordinatorThread;
  private Worker worker;
  private Catalog catalog;
  private IcebergSinkConfig config;
  private SinkTaskContext context;
  private KafkaClientFactory clientFactory;
  private Collection<MemberDescription> membersWhenWorkerIsCoordinator;
  private final AtomicReference<TopicPartition> leaderTopicPartition = new AtomicReference<>(null);
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);
  private String taskId;

  private void initialize(
      Catalog icebergCatalog,
      IcebergSinkConfig icebergSinkConfig,
      SinkTaskContext sinkTaskContext) {
    if (isInitialized.compareAndSet(false, true)) {
      this.catalog = icebergCatalog;
      this.config = icebergSinkConfig;
      this.context = sinkTaskContext;
      this.clientFactory = new KafkaClientFactory(config.kafkaProps());
      this.taskId = config.connectorName() + "-" + config.taskId();
    }
  }

  static class TopicPartitionComparator implements Comparator<TopicPartition> {

    @Override
    public int compare(TopicPartition o1, TopicPartition o2) {
      int result = o1.topic().compareTo(o2.topic());
      if (result == 0) {
        result = Integer.compare(o1.partition(), o2.partition());
      }
      return result;
    }
  }

  @VisibleForTesting
  boolean hasLeaderPartition(Collection<TopicPartition> currentAssignedPartitions) {
    ConsumerGroupDescription groupDesc;
    try (Admin admin = clientFactory.createAdmin()) {
      groupDesc = KafkaUtils.consumerGroupDescription(config.connectGroupId(), admin);
    }

    Collection<MemberDescription> members = groupDesc.members();
    if (containsFirstPartition(members, currentAssignedPartitions)) {
      membersWhenWorkerIsCoordinator = members;
      return true;
    }

    return false;
  }

  @VisibleForTesting
  boolean containsFirstPartition(
      Collection<MemberDescription> members, Collection<TopicPartition> partitions) {
    // Determine the first partition across all members to elect the leader
    TopicPartition firstTopicPartition = findFirstTopicPartition(members);

    if (firstTopicPartition == null) {
      LOG.warn(
          "Committer {} found no partitions assigned across all members, cannot determine leader",
          taskId);
      return false;
    }

    boolean containsFirst = partitions.contains(firstTopicPartition);
    if (containsFirst) {
      LOG.info(
          "Committer {} contains the first partition {}, this task is the leader",
          taskId,
          firstTopicPartition);
      leaderTopicPartition.set(firstTopicPartition);
    } else {
      LOG.debug(
          "Committer {} does not contain the first partition {}, not the leader",
          taskId,
          firstTopicPartition);
    }

    return containsFirst;
  }

  @VisibleForTesting
  TopicPartition findFirstTopicPartition(Collection<MemberDescription> members) {
    return members.stream()
        .flatMap(member -> member.assignment().topicPartitions().stream())
        .min(new TopicPartitionComparator())
        .orElse(null);
  }

  @Override
  public void start(
      Catalog icebergCatalog,
      IcebergSinkConfig icebergSinkConfig,
      SinkTaskContext sinkTaskContext) {
    throw new UnsupportedOperationException(
        "The method start(Catalog, IcebergSinkConfig, SinkTaskContext) is deprecated and will be removed in 2.0.0. "
            + "Use start(Catalog, IcebergSinkConfig, SinkTaskContext, Collection<TopicPartition>) instead.");
  }

  @Override
  public void open(
      Catalog icebergCatalog,
      IcebergSinkConfig icebergSinkConfig,
      SinkTaskContext sinkTaskContext,
      Collection<TopicPartition> addedPartitions) {
    initialize(icebergCatalog, icebergSinkConfig, sinkTaskContext);
    if (hasLeaderPartition(addedPartitions)) {
      LOG.info("Committer {} received leader partition. Starting Coordinator.", taskId);
      startCoordinator();
    }
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException(
        "The method stop() is deprecated and will be removed in 2.0.0. "
            + "Use stop(Collection<TopicPartition>) instead.");
  }

  @Override
  public void close(Collection<TopicPartition> closedPartitions) {
    // Always try to stop the worker to avoid duplicates.
    stopWorker();

    // Defensive: close called without prior initialization (should not happen).
    if (!isInitialized.get()) {
      LOG.warn("Close unexpectedly called on committer {} without partition assignment", taskId);
      return;
    }

    // Empty partitions → task was stopped explicitly. Stop coordinator if running.
    if (closedPartitions.isEmpty()) {
      LOG.info("Committer {} stopped. Closing coordinator.", taskId);
      stopCoordinator();
      return;
    }

    // Normal close: if leader partition is lost, stop coordinator.
    if (closedPartitions.contains(leaderTopicPartition.get())) {
      LOG.info(
          "Committer {} lost leader partition {}. Stopping coordinator.",
          taskId,
          leaderTopicPartition.get());
      stopCoordinator();
    }

    // Reset offsets to last committed to avoid data loss.
    LOG.info("Seeking to last committed offsets for worker {}.", taskId);
    KafkaUtils.seekToLastCommittedOffsets(context);
  }

  @Override
  public void save(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      startWorker();
      worker.save(sinkRecords);
    }
    processControlEvents();
  }

  private void processControlEvents() {
    if (coordinatorThread != null && coordinatorThread.isTerminated()) {
      if (isProducerFenced(coordinatorThread.exception())) {
        // Lost the coordinator race (fenced by a newer coordinator). Clear it so
        // commit thread pool are released.
        LOG.warn("Committer {} coordinator was fenced by a newer coordinator; clearing it", taskId);
        stopCoordinator();
      } else {
        throw new NotRunningException(
            String.format("Coordinator unexpectedly terminated on committer %s", taskId));
      }
    }
    if (worker != null) {
      worker.process();
    }
  }

  private void startWorker() {
    if (null == this.worker) {
      LOG.info("Starting commit worker {}", taskId);
      SinkWriter sinkWriter = new SinkWriter(catalog, config);
      worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();
    }
  }

  private void startCoordinator() {
    if (null == this.coordinatorThread) {
      LOG.info(
          "Task {} elected leader (owns {}), starting commit coordinator.",
          taskId,
          leaderTopicPartition);
      Coordinator coordinator =
          new Coordinator(catalog, config, membersWhenWorkerIsCoordinator, clientFactory, context);
      coordinatorThread = new CoordinatorThread(coordinator);
      coordinatorThread.start();
    }
  }

  private void stopWorker() {
    if (worker != null) {
      worker.stop();
      worker = null;
    }
  }

  private void stopCoordinator() {
    CoordinatorThread thread = coordinatorThread;
    if (thread == null) {
      return;
    }
    coordinatorThread = null;

    try {
      if (!thread.isTerminated()) {
        thread.terminate();
      }
    } catch (RuntimeException e) {
      LOG.warn(
          "Committer {}: error signalling coordinator termination, continuing shutdown", taskId, e);
    }

    try {
      thread.join(COORDINATOR_STOP_TIMEOUT_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn(
          "Committer {}: interrupted while waiting for the coordinator thread to stop", taskId, e);
      return;
    }

    if (thread.isAlive()) {
      LOG.warn(
          "Committer {}: coordinator thread did not stop within {} ms; it will keep shutting down "
              + "in the background (a newer coordinator fences its producer)",
          taskId,
          COORDINATOR_STOP_TIMEOUT_MS);
    }
  }

  private static boolean isProducerFenced(Throwable cause) {
    Throwable current = cause;
    for (int depth = 0; current != null && depth < 20; depth++, current = current.getCause()) {
      if (current instanceof ProducerFencedException
          || current instanceof InvalidProducerEpochException
          || current instanceof UnknownProducerIdException) {
        return true;
      }
    }
    return false;
  }
}
