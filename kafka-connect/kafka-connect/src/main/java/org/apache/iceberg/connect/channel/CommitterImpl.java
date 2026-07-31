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
import java.util.List;
import java.util.Set;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.Committer;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
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
  private String taskId;
  private Consumer<byte[], byte[]> sourceConsumer;

  // Set by a rebalance (open/close) and cleared once save() has reconciled leadership. The
  // coordinator is started/stopped on the task thread in save() rather than in the rebalance
  // callback: this keeps blocking work (initTransactions, partitionsFor) off the callback and
  // avoids an eager rebalance (which revokes then re-adds all partitions) needlessly restarting a
  // still-leading coordinator.
  private boolean reconcileNeeded = false;

  /**
   * The leader partition is partition 0 of the lexicographically-smallest subscribed topic. The
   * subscription is connector-wide (identical across tasks, for both a {@code topics} list and a
   * {@code topics.regex}) and partition 0 of any topic is always owned by exactly one task, so the
   * task that owns this partition is the single, stable coordinator leader. Returns {@code null}
   * when the subscription is empty (not yet known), in which case no task leads.
   */
  @VisibleForTesting
  static TopicPartition leaderPartition(Collection<String> subscribedTopics) {
    return subscribedTopics.stream()
        .min(Comparator.naturalOrder())
        .map(topic -> new TopicPartition(topic, 0))
        .orElse(null);
  }

  @Override
  public void configure(
      Catalog icebergCatalog,
      IcebergSinkConfig icebergSinkConfig,
      SinkTaskContext sinkTaskContext) {
    this.catalog = icebergCatalog;
    this.config = icebergSinkConfig;
    this.context = sinkTaskContext;
    this.clientFactory = new KafkaClientFactory(config.kafkaProps());
    this.taskId = config.connectorName() + "-" + config.taskId();
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
    // Leadership is reconciled on the task thread in save(); flag it and keep the callback fast.
    reconcileNeeded = true;
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

    // Empty partitions → the task is being stopped. Tear the coordinator down here since save()
    // will not be called again to reconcile it away.
    if (closedPartitions.isEmpty()) {
      LOG.info("Committer {} stopped. Closing coordinator.", taskId);
      stopCoordinator();
      return;
    }

    // Partition revocation: leadership may have moved, so reconcile on the next save(). We
    // intentionally do NOT stop the coordinator here — an eager rebalance revokes and re-adds all
    // partitions, and stopping here would churn a coordinator that this task still leads.
    reconcileNeeded = true;

    // Reset offsets to last committed to avoid data loss.
    LOG.info("Seeking to last committed offsets for worker {}.", taskId);
    KafkaUtils.seekToLastCommittedOffsets(sourceConsumer());
  }

  @Override
  public void save(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      startWorker();
      worker.save(sinkRecords);
    }
    if (reconcileNeeded) {
      reconcileLeadership();
      reconcileNeeded = false;
    }
    processControlEvents();
  }

  /**
   * Reconciles the coordinator lifecycle against current leadership. Runs on the single task thread
   * (via save()) so it reads a fully-settled {@link SinkTaskContext#assignment()} with no locks or
   * extra threads: starts the coordinator when this task owns the leader partition, stops it when
   * it does not. Both start and stop are idempotent.
   */
  private void reconcileLeadership() {
    Set<String> subscribedTopics = Sets.newTreeSet(sourceConsumer().subscription());
    TopicPartition leader = leaderPartition(subscribedTopics);
    if (leader != null && context.assignment().contains(leader)) {
      startCoordinator(subscribedTopics);
    } else {
      stopCoordinator();
    }
  }

  private void processControlEvents() {
    if (coordinatorThread != null && coordinatorThread.isTerminated()) {
      if (isProducerFenced(coordinatorThread.exception())) {
        // Lost the coordinator race (fenced by a newer coordinator). Clear it so the commit thread
        // pool is released; a surviving coordinator on another task keeps committing.
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

  private void startCoordinator(Set<String> subscribedTopics) {
    if (null == this.coordinatorThread) {
      int topicPartitionCount = 0;
      for (String topic : subscribedTopics) {
        List<PartitionInfo> partitions = sourceConsumer().partitionsFor(topic);
        if (partitions != null) {
          topicPartitionCount += partitions.size();
        }
      }
      LOG.info(
          "Task {} elected leader, starting commit coordinator (expecting {} partitions)",
          taskId,
          topicPartitionCount);
      Coordinator coordinator =
          new Coordinator(catalog, config, topicPartitionCount, clientFactory, context);
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

  Consumer<byte[], byte[]> sourceConsumer() {
    if (null == sourceConsumer) {
      sourceConsumer = KafkaUtils.kafkaConsumer(this.context);
    }
    return sourceConsumer;
  }
}
