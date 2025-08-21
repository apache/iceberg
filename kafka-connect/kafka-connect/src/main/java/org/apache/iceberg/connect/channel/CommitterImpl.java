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
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.Committer;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitterImpl implements Committer {

  private static final Logger LOG = LoggerFactory.getLogger(CommitterImpl.class);

  private CoordinatorThread coordinatorThread;
  private Worker worker;
  private Catalog catalog;
  private IcebergSinkConfig config;
  private SinkTaskContext context;
  private KafkaClientFactory clientFactory;
  private Collection<MemberDescription> membersWhenWorkerIsCoordinator;
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);

  private void initialize(
      Catalog icebergCatalog,
      IcebergSinkConfig icebergSinkConfig,
      SinkTaskContext sinkTaskContext) {
    if (isInitialized.compareAndSet(false, true)) {
      this.catalog = icebergCatalog;
      this.config = icebergSinkConfig;
      this.context = sinkTaskContext;
      this.clientFactory = new KafkaClientFactory(config.kafkaProps());
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

  private boolean hasLeaderPartition(Collection<TopicPartition> currentAssignedPartitions) {
    ConsumerGroupDescription groupDesc;
    try (Admin admin = clientFactory.createAdmin()) {
      groupDesc = KafkaUtils.consumerGroupDescription(config.connectGroupId(), admin);
    }
    if (groupDesc.state() == ConsumerGroupState.STABLE) {
      Collection<MemberDescription> members = groupDesc.members();
      if (containsFirstPartition(members, currentAssignedPartitions)) {
        membersWhenWorkerIsCoordinator = members;
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  boolean containsFirstPartition(
      Collection<MemberDescription> members, Collection<TopicPartition> partitions) {
    // there should only be one task assigned partition 0 of the first topic,
    // so elect that one the leader
    TopicPartition firstTopicPartition =
        members.stream()
            .flatMap(member -> member.assignment().topicPartitions().stream())
            .min(new TopicPartitionComparator())
            .orElseThrow(
                () -> new ConnectException("No partitions assigned, cannot determine leader"));

    return partitions.contains(firstTopicPartition);
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
      LOG.info("Committer received leader partition. Starting Coordinator.");
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
    if (!isInitialized.get()) {
      LOG.warn("Unexpected close() call without resource initialization");
      return;
    }
    if (hasLeaderPartition(closedPartitions)) {
      LOG.info(
          "Committer {}-{} lost leader partition. Stopping Coordinator.",
          config.connectorName(),
          config.taskId());
      stopCoordinator();
    }
    LOG.info("Stopping worker {}-{}.", config.connectorName(), config.taskId());
    stopWorker();
    LOG.info(
        "Seeking to last committed offsets for worker {}-{}.",
        config.connectorName(),
        config.taskId());
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
      throw new NotRunningException("Coordinator unexpectedly terminated");
    }
    if (worker != null) {
      worker.process();
    }
  }

  private void startWorker() {
    if (null == this.worker) {
      LOG.info("Starting commit worker");
      SinkWriter sinkWriter = new SinkWriter(catalog, config);
      worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();
    }
  }

  private void startCoordinator() {
    if (null == this.coordinatorThread) {
      LOG.info("Task elected leader, starting commit coordinator");
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
    if (coordinatorThread != null) {
      coordinatorThread.terminate();
      coordinatorThread = null;
    }
  }
}
