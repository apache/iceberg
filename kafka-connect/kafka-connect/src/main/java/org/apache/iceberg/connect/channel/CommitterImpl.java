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
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.CatalogUtils;
import org.apache.iceberg.connect.Committer;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
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

  @Override
  public void onTaskStarted(IcebergSinkConfig config, SinkTaskContext context) {
    this.config = config;
    this.catalog = CatalogUtils.loadCatalog(config);
    this.clientFactory = new KafkaClientFactory(config.kafkaProps());
    this.context = context;
  }

  @Override
  public void onPartitionsAdded(Collection<TopicPartition> addedPartitions) {
    if (hasLeaderPartition(addedPartitions)) {
      LOG.info("Committer received leader partition. Starting Coordinator.");
      startCoordinator();
    }
    startWorker();
  }

  private void startCoordinator() {
    if (null == this.coordinatorThread) {
      LOG.info(
          "Task {}-{} elected leader, starting commit coordinator",
          config.connectorName(),
          config.taskId());
      Coordinator coordinator =
          new Coordinator(catalog, config, membersWhenWorkerIsCoordinator, clientFactory, context);
      coordinatorThread = new CoordinatorThread(coordinator);
      coordinatorThread.start();
    }
  }

  private void startWorker() {
    if (null == this.worker) {
      LOG.info("Starting commit worker {}-{}", config.connectorName(), config.taskId());
      SinkWriter sinkWriter = new SinkWriter(catalog, config);
      worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();
    }
  }

  @Override
  public void onPartitionsRemoved(Collection<TopicPartition> closedPartitions) {
    // Stop current worker and start a new worker to avoid duplicates.
    stopWorker();
    startWorker();

    if (hasLeaderPartition(closedPartitions)) {
      LOG.info(
          "Committer {}-{} lost leader partition. Stopping coordinator.",
          config.connectorName(),
          config.taskId());
      stopCoordinator();
    }

    // Reset offsets to last committed to avoid data loss.
    LOG.info(
        "Seeking to last committed offsets for worker {}-{}.",
        config.connectorName(),
        config.taskId());
    KafkaUtils.seekToLastCommittedOffsets(context);
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

  @Override
  public void onTaskStopped() {
    LOG.info("Task stopped. Closing coordinator.");
    stopCoordinator();
    closeCatalog();
  }

  private void closeCatalog() {
    if (catalog != null) {
      if (catalog instanceof AutoCloseable) {
        try {
          ((AutoCloseable) catalog).close();
        } catch (Exception e) {
          LOG.warn("An error occurred closing catalog instance, ignoring...", e);
        }
      }
      catalog = null;
    }
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
      throw new NotRunningException(
          String.format(
              "Coordinator unexpectedly terminated on committer %s-%s",
              config.connectorName(), config.taskId()));
    }
    if (worker != null) {
      worker.process();
    }
  }
}
