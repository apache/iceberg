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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.Committer;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.ResourceType;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toMap;

public class CommitterImpl implements Committer {

  private static final Logger LOG = LoggerFactory.getLogger(CommitterImpl.class);

  private CoordinatorThread coordinatorThread;
  private Worker worker;
  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final SinkTaskContext context;
  private final KafkaClientFactory clientFactory;
  private Collection<MemberDescription> membersWhenWorkerIsCoordinator;
  private final Admin admin;

  public CommitterImpl(Catalog catalog, IcebergSinkConfig config, SinkTaskContext context) {
    this.catalog = catalog;
    this.config = config;
    this.context = context;
    this.clientFactory = new KafkaClientFactory(config.kafkaProps());
    this.admin = clientFactory.createAdmin();
  }

  public CommitterImpl() {
    this.catalog = null;
    this.config = null;
    this.context = null;
    this.clientFactory = null;
    this.admin = null;
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

  @Override
  public boolean isLeader(Collection<TopicPartition> currentAssignedPartitions) {
    ConsumerGroupDescription groupDesc;
    try (Admin admin = clientFactory.createAdmin()) {
      groupDesc = KafkaUtils.consumerGroupDescription(config.connectGroupId(), admin);
    }
    if (groupDesc.state() == ConsumerGroupState.STABLE) {
      Collection<MemberDescription> members = groupDesc.members();
      Set<TopicPartition> partitions = context.assignment();
      if (isLeader(members, partitions)) {
        membersWhenWorkerIsCoordinator = members;
        return true;
      }
    }
    return false;
  }

  @Override
  public void start(ResourceType resourceType) {
    switch (resourceType) {
      case WORKER:
        startWorker();
        break;
      case COORDINATOR:
        startCoordinator();
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

  @Override
  public void syncLastCommittedOffsets() {
    Map<TopicPartition, Long> stableConsumerOffsets;
    try {
      ListConsumerGroupOffsetsResult response =
              admin.listConsumerGroupOffsets(
                      config.connectGroupId(), new ListConsumerGroupOffsetsOptions().requireStable(true));
      stableConsumerOffsets =
              response.partitionsToOffsetAndMetadata().get().entrySet().stream()
                      .filter(entry -> context.assignment().contains(entry.getKey()))
                      .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
    } catch (InterruptedException | ExecutionException e) {
      throw new ConnectException(e);
    }
    context.offset(stableConsumerOffsets);
  }

  @Override
  public void stop(ResourceType resourceType) {
    switch (resourceType) {
      case WORKER:
        stopWorker();
        break;
      case COORDINATOR:
        stopCoordinator();
    }
  }

  @VisibleForTesting
  boolean isLeader(Collection<MemberDescription> members, Collection<TopicPartition> partitions) {
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

  private void processControlEvents() {
    if (coordinatorThread != null && coordinatorThread.isTerminated()) {
      throw new NotRunningException("Coordinator unexpectedly terminated");
    }
    if (worker != null) {
      worker.process();
    }
  }

  private void startWorker() {
    if(null == this.worker) {
      LOG.info("Starting commit worker");
      SinkWriter sinkWriter = new SinkWriter(catalog, config);
      worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();
    }
  }

  private void startCoordinator() {
    LOG.info("Task elected leader, starting commit coordinator");
    Coordinator coordinator = new Coordinator(catalog, config, membersWhenWorkerIsCoordinator, clientFactory, context);
    coordinatorThread = new CoordinatorThread(coordinator);
    coordinatorThread.start();
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