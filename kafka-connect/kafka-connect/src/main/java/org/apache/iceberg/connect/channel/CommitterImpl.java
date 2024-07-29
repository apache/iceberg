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
import java.util.Set;
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
  public void start(Catalog catalog, IcebergSinkConfig config, SinkTaskContext context) {
    KafkaClientFactory clientFactory = new KafkaClientFactory(config.kafkaProps());

    ConsumerGroupDescription groupDesc;
    try (Admin admin = clientFactory.createAdmin()) {
      groupDesc = KafkaUtils.consumerGroupDescription(config.connectGroupId(), admin);
    }

    if (groupDesc.state() == ConsumerGroupState.STABLE) {
      Collection<MemberDescription> members = groupDesc.members();
      Set<TopicPartition> partitions = context.assignment();
      if (isLeader(members, partitions)) {
        LOG.info("Task elected leader, starting commit coordinator");
        Coordinator coordinator = new Coordinator(catalog, config, members, clientFactory, context);
        coordinatorThread = new CoordinatorThread(coordinator);
        coordinatorThread.start();
      }
    }

    LOG.info("Starting commit worker");
    SinkWriter sinkWriter = new SinkWriter(catalog, config);
    worker = new Worker(config, clientFactory, sinkWriter, context);
    worker.start();
  }

  @Override
  public void save(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      worker.save(sinkRecords);
    }
    processControlEvents();
  }

  @Override
  public void stop() {
    if (worker != null) {
      worker.stop();
      worker = null;
    }

    if (coordinatorThread != null) {
      coordinatorThread.terminate();
      coordinatorThread = null;
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
}
