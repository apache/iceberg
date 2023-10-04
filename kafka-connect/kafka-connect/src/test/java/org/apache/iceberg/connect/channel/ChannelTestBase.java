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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;

public class ChannelTestBase {
  protected static final String SRC_TOPIC_NAME = "src-topic";
  protected static final String CTL_TOPIC_NAME = "ctl-topic";

  protected Catalog catalog;
  protected Table table;
  protected AppendFiles appendOp;
  protected RowDelta deltaOp;
  protected IcebergSinkConfig config;
  protected KafkaClientFactory clientFactory;
  protected MockProducer<String, byte[]> producer;
  protected MockConsumer<String, byte[]> consumer;
  protected Admin admin;

  @BeforeEach
  @SuppressWarnings("deprecation")
  public void before() {
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.snapshotId()).thenReturn(1L);

    appendOp = mock(AppendFiles.class);
    deltaOp = mock(RowDelta.class);

    table = mock(Table.class);
    when(table.currentSnapshot()).thenReturn(snapshot);
    when(table.newAppend()).thenReturn(appendOp);
    when(table.newRowDelta()).thenReturn(deltaOp);

    catalog = mock(Catalog.class);
    when(catalog.loadTable(any())).thenReturn(table);

    config = mock(IcebergSinkConfig.class);
    when(config.controlTopic()).thenReturn(CTL_TOPIC_NAME);
    when(config.controlGroupId()).thenReturn("group");
    when(config.commitThreads()).thenReturn(1);
    when(config.controlGroupId()).thenReturn("cg-connector");
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));

    TopicPartitionInfo partitionInfo = mock(TopicPartitionInfo.class);
    when(partitionInfo.partition()).thenReturn(0);
    TopicDescription topicDesc =
        new TopicDescription(SRC_TOPIC_NAME, false, ImmutableList.of(partitionInfo));
    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    when(describeResult.values())
        .thenReturn(ImmutableMap.of(SRC_TOPIC_NAME, KafkaFuture.completedFuture(topicDesc)));

    admin = mock(Admin.class);
    when(admin.describeTopics(anyCollection())).thenReturn(describeResult);

    producer = new MockProducer<>(false, new StringSerializer(), new ByteArraySerializer());
    producer.initTransactions();

    consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

    clientFactory = mock(KafkaClientFactory.class);
    when(clientFactory.createProducer(any())).thenReturn(producer);
    when(clientFactory.createConsumer(any())).thenReturn(consumer);
    when(clientFactory.createAdmin()).thenReturn(admin);
  }

  protected void initConsumer() {
    TopicPartition tp = new TopicPartition(CTL_TOPIC_NAME, 0);
    consumer.rebalance(ImmutableList.of(tp));
    consumer.updateBeginningOffsets(ImmutableMap.of(tp, 0L));
  }
}
