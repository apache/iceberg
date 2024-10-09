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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.WriterFactory;
import org.mockito.Mockito;
import org.apache.iceberg.connect.data.IcebergWriterResult;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.data.SinkWriterResult;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class WorkerTest extends ChannelTestBase {

  @Test
  public void testSave() {
    when(config.catalogName()).thenReturn("catalog");

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
      mockKafkaUtils
          .when(() -> KafkaUtils.consumerGroupMetadata(any()))
          .thenReturn(consumerGroupMetadata);

      SinkTaskContext context = mock(SinkTaskContext.class);
      TopicPartition topicPartition = new TopicPartition(SRC_TOPIC_NAME, 0);
      when(context.assignment()).thenReturn(ImmutableSet.of(topicPartition));

      IcebergWriterResult writeResult =
          new IcebergWriterResult(
              TableIdentifier.parse(TABLE_NAME),
              ImmutableList.of(EventTestUtil.createDataFile()),
              ImmutableList.of(),
              StructType.of());

      Offset offsets =
              new Offset(1L, EventTestUtil.now());

      SinkWriterResult sinkWriterResult =
          new SinkWriterResult(ImmutableList.of(writeResult), offsets);
      SinkWriter sinkWriter = mock(SinkWriter.class);
      when(sinkWriter.completeWrite()).thenReturn(sinkWriterResult);

      Worker worker = new Worker(catalog, config, clientFactory, context);
      worker.start();
      worker.open(topicPartition);

      // init consumer after subscribe()
      initConsumer();

      // save a record
      Map<String, Object> value = ImmutableMap.of();
      SinkRecord rec = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, value, 0L);
      worker.save(ImmutableList.of(rec));

      UUID commitId = UUID.randomUUID();
      Event commitRequest = new Event(config.connectGroupId(), new StartCommit(commitId));
      byte[] bytes = AvroUtil.encode(commitRequest);
      consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", bytes));

      worker.process();

      assertThat(producer.history()).hasSize(2);

      Event event = AvroUtil.decode(producer.history().get(0).value());
      assertThat(event.payload().type()).isEqualTo(PayloadType.DATA_WRITTEN);
      DataWritten dataWritten = (DataWritten) event.payload();
      assertThat(dataWritten.commitId()).isEqualTo(commitId);

      event = AvroUtil.decode(producer.history().get(1).value());
      assertThat(event.type()).isEqualTo(PayloadType.DATA_COMPLETE);
      DataComplete dataComplete = (DataComplete) event.payload();
      assertThat(dataComplete.commitId()).isEqualTo(commitId);
      assertThat(dataComplete.assignments()).hasSize(1);
      assertThat(dataComplete.assignments().get(0).offset()).isEqualTo(1L);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
