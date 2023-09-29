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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.data.IcebergWriter;
import io.tabular.iceberg.connect.data.IcebergWriterFactory;
import io.tabular.iceberg.connect.data.WriterResult;
import io.tabular.iceberg.connect.events.CommitReadyPayload;
import io.tabular.iceberg.connect.events.CommitRequestPayload;
import io.tabular.iceberg.connect.events.CommitResponsePayload;
import io.tabular.iceberg.connect.events.Event;
import io.tabular.iceberg.connect.events.EventTestUtil;
import io.tabular.iceberg.connect.events.EventType;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

public class WorkerTest extends ChannelTestBase {

  private static final String TABLE_NAME = "db.tbl";
  private static final String FIELD_NAME = "fld";

  @Test
  public void testStaticRoute() {
    when(config.getTables()).thenReturn(ImmutableList.of(TABLE_NAME));
    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, "val");
    workerTest(value);
  }

  @Test
  public void testDynamicRoute() {
    when(config.getDynamicTablesEnabled()).thenReturn(true);
    when(config.getTablesRouteField()).thenReturn(FIELD_NAME);
    when(catalog.tableExists(any())).thenReturn(true);
    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    workerTest(value);
  }

  private void workerTest(Map<String, Object> value) {
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(SRC_TOPIC_NAME, 0)));

    WriterResult writeResult =
        new WriterResult(
            TableIdentifier.parse(TABLE_NAME),
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(),
            StructType.of());
    IcebergWriter writer = mock(IcebergWriter.class);
    when(writer.complete()).thenReturn(ImmutableList.of(writeResult));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);

    Worker worker = new Worker(catalog, config, clientFactory, writerFactory, context);
    worker.start();

    // init consumer after subscribe()
    initConsumer();

    // save a record
    SinkRecord rec = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, value, 0L);
    worker.save(ImmutableList.of(rec));

    UUID commitId = UUID.randomUUID();
    Event commitRequest =
        new Event(
            config.getControlGroupId(),
            EventType.COMMIT_REQUEST,
            new CommitRequestPayload(commitId));
    byte[] bytes = Event.encode(commitRequest);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", bytes));

    worker.process();

    assertEquals(2, producer.history().size());

    Event event = Event.decode(producer.history().get(0).value());
    assertEquals(EventType.COMMIT_RESPONSE, event.getType());
    CommitResponsePayload responsePayload = (CommitResponsePayload) event.getPayload();
    assertEquals(commitId, responsePayload.getCommitId());

    event = Event.decode(producer.history().get(1).value());
    assertEquals(EventType.COMMIT_READY, event.getType());
    CommitReadyPayload readyPayload = (CommitReadyPayload) event.getPayload();
    assertEquals(commitId, readyPayload.getCommitId());
    assertEquals(1, readyPayload.getAssignments().size());
    // offset should be one more than the record offset
    assertEquals(1L, readyPayload.getAssignments().get(0).getOffset());
  }
}
