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
package io.tabular.iceberg.connect.events;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;

public class EventSerializationTest {

  @Test
  public void testCommitRequestSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event("cg-connector", EventType.COMMIT_REQUEST, new CommitRequestPayload(commitId));

    byte[] data = Event.encode(event);
    Event result = Event.decode(data);

    assertThat(result.type()).isEqualTo(event.type());
    CommitRequestPayload payload = (CommitRequestPayload) result.payload();
    assertThat(payload.commitId()).isEqualTo(commitId);
  }

  @Test
  public void testCommitResponseSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "cg-connector",
            EventType.COMMIT_RESPONSE,
            new CommitResponsePayload(
                StructType.of(),
                commitId,
                new TableName(Collections.singletonList("db"), "tbl"),
                Arrays.asList(EventTestUtil.createDataFile(), EventTestUtil.createDataFile()),
                Arrays.asList(EventTestUtil.createDeleteFile(), EventTestUtil.createDeleteFile())));

    byte[] data = Event.encode(event);
    Event result = Event.decode(data);

    assertThat(result.type()).isEqualTo(event.type());
    CommitResponsePayload payload = (CommitResponsePayload) result.payload();
    assertThat(payload.commitId()).isEqualTo(commitId);
    assertThat(payload.tableName().toIdentifier()).isEqualTo(TableIdentifier.parse("db.tbl"));
    assertThat(payload.dataFiles()).hasSize(2);
    assertThat(payload.dataFiles()).allMatch(f -> f.specId() == 1);
    assertThat(payload.deleteFiles()).hasSize(2);
    assertThat(payload.deleteFiles()).allMatch(f -> f.specId() == 1);
  }

  @Test
  public void testCommitReadySerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "cg-connector",
            EventType.COMMIT_READY,
            new CommitReadyPayload(
                commitId,
                Arrays.asList(
                    new TopicPartitionOffset("topic", 1, 1L, 1L),
                    new TopicPartitionOffset("topic", 2, null, null))));

    byte[] data = Event.encode(event);
    Event result = Event.decode(data);

    assertThat(result.type()).isEqualTo(event.type());
    CommitReadyPayload payload = (CommitReadyPayload) result.payload();
    assertThat(payload.commitId()).isEqualTo(commitId);
    assertThat(payload.assignments()).hasSize(2);
    assertThat(payload.assignments()).allMatch(tp -> tp.topic().equals("topic"));
  }

  @Test
  public void testCommitTableSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "cg-connector",
            EventType.COMMIT_TABLE,
            new CommitTablePayload(
                commitId, new TableName(Collections.singletonList("db"), "tbl"), 1L, 2L));

    byte[] data = Event.encode(event);
    Event result = Event.decode(data);

    assertThat(result.type()).isEqualTo(event.type());
    CommitTablePayload payload = (CommitTablePayload) result.payload();
    assertThat(payload.commitId()).isEqualTo(commitId);
    assertThat(payload.tableName().toIdentifier()).isEqualTo(TableIdentifier.parse("db.tbl"));
    assertThat(payload.snapshotId()).isEqualTo(1L);
    assertThat(payload.vtts()).isEqualTo(2L);
  }

  @Test
  public void testCommitCompleteSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "cg-connector", EventType.COMMIT_COMPLETE, new CommitCompletePayload(commitId, 2L));

    byte[] data = Event.encode(event);
    Event result = Event.decode(data);

    assertThat(result.type()).isEqualTo(event.type());
    CommitCompletePayload payload = (CommitCompletePayload) result.payload();
    assertThat(payload.commitId()).isEqualTo(commitId);
    assertThat(payload.vtts()).isEqualTo(2L);
  }
}
