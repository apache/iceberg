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
package org.apache.iceberg.connect.events;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;

public class EventSerializationTest {

  @Test
  public void testStartCommitSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event = new Event("cg-connector", new StartCommit(commitId));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(result.type()).isEqualTo(event.type());
    StartCommit payload = (StartCommit) result.payload();
    assertThat(payload.commitId()).isEqualTo(commitId);
  }

  @Test
  public void testDataWrittenSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "cg-connector",
            new DataWritten(
                StructType.of(),
                commitId,
                new TableReference("catalog", Collections.singletonList("db"), "tbl"),
                Arrays.asList(EventTestUtil.createDataFile(), EventTestUtil.createDataFile()),
                Arrays.asList(EventTestUtil.createDeleteFile(), EventTestUtil.createDeleteFile())));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(result.type()).isEqualTo(event.type());
    DataWritten payload = (DataWritten) result.payload();
    assertThat(payload.commitId()).isEqualTo(commitId);
    assertThat(payload.tableReference().catalog()).isEqualTo("catalog");
    assertThat(payload.tableReference().identifier()).isEqualTo(TableIdentifier.parse("db.tbl"));
    assertThat(payload.dataFiles()).hasSize(2);
    assertThat(payload.dataFiles()).allMatch(f -> f.specId() == 1);
    assertThat(payload.deleteFiles()).hasSize(2);
    assertThat(payload.deleteFiles()).allMatch(f -> f.specId() == 1);
  }

  @Test
  public void testDataCompleteSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "cg-connector",
            new DataComplete(
                commitId,
                Arrays.asList(
                    new TopicPartitionOffset("topic", 1, 1L, 1L),
                    new TopicPartitionOffset("topic", 2, null, null))));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(result.type()).isEqualTo(event.type());
    DataComplete payload = (DataComplete) result.payload();
    assertThat(payload.commitId()).isEqualTo(commitId);
    assertThat(payload.assignments()).hasSize(2);
    assertThat(payload.assignments()).allMatch(tp -> tp.topic().equals("topic"));
  }

  @Test
  public void testCommitToTableSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "cg-connector",
            new CommitToTable(
                commitId,
                new TableReference("catalog", Collections.singletonList("db"), "tbl"),
                1L,
                2L));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(result.type()).isEqualTo(event.type());
    CommitToTable payload = (CommitToTable) result.payload();
    assertThat(payload.commitId()).isEqualTo(commitId);
    assertThat(payload.tableReference().catalog()).isEqualTo("catalog");
    assertThat(payload.tableReference().identifier()).isEqualTo(TableIdentifier.parse("db.tbl"));
    assertThat(payload.snapshotId()).isEqualTo(1L);
    assertThat(payload.validThroughTs()).isEqualTo(2L);
  }

  @Test
  public void testCommitCompleteSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event = new Event("cg-connector", new CommitComplete(commitId, 2L));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(result.type()).isEqualTo(event.type());
    CommitComplete payload = (CommitComplete) result.payload();
    assertThat(payload.commitId()).isEqualTo(commitId);
    assertThat(payload.validThroughTs()).isEqualTo(2L);
  }
}
