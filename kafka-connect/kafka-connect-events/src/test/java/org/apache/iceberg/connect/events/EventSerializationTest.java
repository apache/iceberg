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
import org.junit.jupiter.api.Test;

public class EventSerializationTest {

  @Test
  public void testStartCommitSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event = new Event("cg-connector", new StartCommit(commitId));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(result)
        .usingRecursiveComparison()
        .ignoringFieldsMatchingRegexes(".*avroSchema")
        .isEqualTo(event);
  }

  @Test
  public void testDataWrittenSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "cg-connector",
            new DataWritten(
                EventTestUtil.SPEC.partitionType(),
                commitId,
                new TableReference("catalog", Collections.singletonList("db"), "tbl"),
                Arrays.asList(EventTestUtil.createDataFile(), EventTestUtil.createDataFile()),
                Arrays.asList(EventTestUtil.createDeleteFile(), EventTestUtil.createDeleteFile())));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(result)
        .usingRecursiveComparison()
        .ignoringFieldsMatchingRegexes(
            "payload\\.partitionType",
            ".*avroSchema",
            ".*icebergSchema",
            ".*schema",
            ".*fromProjectionPos")
        .isEqualTo(event);
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
                    new TopicPartitionOffset("topic", 1, 1L, EventTestUtil.now()),
                    new TopicPartitionOffset("topic", 2, null, null))));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(result)
        .usingRecursiveComparison()
        .ignoringFieldsMatchingRegexes(".*avroSchema")
        .isEqualTo(event);
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
                EventTestUtil.now()));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(result)
        .usingRecursiveComparison()
        .ignoringFieldsMatchingRegexes(".*avroSchema")
        .isEqualTo(event);
  }

  @Test
  public void testCommitCompleteSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event = new Event("cg-connector", new CommitComplete(commitId, EventTestUtil.now()));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(result)
        .usingRecursiveComparison()
        .ignoringFieldsMatchingRegexes(".*avroSchema")
        .isEqualTo(event);
  }
}
