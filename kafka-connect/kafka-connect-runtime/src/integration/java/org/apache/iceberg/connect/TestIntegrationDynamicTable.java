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
package org.apache.iceberg.connect;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestIntegrationDynamicTable extends IntegrationTestBase {

  private static final String TEST_TABLE1 = "tbl1";
  private static final String TEST_TABLE2 = "tbl2";
  private static final TableIdentifier TABLE_IDENTIFIER1 = TableIdentifier.of(TEST_DB, TEST_TABLE1);
  private static final TableIdentifier TABLE_IDENTIFIER2 = TableIdentifier.of(TEST_DB, TEST_TABLE2);

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testIcebergSink(String branch) {
    // partitioned table
    catalog().createTable(TABLE_IDENTIFIER1, TestEvent.TEST_SCHEMA, TestEvent.TEST_SPEC);
    // unpartitioned table
    catalog().createTable(TABLE_IDENTIFIER2, TestEvent.TEST_SCHEMA);

    boolean useSchema = branch == null; // use a schema for one of the tests
    runTest(branch, useSchema, ImmutableMap.of(), List.of(TABLE_IDENTIFIER1, TABLE_IDENTIFIER2));

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER1, branch);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).recordCount()).isEqualTo(1);
    assertSnapshotProps(TABLE_IDENTIFIER1, branch);

    files = dataFiles(TABLE_IDENTIFIER2, branch);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).recordCount()).isEqualTo(1);
    assertSnapshotProps(TABLE_IDENTIFIER2, branch);
  }

  /**
   * Verifies dynamic routing works when topic-rewriting SMTs (e.g. RegexRouter) change
   * record.topic(). Before the fix, SinkWriter tracked offsets under the rewritten topic, causing a
   * mismatch with context.assignment() and preventing proper offset commits.
   */
  @Test
  public void testDynamicRouteWithTopicRewritingSMT() {
    String smtTable = "smttbl";
    TableIdentifier smtTableId = TableIdentifier.of(TEST_DB, smtTable);
    catalog().createTable(smtTableId, TestEvent.TEST_SCHEMA);

    try {
      // RegexRouter rewrites topic to "test.smttbl", then InsertField copies
      // record.topic() (now "test.smttbl") into field "srcTopic", and dynamic
      // routing uses "srcTopic" to pick the destination table.
      KafkaConnectUtils.Config connectorConfig =
          createCommonConfig(false)
              .config("iceberg.tables.dynamic-enabled", true)
              .config("iceberg.tables.route-field", "srcTopic")
              .config("transforms", "rewriteTopic,insertTopic")
              .config(
                  "transforms.rewriteTopic.type", "org.apache.kafka.connect.transforms.RegexRouter")
              .config("transforms.rewriteTopic.regex", ".*")
              .config("transforms.rewriteTopic.replacement", TEST_DB + "." + smtTable)
              .config(
                  "transforms.insertTopic.type",
                  "org.apache.kafka.connect.transforms.InsertField$Value")
              .config("transforms.insertTopic.topic.field", "srcTopic");

      context().connectorCatalogProperties().forEach(connectorConfig::config);
      context().startConnector(connectorConfig);

      send(testTopic(), new TestEvent(1, "type1", Instant.now(), "hello"), false);
      send(testTopic(), new TestEvent(2, "type2", Instant.now(), "world"), false);
      flush();

      Awaitility.await()
          .atMost(Duration.ofSeconds(30))
          .pollInterval(Duration.ofSeconds(1))
          .untilAsserted(() -> assertSnapshotAdded(List.of(smtTableId)));

      List<DataFile> files = dataFiles(smtTableId, null);
      assertThat(files).hasSizeBetween(1, 2);
      assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(2);
      assertSnapshotProps(smtTableId, null);
    } finally {
      catalog().dropTable(smtTableId);
    }
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createCommonConfig(useSchema)
        .config("iceberg.tables.dynamic-enabled", true)
        .config("iceberg.tables.route-field", "payload");
  }

  @Override
  protected void sendEvents(boolean useSchema) {
    TestEvent event1 = new TestEvent(1, "type1", Instant.now(), TEST_DB + "." + TEST_TABLE1);
    TestEvent event2 = new TestEvent(2, "type2", Instant.now(), TEST_DB + "." + TEST_TABLE2);
    TestEvent event3 = new TestEvent(3, "type3", Instant.now(), TEST_DB + ".tbl3");

    send(testTopic(), event1, useSchema);
    send(testTopic(), event2, useSchema);
    send(testTopic(), event3, useSchema);
  }

  @Override
  void dropTables() {
    catalog().dropTable(TableIdentifier.of(TEST_DB, TEST_TABLE1));
    catalog().dropTable(TableIdentifier.of(TEST_DB, TEST_TABLE2));
  }
}
