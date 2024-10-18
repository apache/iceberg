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
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

public class IntegrationDynamicTableTest extends IntegrationTestBase {

  private static final String TEST_DB = "test";
  private static final String TEST_TABLE1 = "tbl1";
  private static final String TEST_TABLE2 = "tbl2";
  private static final TableIdentifier TABLE_IDENTIFIER1 = TableIdentifier.of(TEST_DB, TEST_TABLE1);
  private static final TableIdentifier TABLE_IDENTIFIER2 = TableIdentifier.of(TEST_DB, TEST_TABLE2);

  @BeforeEach
  public void before() {
    createTopic(testTopic(), TEST_TOPIC_PARTITIONS);
    ((SupportsNamespaces) catalog()).createNamespace(Namespace.of(TEST_DB));
  }

  @AfterEach
  public void after() {
    context().stopConnector(connectorName());
    deleteTopic(testTopic());
    catalog().dropTable(TableIdentifier.of(TEST_DB, TEST_TABLE1));
    catalog().dropTable(TableIdentifier.of(TEST_DB, TEST_TABLE2));
    ((SupportsNamespaces) catalog()).dropNamespace(Namespace.of(TEST_DB));
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testIcebergSink(String branch) {
    // partitioned table
    catalog().createTable(TABLE_IDENTIFIER1, TestEvent.TEST_SCHEMA, TestEvent.TEST_SPEC);
    // unpartitioned table
    catalog().createTable(TABLE_IDENTIFIER2, TestEvent.TEST_SCHEMA);

    boolean useSchema = branch == null; // use a schema for one of the tests
    runTest(branch, useSchema);

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER1, branch);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).recordCount()).isEqualTo(1);
    assertSnapshotProps(TABLE_IDENTIFIER1, branch);

    files = dataFiles(TABLE_IDENTIFIER2, branch);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).recordCount()).isEqualTo(1);
    assertSnapshotProps(TABLE_IDENTIFIER2, branch);
  }

  private void runTest(String branch, boolean useSchema) {
    // set offset reset to earliest so we don't miss any test messages
    KafkaConnectUtils.Config connectorConfig =
        new KafkaConnectUtils.Config(connectorName())
            .config("topics", testTopic())
            .config("connector.class", IcebergSinkConnector.class.getName())
            .config("tasks.max", 2)
            .config("consumer.override.auto.offset.reset", "earliest")
            .config("key.converter", "org.apache.kafka.connect.json.JsonConverter")
            .config("key.converter.schemas.enable", false)
            .config("value.converter", "org.apache.kafka.connect.json.JsonConverter")
            .config("value.converter.schemas.enable", useSchema)
            .config("iceberg.tables.dynamic-enabled", true)
            .config("iceberg.tables.route-field", "payload")
            .config("iceberg.control.commit.interval-ms", 1000)
            .config("iceberg.control.commit.timeout-ms", Integer.MAX_VALUE)
            .config("iceberg.kafka.auto.offset.reset", "earliest");

    context().connectorCatalogProperties().forEach(connectorConfig::config);

    if (branch != null) {
      connectorConfig.config("iceberg.tables.default-commit-branch", branch);
    }

    if (!useSchema) {
      connectorConfig.config("value.converter.schemas.enable", false);
    }

    context().startConnector(connectorConfig);

    TestEvent event1 = new TestEvent(1, "type1", Instant.now(), TEST_DB + "." + TEST_TABLE1);
    TestEvent event2 = new TestEvent(2, "type2", Instant.now(), TEST_DB + "." + TEST_TABLE2);
    TestEvent event3 = new TestEvent(3, "type3", Instant.now(), TEST_DB + ".tbl3");

    send(testTopic(), event1, useSchema);
    send(testTopic(), event2, useSchema);
    send(testTopic(), event3, useSchema);
    flush();

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(this::assertSnapshotAdded);
  }

  private void assertSnapshotAdded() {
    Table table = catalog().loadTable(TABLE_IDENTIFIER1);
    assertThat(table.snapshots()).hasSize(1);
    table = catalog().loadTable(TABLE_IDENTIFIER2);
    assertThat(table.snapshots()).hasSize(1);
  }
}
