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
package io.tabular.iceberg.connect;

import static io.tabular.iceberg.connect.TestEvent.TEST_SCHEMA;
import static io.tabular.iceberg.connect.TestEvent.TEST_SPEC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StringType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

public abstract class AbstractIntegrationTest extends IntegrationTestBase {

  private static final String TEST_DB = "test";
  private static final String TEST_TABLE = "foobar";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(TEST_DB, TEST_TABLE);

  @BeforeEach
  public void before() {
    createTopic(testTopic, TEST_TOPIC_PARTITIONS);
    ((SupportsNamespaces) catalog).createNamespace(Namespace.of(TEST_DB));
  }

  @AfterEach
  public void after() {
    context.stopKafkaConnector(connectorName);
    deleteTopic(testTopic);
    catalog.dropTable(TableIdentifier.of(TEST_DB, TEST_TABLE));
    ((SupportsNamespaces) catalog).dropNamespace(Namespace.of(TEST_DB));
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testIcebergSinkPartitionedTable(String branch) {
    catalog.createTable(TABLE_IDENTIFIER, TEST_SCHEMA, TEST_SPEC);

    runTest(branch);

    List<DataFile> files = getDataFiles(TABLE_IDENTIFIER, branch);
    // partition may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(1, 2);
    assertEquals(1, files.get(0).recordCount());
    assertEquals(1, files.get(1).recordCount());
    assertSnapshotProps(TABLE_IDENTIFIER, branch);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testIcebergSinkUnpartitionedTable(String branch) {
    catalog.createTable(TABLE_IDENTIFIER, TEST_SCHEMA);

    runTest(branch);

    List<DataFile> files = getDataFiles(TABLE_IDENTIFIER, branch);
    // may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(1, 2);
    assertEquals(2, files.stream().mapToLong(DataFile::recordCount).sum());
    assertSnapshotProps(TABLE_IDENTIFIER, branch);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testIcebergSinkSchemaEvolution(String branch) {
    Schema initialSchema =
        new Schema(ImmutableList.of(Types.NestedField.required(1, "id", Types.LongType.get())));
    catalog.createTable(TABLE_IDENTIFIER, initialSchema);

    runTest(branch, ImmutableMap.of("iceberg.tables.evolveSchemaEnabled", "true"));

    List<DataFile> files = getDataFiles(TABLE_IDENTIFIER, branch);
    // may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(1, 2);
    assertEquals(2, files.stream().mapToLong(DataFile::recordCount).sum());
    assertSnapshotProps(TABLE_IDENTIFIER, branch);

    assertGeneratedSchema();
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testIcebergSinkAutoCreate(String branch) {
    runTest(branch, ImmutableMap.of("iceberg.tables.autoCreateEnabled", "true"));

    List<DataFile> files = getDataFiles(TABLE_IDENTIFIER, branch);
    // may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(1, 2);
    assertEquals(2, files.stream().mapToLong(DataFile::recordCount).sum());
    assertSnapshotProps(TABLE_IDENTIFIER, branch);

    assertGeneratedSchema();
  }

  private void assertGeneratedSchema() {
    Schema tableSchema = catalog.loadTable(TABLE_IDENTIFIER).schema();
    assertThat(tableSchema.findField("id").type()).isInstanceOf(LongType.class);
    assertThat(tableSchema.findField("type").type()).isInstanceOf(StringType.class);
    assertThat(tableSchema.findField("ts").type()).isInstanceOf(LongType.class);
    assertThat(tableSchema.findField("payload").type()).isInstanceOf(StringType.class);
  }

  private void runTest(String branch) {
    runTest(branch, ImmutableMap.of());
  }

  private void runTest(String branch, Map<String, String> extraConfig) {
    // set offset reset to earliest so we don't miss any test messages
    KafkaConnectContainer.Config connectorConfig =
        new KafkaConnectContainer.Config(connectorName)
            .config("topics", testTopic)
            .config("connector.class", IcebergSinkConnector.class.getName())
            .config("tasks.max", 2)
            .config("consumer.override.auto.offset.reset", "earliest")
            .config("key.converter", "org.apache.kafka.connect.json.JsonConverter")
            .config("key.converter.schemas.enable", false)
            .config("value.converter", "org.apache.kafka.connect.json.JsonConverter")
            .config("value.converter.schemas.enable", false)
            .config("iceberg.tables", String.format("%s.%s", TEST_DB, TEST_TABLE))
            .config("iceberg.control.commitIntervalMs", 1000)
            .config("iceberg.control.commitTimeoutMs", Integer.MAX_VALUE)
            .config("iceberg.kafka.auto.offset.reset", "earliest");
    connectorCatalogProperties().forEach(connectorConfig::config);

    if (branch != null) {
      connectorConfig.config("iceberg.tables.defaultCommitBranch", branch);
    }

    extraConfig.forEach(connectorConfig::config);

    context.startKafkaConnector(connectorConfig);

    TestEvent event1 = new TestEvent(1, "type1", System.currentTimeMillis(), "hello world!");

    long threeDaysAgo = System.currentTimeMillis() - Duration.ofDays(3).toMillis();
    TestEvent event2 = new TestEvent(2, "type2", threeDaysAgo, "having fun?");

    send(testTopic, event1);
    send(testTopic, event2);
    flush();

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(this::assertSnapshotAdded);
  }

  private void assertSnapshotAdded() {
    try {
      Table table = catalog.loadTable(TABLE_IDENTIFIER);
      assertThat(table.snapshots()).hasSize(1);
    } catch (NoSuchTableException e) {
      fail();
    }
  }
}
