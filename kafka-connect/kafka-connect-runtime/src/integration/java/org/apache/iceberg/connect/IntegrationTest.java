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
import static org.assertj.core.api.Assertions.fail;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.TimestampType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

public class IntegrationTest extends IntegrationTestBase {

  private static final String TEST_DB = "test";
  private static final String TEST_TABLE = "foobar";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(TEST_DB, TEST_TABLE);

  @BeforeEach
  public void before() {
    createTopic(testTopic(), TEST_TOPIC_PARTITIONS);
    ((SupportsNamespaces) catalog()).createNamespace(Namespace.of(TEST_DB));
  }

  @AfterEach
  public void after() {
    context().stopConnector(connectorName());
    deleteTopic(testTopic());
    catalog().dropTable(TableIdentifier.of(TEST_DB, TEST_TABLE));
    ((SupportsNamespaces) catalog()).dropNamespace(Namespace.of(TEST_DB));
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testIcebergSinkPartitionedTable(String branch) {
    catalog().createTable(TABLE_IDENTIFIER, TestEvent.TEST_SCHEMA, TestEvent.TEST_SPEC);

    boolean useSchema = branch == null; // use a schema for one of the tests
    runTest(branch, useSchema, ImmutableMap.of());

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, branch);
    // partition may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(1, 2);
    assertThat(files.get(0).recordCount()).isEqualTo(1);
    assertThat(files.get(1).recordCount()).isEqualTo(1);
    assertSnapshotProps(TABLE_IDENTIFIER, branch);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testIcebergSinkUnpartitionedTable(String branch) {
    catalog().createTable(TABLE_IDENTIFIER, TestEvent.TEST_SCHEMA);

    boolean useSchema = branch == null; // use a schema for one of the tests
    runTest(branch, useSchema, ImmutableMap.of());

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, branch);
    // may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(1, 2);
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(2);
    assertSnapshotProps(TABLE_IDENTIFIER, branch);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testIcebergSinkSchemaEvolution(String branch) {
    Schema initialSchema =
        new Schema(
            ImmutableList.of(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "type", Types.StringType.get())));
    catalog().createTable(TABLE_IDENTIFIER, initialSchema);

    boolean useSchema = branch == null; // use a schema for one of the tests
    runTest(branch, useSchema, ImmutableMap.of("iceberg.tables.evolve-schema-enabled", "true"));

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, branch);
    // may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(1, 2);
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(2);
    assertSnapshotProps(TABLE_IDENTIFIER, branch);

    // when not using a value schema, the ID data type will not be updated
    Class<? extends Type> expectedIdType =
        useSchema ? Types.LongType.class : Types.IntegerType.class;

    assertGeneratedSchema(useSchema, expectedIdType);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testIcebergSinkAutoCreate(String branch) {
    boolean useSchema = branch == null; // use a schema for one of the tests

    Map<String, String> extraConfig = Maps.newHashMap();
    extraConfig.put("iceberg.tables.auto-create-enabled", "true");
    if (useSchema) {
      // partition the table for one of the tests
      extraConfig.put("iceberg.tables.default-partition-by", "hour(ts)");
    }

    runTest(branch, useSchema, extraConfig);

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, branch);
    // may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(1, 2);
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(2);
    assertSnapshotProps(TABLE_IDENTIFIER, branch);

    assertGeneratedSchema(useSchema, LongType.class);

    PartitionSpec spec = catalog().loadTable(TABLE_IDENTIFIER).spec();
    assertThat(spec.isPartitioned()).isEqualTo(useSchema);
  }

  private void assertGeneratedSchema(boolean useSchema, Class<? extends Type> expectedIdType) {
    Schema tableSchema = catalog().loadTable(TABLE_IDENTIFIER).schema();
    assertThat(tableSchema.findField("id").type()).isInstanceOf(expectedIdType);
    assertThat(tableSchema.findField("type").type()).isInstanceOf(StringType.class);
    assertThat(tableSchema.findField("payload").type()).isInstanceOf(StringType.class);

    if (!useSchema) {
      // without a schema we can only map the primitive type
      assertThat(tableSchema.findField("ts").type()).isInstanceOf(LongType.class);
      // null values should be ignored when not using a value schema
      assertThat(tableSchema.findField("op")).isNull();
    } else {
      assertThat(tableSchema.findField("ts").type()).isInstanceOf(TimestampType.class);
      assertThat(tableSchema.findField("op").type()).isInstanceOf(StringType.class);
    }
  }

  private void runTest(String branch, boolean useSchema, Map<String, String> extraConfig) {
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
            .config("iceberg.tables", String.format("%s.%s", TEST_DB, TEST_TABLE))
            .config("iceberg.control.commit.interval-ms", 1000)
            .config("iceberg.control.commit.timeout-ms", Integer.MAX_VALUE)
            .config("iceberg.kafka.auto.offset.reset", "earliest");

    context().connectorCatalogProperties().forEach(connectorConfig::config);

    if (branch != null) {
      connectorConfig.config("iceberg.tables.default-commit-branch", branch);
    }

    extraConfig.forEach(connectorConfig::config);

    context().startConnector(connectorConfig);

    TestEvent event1 = new TestEvent(1, "type1", Instant.now(), "hello world!");

    Instant threeDaysAgo = Instant.now().minus(Duration.ofDays(3));
    TestEvent event2 = new TestEvent(2, "type2", threeDaysAgo, "having fun?");

    send(testTopic(), event1, useSchema);
    send(testTopic(), event2, useSchema);
    flush();

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(this::assertSnapshotAdded);
  }

  private void assertSnapshotAdded() {
    try {
      Table table = catalog().loadTable(TABLE_IDENTIFIER);
      assertThat(table.snapshots()).hasSize(1);
    } catch (NoSuchTableException e) {
      fail("Table should exist");
    }
  }
}
