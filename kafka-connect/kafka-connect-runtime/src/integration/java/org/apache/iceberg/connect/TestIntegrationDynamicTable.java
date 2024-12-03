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

import java.time.Instant;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
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
//<<<<<<< HEAD
    runTest(branch, useSchema, ImmutableMap.of(), List.of(TABLE_IDENTIFIER1, TABLE_IDENTIFIER2));
//=======
    boolean useNewConfiguration = branch == null; // use new configuration for one of the tests
    runTest(branch, useSchema, useNewConfiguration, List.of(TABLE_IDENTIFIER1, TABLE_IDENTIFIER2));
//>>>>>>> 137439d47 (Overload existing configuration instead of new configuration keys)

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER1, branch);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).recordCount()).isEqualTo(1);
    assertSnapshotProps(TABLE_IDENTIFIER1, branch);

    files = dataFiles(TABLE_IDENTIFIER2, branch);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).recordCount()).isEqualTo(1);
    assertSnapshotProps(TABLE_IDENTIFIER2, branch);
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createCommonConfig(useSchema)
        .config("iceberg.tables.dynamic-enabled", true)
        .config("iceberg.tables.route-field", "payload");
  }

  private void runTest(String branch, boolean useSchema, boolean useNewConfiguration) {
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
            .config("iceberg.tables.route-field", "payload")
            .config("iceberg.control.commit.interval-ms", 1000)
            .config("iceberg.control.commit.timeout-ms", Integer.MAX_VALUE)
            .config("iceberg.kafka.auto.offset.reset", "earliest");

    if (useNewConfiguration) {
      connectorConfig.config(
          "iceberg.tables.route-with",
          "org.apache.iceberg.connect.data.RecordRouter$DynamicRecordRouter");
    } else {
      connectorConfig.config("iceberg.tables.dynamic-enabled", true);
    }

    context().connectorCatalogProperties().forEach(connectorConfig::config);

    if (branch != null) {
      connectorConfig.config("iceberg.tables.default-commit-branch", branch);
    }

    if (!useSchema) {
      connectorConfig.config("value.converter.schemas.enable", false);
    }

    context().startConnector(connectorConfig);

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
