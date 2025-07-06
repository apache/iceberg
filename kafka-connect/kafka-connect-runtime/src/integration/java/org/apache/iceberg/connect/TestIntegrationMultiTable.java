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

public class TestIntegrationMultiTable extends IntegrationTestBase {

  private static final String TEST_TABLE1 = "foobar1";
  private static final String TEST_TABLE2 = "foobar2";
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

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createCommonConfig(useSchema)
        .config(
            "iceberg.tables",
            String.format("%s.%s, %s.%s", TEST_DB, TEST_TABLE1, TEST_DB, TEST_TABLE2))
        .config("iceberg.tables.route-field", "type")
        .config(String.format("iceberg.table.%s.%s.route-regex", TEST_DB, TEST_TABLE1), "type1")
        .config(String.format("iceberg.table.%s.%s.route-regex", TEST_DB, TEST_TABLE2), "type2");
  }

  @Override
  protected void sendEvents(boolean useSchema) {
    TestEvent event1 = new TestEvent(1, "type1", Instant.now(), "hello world!");
    TestEvent event2 = new TestEvent(2, "type2", Instant.now(), "having fun?");
    TestEvent event3 = new TestEvent(3, "type3", Instant.now(), "ignore me");

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
