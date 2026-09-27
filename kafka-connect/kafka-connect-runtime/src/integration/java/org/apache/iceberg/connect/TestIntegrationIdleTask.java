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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

public class TestIntegrationIdleTask extends IntegrationTestBase {

  private static final String TEST_TABLE = "idle";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(TEST_DB, TEST_TABLE);

  @Override
  protected int connectPort() {
    return TestContext.DEFAULT_FLUSH_CONNECT_PORT;
  }

  @Test
  public void testIdleTaskCommits() {
    catalog().createTable(TABLE_IDENTIFIER, TestEvent.TEST_SCHEMA);

    KafkaConnectUtils.Config connectorConfig = createConfig(false);
    context().connectorCatalogProperties().forEach(connectorConfig::config);
    context().startConnector(connectPort(), connectorConfig);

    // a few records, then nothing: Connect calls put() about once per offset.flush.interval.ms
    sendEvents(false);
    flush();

    Awaitility.await()
        .atMost(Duration.ofMinutes(3))
        .pollInterval(Duration.ofSeconds(5))
        .untilAsserted(() -> assertSnapshotAdded(List.of(TABLE_IDENTIFIER)));

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, null);
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(2);
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createCommonConfig(useSchema)
        .config("tasks.max", 1)
        .config("iceberg.control.commit.timeout-ms", 30_000)
        .config("iceberg.tables", String.format("%s.%s", TEST_DB, TEST_TABLE));
  }

  @Override
  protected void sendEvents(boolean useSchema) {
    send(testTopic(), new TestEvent(1, "type1", Instant.now(), "hello world!"), useSchema);
    send(testTopic(), new TestEvent(2, "type2", Instant.now(), "having fun?"), useSchema);
  }

  @Override
  void dropTables() {
    catalog().dropTable(TABLE_IDENTIFIER);
  }
}
