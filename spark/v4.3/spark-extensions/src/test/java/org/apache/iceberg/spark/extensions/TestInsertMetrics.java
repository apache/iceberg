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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for verifying that insert metrics from Spark's InsertSummary are persisted in Iceberg's
 * snapshot summary.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestInsertMetrics extends SparkRowLevelOperationsTestBase {

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of();
  }

  @TestTemplate
  public void testInsertMetrics() {
    createAndInitTable("id INT, dep STRING", "{ \"id\": 1, \"dep\": \"hr\" }");

    sql("INSERT INTO %s VALUES (2, 'software'), (3, 'finance'), (4, 'marketing')", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    Map<String, String> summary = currentSnapshot.summary();

    assertInsertMetric(summary, "spark.insert.num-inserted-rows", 3);
  }

  private void assertInsertMetric(Map<String, String> summary, String key, long expectedValue) {
    assertThat(summary)
        .as("Snapshot summary should contain insert metric: " + key)
        .containsKey(key);
    assertThat(summary.get(key))
        .as("Insert metric " + key + " should have expected value")
        .isEqualTo(String.valueOf(expectedValue));
  }
}
