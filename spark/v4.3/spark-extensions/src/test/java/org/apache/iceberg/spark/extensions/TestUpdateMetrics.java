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
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for verifying that update metrics from Spark's UpdateSummary are persisted in Iceberg's
 * snapshot summary.
 */
@ExtendWith(ParameterizedTestExtension.class)
public abstract class TestUpdateMetrics extends SparkRowLevelOperationsTestBase {

  @BeforeAll
  public static void setupSparkConf() {
    spark.conf().set("spark.sql.shuffle.partitions", "4");
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  protected abstract long expectedRowsCopied(long unchangedRowsInModifiedFiles);

  @TestTemplate
  public void testUpdateMetrics() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"software\" }\n"
            + "{ \"id\": 3, \"dep\": \"finance\" }");

    sql("UPDATE %s SET dep = 'updated' WHERE id = 1", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    Map<String, String> summary = currentSnapshot.summary();

    assertUpdateMetric(summary, "spark.update.num-updated-rows", 1);
    assertUpdateMetric(summary, "spark.update.num-copied-rows", expectedRowsCopied(2));
  }

  private void assertUpdateMetric(Map<String, String> summary, String key, long expectedValue) {
    assertThat(summary)
        .as("Snapshot summary should contain update metric: " + key)
        .containsKey(key);
    assertThat(summary.get(key))
        .as("Update metric " + key + " should have expected value")
        .isEqualTo(String.valueOf(expectedValue));
  }
}
