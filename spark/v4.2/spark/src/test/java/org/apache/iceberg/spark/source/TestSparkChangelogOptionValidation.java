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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

class TestSparkChangelogOptionValidation extends TestBaseWithCatalog {

  private static final Map<String, String> UNSUPPORTED_OPTIONS =
      Map.ofEntries(
          Map.entry("branch", "audit"),
          Map.entry("versionAsOf", "1"),
          Map.entry("timestampAsOf", "2024-01-01 00:00:00"),
          Map.entry("snapshot-id", "1"),
          Map.entry("as-of-timestamp", "0"),
          Map.entry("tag", "audit"),
          Map.entry("start-snapshot-id", "1"),
          Map.entry("end-snapshot-id", "1"),
          Map.entry("start-timestamp", "0"),
          Map.entry("end-timestamp", "0"),
          Map.entry("stream-from-timestamp", "0"),
          Map.entry("streaming-skip-delete-snapshots", "true"),
          Map.entry("streaming-skip-overwrite-snapshots", "true"));

  @AfterEach
  void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  void rejectsUnsupportedDataFrameOptions() {
    createTable();
    for (Map.Entry<String, String> option : UNSUPPORTED_OPTIONS.entrySet()) {
      assertThatThrownBy(
              () ->
                  spark
                      .read()
                      .option("deduplicationMode", "none")
                      .option("computeUpdates", "false")
                      .option(option.getKey(), option.getValue())
                      .changes(tableName)
                      .collectAsList())
          .as("CDC must reject option %s", option.getKey())
          .hasStackTraceContaining("is not supported for Spark CDC")
          .hasStackTraceContaining(option.getKey());
    }
  }

  @TestTemplate
  void rejectsUnsupportedSqlOptions() {
    createTable();
    long version = validationCatalog.loadTable(tableIdent).currentSnapshot().sequenceNumber();
    for (Map.Entry<String, String> option : UNSUPPORTED_OPTIONS.entrySet()) {
      assertThatThrownBy(
              () ->
                  sql(
                      "SELECT * FROM %s CHANGES FROM VERSION %d TO VERSION %d WITH "
                          + "(deduplicationMode = 'none', computeUpdates = 'false', `%s` = '%s')",
                      tableName, version, version, option.getKey(), option.getValue()))
          .as("SQL CDC must reject option %s", option.getKey())
          .hasStackTraceContaining("is not supported for Spark CDC")
          .hasStackTraceContaining(option.getKey());
    }
  }

  @TestTemplate
  void rejectsUnsupportedStreamingOptions() {
    createTable();
    for (Map.Entry<String, String> option : UNSUPPORTED_OPTIONS.entrySet()) {
      assertThatThrownBy(
              () ->
                  spark
                      .readStream()
                      .option("deduplicationMode", "none")
                      .option("computeUpdates", "false")
                      .option(option.getKey(), option.getValue())
                      .changes(tableName)
                      .schema())
          .as("Streaming CDC must reject option %s", option.getKey())
          .hasStackTraceContaining("is not supported for Spark CDC")
          .hasStackTraceContaining(option.getKey());
    }
  }

  private void createTable() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
  }
}
