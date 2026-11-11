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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

class TestNativeChangelogCatalogs extends CatalogTestBase {

  @AfterEach
  void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  void routesNativeCopyOnWriteChangesThroughCatalogs() throws Exception {
    withSQLConf(
        Map.of("spark.sql.shuffle.partitions", "4"),
        () -> assertThatCode(this::assertNativeCopyOnWriteChanges).doesNotThrowAnyException());
  }

  private void assertNativeCopyOnWriteChanges() throws Exception {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3', 'write.update.mode'='copy-on-write')",
        tableName);
    sql("INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 'a'), (2, 'b')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot initial = table.currentSnapshot();
    Map<Long, Long> rowIds = Maps.newHashMap();
    for (Row row : spark.table(tableName).select("id", "_row_id").collectAsList()) {
      rowIds.put(row.getLong(0), row.getLong(1));
    }

    waitUntilAfter(initial.timestampMillis());
    sql("UPDATE %s SET data = 'updated' WHERE id = 1", tableName);
    table.refresh();
    Snapshot update = table.currentSnapshot();
    String version = String.valueOf(update.sequenceNumber());
    List<Row> changes =
        List.of(
            event(1, "a", rowIds.get(1L), initial, "delete", update),
            event(1, "updated", rowIds.get(1L), update, "insert", update));
    List<Row> updates =
        List.of(
            event(1, "a", rowIds.get(1L), initial, "update_preimage", update),
            event(1, "updated", rowIds.get(1L), update, "update_postimage", update));
    List<Row> raw =
        List.of(
            changes.get(0),
            changes.get(1),
            event(2, "b", rowIds.get(2L), initial, "delete", update),
            event(2, "b", rowIds.get(2L), initial, "insert", update));

    for (String setting : List.of("defaults", "dropCarryovers", "updates", "raw")) {
      Map<String, String> options = Maps.newHashMap();
      options.put("startingVersion", version);
      options.put("endingVersion", version);
      String withClause = "";
      List<Row> expected = changes;
      if (!setting.equals("defaults")) {
        String mode = setting.equals("raw") ? "none" : "dropCarryovers";
        boolean computeUpdates = setting.equals("updates");
        options.put("deduplicationMode", mode);
        options.put("computeUpdates", String.valueOf(computeUpdates));
        withClause =
            String.format(
                " WITH (deduplicationMode = '%s', computeUpdates = '%s')", mode, computeUpdates);
        expected =
            switch (setting) {
              case "raw" -> raw;
              case "updates" -> updates;
              default -> changes;
            };
      }

      assertThat(spark.read().options(options).changes(tableName).collectAsList())
          .as("%s DataFrame %s", catalogName, setting)
          .containsExactlyInAnyOrderElementsOf(expected);
      assertThat(
              spark
                  .sql(
                      String.format(
                          "SELECT * FROM %s CHANGES FROM VERSION %s TO VERSION %s%s",
                          tableName, version, version, withClause))
                  .collectAsList())
          .as("%s SQL %s", catalogName, setting)
          .containsExactlyInAnyOrderElementsOf(expected);

      if (setting.equals("updates") || setting.equals("raw")) {
        Dataset<Row> stream = spark.readStream().options(options).changes(tableName);
        String checkpoint = temp.resolve(setting + "-checkpoint").toString();
        String output = temp.resolve(setting + "-output").toString();
        StreamingQuery query =
            stream
                .writeStream()
                .format("parquet")
                .option("checkpointLocation", checkpoint)
                .trigger(Trigger.AvailableNow())
                .start(output);
        try {
          assertThat(query.awaitTermination(60_000)).isTrue();
        } finally {
          query.stop();
        }

        assertThat(spark.read().parquet(output).collectAsList())
            .as("%s streaming %s", catalogName, setting)
            .containsExactlyInAnyOrderElementsOf(expected);
      }
    }
  }

  @TestTemplate
  void sessionCatalogFallsBackForNonIcebergChanges() {
    assumeTrue(catalogName.equals(SparkCatalogConfig.SPARK_SESSION.catalogName()));
    sql("CREATE TABLE %s (id bigint) USING parquet", tableName);
    assertThatThrownBy(() -> spark.read().changes(tableName).collectAsList())
        .hasStackTraceContaining("CHANGE_DATA_CAPTURE");
    assertThatThrownBy(
            () ->
                spark
                    .sql(String.format("SELECT * FROM %s CHANGES FROM VERSION 1", tableName))
                    .collectAsList())
        .hasStackTraceContaining("CHANGE_DATA_CAPTURE");
  }

  private static Row event(
      long id, String data, long rowId, Snapshot rowWrite, String change, Snapshot commit) {
    return RowFactory.create(
        id,
        data,
        rowId,
        rowWrite.sequenceNumber(),
        change,
        commit.sequenceNumber(),
        new Timestamp(commit.timestampMillis()));
  }
}
