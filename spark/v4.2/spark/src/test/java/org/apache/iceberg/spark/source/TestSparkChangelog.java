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

import static org.apache.iceberg.TestHelpers.row;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

class TestSparkChangelog extends TestBaseWithCatalog {

  @AfterEach
  void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  void readsChangesUsingSparkCdcSyntax() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    long firstVersion = table.currentSnapshot().sequenceNumber();

    sql("INSERT INTO %s VALUES (3, 'c')", tableName);
    table.refresh();
    long secondVersion = table.currentSnapshot().sequenceNumber();

    assertThat(
            sql(
                "SELECT id, data, _change_type, _commit_version "
                    + "FROM %s CHANGES FROM VERSION %d TO VERSION %d ORDER BY id",
                tableName, firstVersion, secondVersion))
        .containsExactly(
            row(1L, "a", "insert", firstVersion),
            row(2L, "b", "insert", firstVersion),
            row(3L, "c", "insert", secondVersion));
  }

  @TestTemplate
  void readsCopyOnWriteChangesUsingSparkCdcSyntax() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    sql("DELETE FROM %s WHERE id = 1", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    long deleteVersion = table.currentSnapshot().sequenceNumber();
    assertThat(table.currentSnapshot().deleteManifests(table.io())).isEmpty();

    assertThat(
            sql(
                "SELECT id, data, _change_type, _commit_version "
                    + "FROM %s CHANGES FROM VERSION %d TO VERSION %d "
                    + "ORDER BY _change_type, id",
                tableName, deleteVersion, deleteVersion))
        .containsExactly(row(1L, "a", "delete", deleteVersion));

    assertThat(
            sql(
                "SELECT id, data, _change_type, _commit_version "
                    + "FROM %s CHANGES FROM VERSION %d TO VERSION %d "
                    + "WITH (deduplicationMode = 'none') "
                    + "ORDER BY _change_type, id",
                tableName, deleteVersion, deleteVersion))
        .containsExactly(
            row(1L, "a", "delete", deleteVersion),
            row(2L, "b", "delete", deleteVersion),
            row(2L, "b", "insert", deleteVersion));
  }

  @TestTemplate
  void computesUpdatesForCopyOnWriteChanges() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    sql("UPDATE %s SET data = 'updated' WHERE id = 1", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    long updateVersion = table.currentSnapshot().sequenceNumber();

    assertThat(
            sql(
                "SELECT id, data, _change_type, _commit_version "
                    + "FROM %s CHANGES FROM VERSION %d TO VERSION %d "
                    + "WITH (computeUpdates = 'true') ORDER BY data",
                tableName, updateVersion, updateVersion))
        .containsExactly(
            row(1L, "a", "update_preimage", updateVersion),
            row(1L, "updated", "update_postimage", updateVersion));
  }

  @TestTemplate
  void availableNowPinsLatestSnapshot() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    long availableSnapshotId = table.currentSnapshot().snapshotId();
    SparkReadConf readConf = new SparkReadConf(spark, table, CaseInsensitiveStringMap.empty());
    SparkChangelogMicroBatchStream stream =
        new SparkChangelogMicroBatchStream(
            JavaSparkContext.fromSparkContext(spark.sparkContext()),
            table,
            readConf,
            SparkChangelogTable.cdcDataSchema(table),
            temp.resolve("cdc-available-now").toString());

    stream.prepareForTriggerAvailableNow();
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);

    StreamingOffset latestOffset = (StreamingOffset) stream.latestOffset();
    assertThat(latestOffset.snapshotId()).isEqualTo(availableSnapshotId);
    stream.stop();
  }

  @TestTemplate
  void streamsChangesUsingSparkCdcApi() throws Exception {
    String queryName = "iceberg_cdc_changes";
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    Dataset<Row> changes = spark.readStream().changes(tableName);
    StreamingQuery query =
        changes
            .writeStream()
            .format("memory")
            .queryName(queryName)
            .trigger(Trigger.AvailableNow())
            .start();
    query.awaitTermination();

    assertThat(sql("SELECT id, data, _change_type FROM %s ORDER BY id", queryName))
        .containsExactly(row(1L, "a", "insert"), row(2L, "b", "insert"));
    spark.catalog().dropTempView(queryName);
  }

  @TestTemplate
  void tableChangesKeepsIcebergChangelogColumns() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);

    assertThat(sql("SELECT id, data, _change_type, _commit_snapshot_id FROM %s.changes", tableName))
        .hasSize(1)
        .allSatisfy(
            row -> {
              assertThat(row[2]).isEqualTo("INSERT");
              assertThat(row[3]).isInstanceOf(Long.class);
            });
  }
}
