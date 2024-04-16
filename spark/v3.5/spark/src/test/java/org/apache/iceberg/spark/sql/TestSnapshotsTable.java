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
package org.apache.iceberg.spark.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.spark.CatalogTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestSnapshotsTable extends CatalogTestBase {

  @BeforeEach
  public void createTables() {
    sql(
        "CREATE TABLE %s (id int, data string) USING iceberg "
            + "TBLPROPERTIES"
            + "('format-version'='2',"
            + "'write.delete.mode'='merge-on-read')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a1'),(2, 'a2'),(3, 'a3')", tableName);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testSnapshotsTable() {
    List<Object[]> sql = sql("SELECT * FROM %s.%s", tableName, MetadataTableType.SNAPSHOTS);
    assertThat(sql).hasSize(1);
  }

  @TestTemplate
  public void testTotalDataManifestFilesWithSnapshotsTableSummary() {
    List<Object[]> sql = sql("SELECT * FROM %s.%s", tableName, MetadataTableType.SNAPSHOTS);
    assertThat(sql).hasSize(1);
    Map<String, String> summary = (Map) sql.get(0)[5];
    assertThat(summary.get("total-data-manifest-files")).isEqualTo("1");
    assertThat(summary.get("total-delete-manifest-files")).isEqualTo(null);
    sql("INSERT INTO %s VALUES (4, 'a4')", tableName);
    sql = sql("SELECT * FROM %s.%s", tableName, MetadataTableType.SNAPSHOTS);
    assertThat(sql).hasSize(2);
    summary = (Map) sql.get(1)[5];
    assertThat(summary.get("total-data-manifest-files")).isEqualTo("2");
    assertThat(summary.get("total-delete-manifest-files")).isEqualTo(null);
  }

  @TestTemplate
  public void testTotalDeleteManifestFilesWithSnapshotsTableSummary() {
    List<Object[]> sql = sql("SELECT * FROM %s.%s", tableName, MetadataTableType.SNAPSHOTS);
    assertThat(sql).hasSize(1);
    Map<String, String> summary = (Map) sql.get(0)[5];
    assertThat(summary.get("total-data-manifest-files")).isEqualTo("1");
    assertThat(summary.get("total-delete-manifest-files")).isEqualTo(null);
    sql("INSERT INTO %s VALUES (1, 'a1'),(2, 'a2'),(3, 'a3'),(4, 'a4')", tableName);
    sql("INSERT INTO %s VALUES (1, 'a1'),(2, 'a2'),(3, 'a3'),(4, 'a4'),(5, 'a5')", tableName);
    sql("INSERT INTO %s VALUES (1, 'b1'),(2, 'b2'),(3, 'b3'),(4, 'b4')", tableName);
    sql("INSERT INTO %s VALUES (1, 'b1'),(2, 'b2'),(3, 'b3'),(4, 'b4'),(5, 'b5')", tableName);
    sql = sql("SELECT * FROM %s.%s", tableName, MetadataTableType.SNAPSHOTS);
    assertThat(sql).hasSize(5);
    summary = (Map) sql.get(4)[5];
    assertThat(summary.get("total-data-manifest-files")).isEqualTo("5");
    assertThat(summary.get("total-delete-manifest-files")).isEqualTo(null);

    sql("DELETE FROM %s WHERE id = 1", tableName);
    sql = sql("SELECT * FROM %s.%s", tableName, MetadataTableType.SNAPSHOTS);
    assertThat(sql).hasSize(6);
    summary = (Map) sql.get(5)[5];
    assertThat(summary.get("total-data-manifest-files")).isEqualTo("5");
    assertThat(summary.get("total-delete-manifest-files")).isEqualTo("1");
  }
}
