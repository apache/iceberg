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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.LocationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * End-to-end tests for v4 table reads using the Adaptive Metadata Tree format.
 *
 * <p>V4 manifests use TrackedFile schema in Parquet format. These tests verify that the full
 * pipeline works: Spark INSERT -> v4 Parquet manifest write -> v4 manifest read -> Spark SELECT.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestV4ReadEndToEnd extends TestBaseWithCatalog {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties()
      }
    };
  }

  @AfterEach
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testV4DataQuery() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version' = '4')",
        tableName);

    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

    List<Object[]> rows = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(rows).hasSize(3);
    assertThat(rows.get(0)).isEqualTo(row(1L, "a"));
    assertThat(rows.get(1)).isEqualTo(row(2L, "b"));
    assertThat(rows.get(2)).isEqualTo(row(3L, "c"));
  }

  @TestTemplate
  public void testV4MetadataTableQuery() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version' = '4')",
        tableName);

    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

    List<Object[]> files = sql("SELECT sum(record_count), count(*) FROM %s.files", tableName);
    assertThat(files).hasSize(1);
    assertThat(files.get(0)[0]).isEqualTo(3L); // total record count
    assertThat((long) files.get(0)[1]).isGreaterThanOrEqualTo(1L); // at least 1 data file
  }

  @TestTemplate
  public void testV4RootManifestFormat() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version' = '4')",
        tableName);

    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

    // verify data is readable
    List<Object[]> rows = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(rows).hasSize(3);

    // verify no snap-*.avro manifest list files exist (v4 uses root manifests in Parquet)
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.manifestListLocation()).endsWith(".parquet");
    assertThat(snapshot.manifestListLocation()).doesNotContain("snap-");
  }

  @TestTemplate
  public void testV4MultiSnapshot() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version' = '4')",
        tableName);

    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
    sql("INSERT INTO %s VALUES (4, 'd')", tableName);

    List<Object[]> rows = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(rows).hasSize(4);
    assertThat(rows.get(0)).isEqualTo(row(1L, "a"));
    assertThat(rows.get(3)).isEqualTo(row(4L, "d"));

    List<Object[]> files = sql("SELECT sum(record_count) FROM %s.files", tableName);
    assertThat(files).hasSize(1);
    assertThat(files.get(0)[0]).isEqualTo(4L);
  }

  @TestTemplate
  public void testV4RelativePathsInMetadata() throws IOException {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version' = '4')",
        tableName);

    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    FileIO io = table.io();

    // read the raw metadata JSON and verify manifest-list is a relative path
    JsonNode metadataJson;
    try (InputStream input = io.newInputFile(metadata.metadataFileLocation()).newStream()) {
      metadataJson = JsonUtil.mapper().readTree(input);
    }

    JsonNode snapshots = metadataJson.get("snapshots");
    assertThat(snapshots).isNotNull();
    assertThat(snapshots.size()).isGreaterThanOrEqualTo(1);

    for (JsonNode snap : snapshots) {
      String manifestList = snap.get("manifest-list").asText();
      // the stored path should be relative (no URI scheme)
      assertThat(LocationUtil.isAbsolute(manifestList))
          .as("manifest-list should be a relative path in v4 metadata: %s", manifestList)
          .isFalse();
      assertThat(manifestList).startsWith("/");
    }

    // verify the resolved paths work (data is still readable)
    Snapshot snapshot = table.currentSnapshot();
    assertThat(LocationUtil.isAbsolute(snapshot.manifestListLocation())).isTrue();

    List<Object[]> rows = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(rows).hasSize(2);
  }
}
