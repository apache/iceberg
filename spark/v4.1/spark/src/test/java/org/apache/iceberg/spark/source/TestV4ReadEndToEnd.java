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

import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.TestBaseWithCatalog;
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

    List<Object[]> files =
        sql("SELECT sum(record_count), count(*) FROM %s.files", tableName);
    assertThat(files).hasSize(1);
    assertThat(files.get(0)[0]).isEqualTo(3L); // total record count
    assertThat((long) files.get(0)[1]).isGreaterThanOrEqualTo(1L); // at least 1 data file
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
}
