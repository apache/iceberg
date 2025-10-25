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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.List;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.TestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.unsafe.types.VariantVal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestSparkVariantRead extends TestBase {

  private static final String CATALOG = "local";
  private static final String TABLE = CATALOG + ".default.var";

  @BeforeAll
  public static void setupCatalog() {
    // Use a Hadoop catalog to avoid Hive schema conversion (Hive doesn't support VARIANT yet)
    spark.conf().set("spark.sql.catalog." + CATALOG, SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog." + CATALOG + ".type", "hadoop");
    spark.conf().set("spark.sql.catalog." + CATALOG + ".default-namespace", "default");
    spark.conf().set("spark.sql.catalog." + CATALOG + ".cache-enabled", "false");
    // point warehouse to a temp directory
    String temp = System.getProperty("java.io.tmpdir") + "/iceberg_spark_variant_warehouse";
    spark.conf().set("spark.sql.catalog." + CATALOG + ".warehouse", temp);
  }

  @BeforeEach
  public void setupTable() {
    sql("DROP TABLE IF EXISTS %s", TABLE);
    sql(
        "CREATE TABLE %s (id BIGINT, v1 VARIANT, v2 VARIANT) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        TABLE);

    String v1r1 = "{\"a\":1}";
    String v2r1 = "{\"x\":10}";
    String v1r2 = "{\"b\":2}";
    String v2r2 = "{\"y\":20}";

    sql("INSERT INTO %s SELECT 1, parse_json('%s'), parse_json('%s')", TABLE, v1r1, v2r1);
    sql("INSERT INTO %s SELECT 2, parse_json('%s'), parse_json('%s')", TABLE, v1r2, v2r2);
  }

  @AfterEach
  public void cleanup() {
    sql("DROP TABLE IF EXISTS %s", TABLE);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testVariantColumnProjection_singleVariant(boolean vectorized) {
    assumeThat(vectorized).as("Variant vectorized Parquet read is not implemented yet").isFalse();
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('read.parquet.vectorization.enabled'='%s')",
        TABLE, String.valueOf(vectorized));
    Dataset<Row> df = spark.table(TABLE).select("id", "v1").orderBy("id");
    assertThat(df.schema().fieldNames()).containsExactly("id", "v1");
    assertThat(df.count()).isEqualTo(2);

    List<Row> directRows = df.collectAsList();
    Object v1row1 = directRows.get(0).get(1);
    Object v1row2 = directRows.get(1).get(1);
    assertThat(v1row1).isInstanceOf(VariantVal.class);
    assertThat(v1row2).isInstanceOf(VariantVal.class);
    VariantVal r1 = (VariantVal) v1row1;
    VariantVal r2 = (VariantVal) v1row2;
    Variant vv1 = new Variant(r1.getValue(), r1.getMetadata());
    Variant vv2 = new Variant(r2.getValue(), r2.getMetadata());

    // row 1 has {"a":1}
    Variant fieldA = vv1.getFieldByKey("a");
    assertThat(fieldA).isNotNull();
    assertThat(fieldA.getLong()).isEqualTo(1L);

    // row 2 has {"b":2}
    Variant fieldB = vv2.getFieldByKey("b");
    assertThat(fieldB).isNotNull();
    assertThat(fieldB.getLong()).isEqualTo(2L);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testVariantColumnProjectionNoVariant(boolean vectorized) {
    assumeThat(vectorized).as("Variant vectorized Parquet read is not implemented yet").isFalse();
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('read.parquet.vectorization.enabled'='%s')",
        TABLE, String.valueOf(vectorized));
    Dataset<Row> df = spark.table(TABLE).select("id");
    assertThat(df.schema().fieldNames()).containsExactly("id");
    assertThat(df.count()).isEqualTo(2);
    assertThat(df.collectAsList()).extracting(r -> r.getLong(0)).containsExactlyInAnyOrder(1L, 2L);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testFilterOnVariantColumnOnWholeValue(boolean vectorized) {
    assumeThat(vectorized).as("Variant vectorized Parquet read is not implemented yet").isFalse();
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('read.parquet.vectorization.enabled'='%s')",
        TABLE, String.valueOf(vectorized));
    sql("INSERT INTO %s SELECT 3, NULL, NULL", TABLE);

    Dataset<Row> nullDf = spark.table(TABLE).where("v1 IS NULL").select("id");
    assertThat(nullDf.collectAsList()).extracting(r -> r.getLong(0)).containsExactly(3L);

    Dataset<Row> notNullDf = spark.table(TABLE).where("v1 IS NOT NULL").select("id");
    assertThat(notNullDf.collectAsList())
        .extracting(r -> r.getLong(0))
        .containsExactlyInAnyOrder(1L, 2L);

    // verify variant contents for non-null rows
    Dataset<Row> notNullVals =
        spark
            .table(TABLE)
            .where("v1 IS NOT NULL")
            .selectExpr("id", "to_json(v1) as v1_json")
            .orderBy("id");
    List<Row> nn = notNullVals.collectAsList();
    assertThat(nn).hasSize(2);
    assertThat(nn.get(0).getLong(0)).isEqualTo(1L);
    assertThat(nn.get(0).getString(1)).isEqualTo("{\"a\":1}");
    assertThat(nn.get(1).getLong(0)).isEqualTo(2L);
    assertThat(nn.get(1).getString(1)).isEqualTo("{\"b\":2}");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testVariantNullValueProjection(boolean vectorized) {
    assumeThat(vectorized).as("Variant vectorized Parquet read is not implemented yet").isFalse();
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('read.parquet.vectorization.enabled'='%s')",
        TABLE, String.valueOf(vectorized));

    // insert a row with NULL variant values
    sql("INSERT INTO %s SELECT 10, NULL, NULL", TABLE);

    // select id and variant; ensure the variant value is null
    Dataset<Row> df = spark.table(TABLE).where("id = 10").select("id", "v1");
    List<Row> rows = df.collectAsList();
    assertThat(rows).hasSize(1);
    Row row = rows.get(0);
    assertThat(row.getLong(0)).isEqualTo(10L);
    assertThat(row.isNullAt(1)).isTrue();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testNestedStructVariant(boolean vectorized) {
    assumeThat(vectorized).as("Variant vectorized Parquet read is not implemented yet").isFalse();
    spark.conf().set("spark.sql.parquet.enableVectorizedReader", String.valueOf(vectorized));

    String structTable = CATALOG + ".default.var_struct";
    sql("DROP TABLE IF EXISTS %s", structTable);
    sql(
        "CREATE TABLE %s (id BIGINT, s STRUCT<v: VARIANT>) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3', 'read.parquet.vectorization.enabled'='false')",
        structTable);

    String j1 = "{\"a\":1}";
    String j2 = "{\"b\":2}";
    sql("INSERT INTO %s SELECT 1, named_struct('v', parse_json('%s'))", structTable, j1);
    sql("INSERT INTO %s SELECT 2, named_struct('v', parse_json('%s'))", structTable, j2);

    Dataset<Row> df =
        spark.table(structTable).selectExpr("id", "to_json(s.v) as v_json").orderBy("id");
    java.util.List<Row> rows = df.collectAsList();
    assertThat(rows.get(0).getLong(0)).isEqualTo(1L);
    assertThat(rows.get(0).getString(1)).isEqualTo("{\"a\":1}");
    assertThat(rows.get(1).getLong(0)).isEqualTo(2L);
    assertThat(rows.get(1).getString(1)).isEqualTo("{\"b\":2}");

    sql("DROP TABLE IF EXISTS %s", structTable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testNestedArrayVariant(boolean vectorized) {
    assumeThat(vectorized).as("Variant vectorized Parquet read is not implemented yet").isFalse();
    spark.conf().set("spark.sql.parquet.enableVectorizedReader", String.valueOf(vectorized));

    String arrayTable = CATALOG + ".default.var_array";
    sql("DROP TABLE IF EXISTS %s", arrayTable);
    sql(
        "CREATE TABLE %s (id BIGINT, arr ARRAY<VARIANT>) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3', 'read.parquet.vectorization.enabled'='false')",
        arrayTable);

    String a1 = "{\"a\":1}";
    String a2 = "{\"x\":10}";
    String b1 = "{\"b\":2}";
    String b2 = "{\"y\":20}";
    sql("INSERT INTO %s SELECT 1, array(parse_json('%s'), parse_json('%s'))", arrayTable, a1, a2);
    sql("INSERT INTO %s SELECT 2, array(parse_json('%s'), parse_json('%s'))", arrayTable, b1, b2);

    Dataset<Row> df =
        spark
            .table(arrayTable)
            .selectExpr("id", "to_json(arr[0]) as e0_json", "to_json(arr[1]) as e1_json")
            .orderBy("id");
    java.util.List<Row> rows = df.collectAsList();
    assertThat(rows.get(0).getLong(0)).isEqualTo(1L);
    assertThat(rows.get(0).getString(1)).isEqualTo("{\"a\":1}");
    assertThat(rows.get(0).getString(2)).isEqualTo("{\"x\":10}");
    assertThat(rows.get(1).getLong(0)).isEqualTo(2L);
    assertThat(rows.get(1).getString(1)).isEqualTo("{\"b\":2}");
    assertThat(rows.get(1).getString(2)).isEqualTo("{\"y\":20}");

    sql("DROP TABLE IF EXISTS %s", arrayTable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testNestedMapVariant(boolean vectorized) {
    assumeThat(vectorized).as("Variant vectorized Parquet read is not implemented yet").isFalse();
    spark.conf().set("spark.sql.parquet.enableVectorizedReader", String.valueOf(vectorized));

    String mapTable = CATALOG + ".default.var_map";
    sql("DROP TABLE IF EXISTS %s", mapTable);
    sql(
        "CREATE TABLE %s (id BIGINT, m MAP<STRING, VARIANT>) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3', 'read.parquet.vectorization.enabled'='false')",
        mapTable);

    String k1a = "{\"a\":1}";
    String k2x = "{\"x\":10}";
    String k1b = "{\"b\":2}";
    String k2y = "{\"y\":20}";
    sql(
        "INSERT INTO %s SELECT 1, map('k1', parse_json('%s'), 'k2', parse_json('%s'))",
        mapTable, k1a, k2x);
    sql(
        "INSERT INTO %s SELECT 2, map('k1', parse_json('%s'), 'k2', parse_json('%s'))",
        mapTable, k1b, k2y);

    Dataset<Row> df =
        spark
            .table(mapTable)
            .selectExpr(
                "id",
                "to_json(element_at(m, 'k1')) as k1_json",
                "to_json(element_at(m, 'k2')) as k2_json")
            .orderBy("id");
    java.util.List<Row> rows = df.collectAsList();
    assertThat(rows.get(0).getLong(0)).isEqualTo(1L);
    assertThat(rows.get(0).getString(1)).isEqualTo("{\"a\":1}");
    assertThat(rows.get(0).getString(2)).isEqualTo("{\"x\":10}");
    assertThat(rows.get(1).getLong(0)).isEqualTo(2L);
    assertThat(rows.get(1).getString(1)).isEqualTo("{\"b\":2}");
    assertThat(rows.get(1).getString(2)).isEqualTo("{\"y\":20}");

    sql("DROP TABLE IF EXISTS %s", mapTable);
  }
}
