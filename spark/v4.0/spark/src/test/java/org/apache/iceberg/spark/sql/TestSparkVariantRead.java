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
}
