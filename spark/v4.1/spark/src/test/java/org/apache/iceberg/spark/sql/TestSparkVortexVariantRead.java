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
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.unsafe.types.VariantVal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Round-trips VARIANT columns through Vortex-format tables via Spark SQL.
 *
 * <p>Vortex reads default to the columnar ({@code ColumnarBatch}) path, but that path cannot
 * surface a variant as Spark's {@link VariantVal}, so {@code SparkBatch} falls back to the
 * row-based reader whenever a variant is projected. Projecting only {@code id} keeps the columnar
 * path; projecting a variant column exercises the row-based reader.
 */
public class TestSparkVortexVariantRead extends TestBase {

  private static final String CATALOG = "local";
  private static final String TABLE = CATALOG + ".default.var_vortex";

  @BeforeAll
  public static void setupCatalog() {
    // Use a Hadoop catalog to avoid Hive schema conversion (Hive doesn't support VARIANT yet)
    spark.conf().set("spark.sql.catalog." + CATALOG, SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog." + CATALOG + ".type", "hadoop");
    spark.conf().set("spark.sql.catalog." + CATALOG + ".default-namespace", "default");
    spark.conf().set("spark.sql.catalog." + CATALOG + ".cache-enabled", "false");
    String temp = System.getProperty("java.io.tmpdir") + "/iceberg_spark_vortex_variant_warehouse";
    spark.conf().set("spark.sql.catalog." + CATALOG + ".warehouse", temp);
  }

  @BeforeEach
  public void setupTable() {
    sql("DROP TABLE IF EXISTS %s", TABLE);
    sql(
        "CREATE TABLE %s (id BIGINT, v1 VARIANT, v2 VARIANT, values ARRAY<BIGINT>, "
            + "details STRUCT<existing: STRING>) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3', 'write.format.default'='vortex')",
        TABLE);

    sql(
        "INSERT INTO %s SELECT 1, parse_json('{\"a\":1}'), parse_json('{\"x\":10}'), "
            + "array(1L, NULL, 3L), named_struct('existing', 'one')",
        TABLE);
    sql(
        "INSERT INTO %s SELECT 2, parse_json('{\"b\":2}'), parse_json('{\"y\":20}'), "
            + "array(), named_struct('existing', 'two')",
        TABLE);
  }

  @AfterEach
  public void cleanup() {
    sql("DROP TABLE IF EXISTS %s", TABLE);
  }

  @Test
  public void readVariantColumns() {
    List<Row> rows = spark.table(TABLE).select("id", "v1", "v2").orderBy("id").collectAsList();
    assertThat(rows).hasSize(2);

    assertThat(rows.get(0).getLong(0)).isEqualTo(1L);
    assertVariantField(rows.get(0).get(1), "a", 1L);
    assertVariantField(rows.get(0).get(2), "x", 10L);

    assertThat(rows.get(1).getLong(0)).isEqualTo(2L);
    assertVariantField(rows.get(1).get(1), "b", 2L);
    assertVariantField(rows.get(1).get(2), "y", 20L);
  }

  @Test
  public void readNonVariantProjectionUsesColumnarPath() {
    // Projecting only non-variant columns keeps the columnar Vortex path enabled and must still
    // return correct data after the variant fallback guard was added.
    List<Row> rows = spark.table(TABLE).select("id").orderBy("id").collectAsList();
    assertThat(rows).extracting(row -> row.getLong(0)).containsExactly(1L, 2L);
  }

  @Test
  public void readNullVariant() {
    sql("INSERT INTO %s SELECT 3, NULL, NULL, NULL, NULL", TABLE);

    List<Row> rows = spark.table(TABLE).select("id", "v1").where("id = 3").collectAsList();
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get(1)).isNull();
  }

  @Test
  public void readListWithVariantProjection() {
    List<Row> rows = spark.table(TABLE).select("id", "v1", "values").orderBy("id").collectAsList();

    assertThat(rows.get(0).getList(2)).containsExactly(1L, null, 3L);
    assertThat(rows.get(1).getList(2)).isEmpty();
  }

  @Test
  public void readFieldDefault() throws Exception {
    Table table = Spark3Util.loadIcebergTable(spark, TABLE);
    table.updateSchema().addColumn("added", Types.StringType.get(), Literal.of("default")).commit();
    sql("REFRESH TABLE %s", TABLE);

    List<Row> rows = spark.table(TABLE).select("id", "added").orderBy("id").collectAsList();

    assertThat(rows).extracting(row -> row.getString(1)).containsExactly("default", "default");
  }

  @Test
  public void readNestedFieldDefault() throws Exception {
    Table table = Spark3Util.loadIcebergTable(spark, TABLE);
    table
        .updateSchema()
        .addColumn("details", "added", Types.IntegerType.get(), Literal.of(34))
        .commit();
    sql("REFRESH TABLE %s", TABLE);

    List<Row> rows = spark.table(TABLE).orderBy("id").selectExpr("details.added").collectAsList();

    assertThat(rows).extracting(row -> row.getInt(0)).containsExactly(34, 34);
  }

  private static void assertVariantField(Object value, String key, long expected) {
    assertThat(value).isInstanceOf(VariantVal.class);
    VariantVal variantVal = (VariantVal) value;
    Variant variant = new Variant(variantVal.getValue(), variantVal.getMetadata());
    Variant field = variant.getFieldByKey(key);
    assertThat(field).isNotNull();
    assertThat(field.getLong()).isEqualTo(expected);
  }
}
