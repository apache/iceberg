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

import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.TestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestSparkGeospatial extends TestBase {

  private static final String CATALOG = "local";
  private static final String TABLE = CATALOG + ".default.geo";

  // WKB (little-endian) for POINT(30 10) and POINT(-71 42).
  private static final String GEOM_WKB = "01010000000000000000003e400000000000002440";
  private static final String GEOG_WKB = "01010000000000000000c051c00000000000004540";

  @BeforeAll
  public static void setupCatalog() {
    // Use a Hadoop catalog to avoid Hive schema conversion (Hive doesn't support geo yet), and
    // enable the geospatial feature, which Spark keeps behind a flag.
    spark.conf().set("spark.sql.catalog." + CATALOG, SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog." + CATALOG + ".type", "hadoop");
    spark.conf().set("spark.sql.catalog." + CATALOG + ".default-namespace", "default");
    spark.conf().set("spark.sql.catalog." + CATALOG + ".cache-enabled", "false");
    String warehouse = System.getProperty("java.io.tmpdir") + "/iceberg_spark_geo_warehouse";
    spark.conf().set("spark.sql.catalog." + CATALOG + ".warehouse", warehouse);
    spark.conf().set("spark.sql.geospatial.enabled", "true");
  }

  @BeforeEach
  public void setupTable() {
    sql("DROP TABLE IF EXISTS %s", TABLE);
    // A bare GEOMETRY column is mixed-SRID, which Iceberg does not support, so the column SRID is
    // pinned to 4326 (OGC:CRS84).
    sql(
        "CREATE TABLE %s (id BIGINT, geom GEOMETRY(4326), geog GEOGRAPHY(4326)) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        TABLE);

    // st_geomfromwkb yields SRID 0, so set it to the column's SRID for the write to be accepted.
    sql(
        "INSERT INTO %s VALUES "
            + "(1, st_setsrid(st_geomfromwkb(X'%s'), 4326), st_setsrid(st_geogfromwkb(X'%s'), 4326)), "
            + "(2, NULL, NULL)",
        TABLE, GEOM_WKB, GEOG_WKB);
  }

  @AfterEach
  public void cleanup() {
    sql("DROP TABLE IF EXISTS %s", TABLE);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testGeospatialWkbReadBack(boolean vectorized) {
    assumeThat(vectorized).as("Geo vectorized Parquet read is not implemented yet").isFalse();
    setVectorization(vectorized);

    // st_asbinary strips the SRID header back to pure WKB, matching what was inserted.
    assertEquals(
        "Geometry and geography WKB should round-trip through a Spark scan",
        ImmutableList.of(
            row(1L, GEOM_WKB.toUpperCase(Locale.ROOT), GEOG_WKB.toUpperCase(Locale.ROOT)),
            row(2L, null, null)),
        sql(
            "SELECT id, hex(st_asbinary(geom)), hex(st_asbinary(geog)) FROM %s ORDER BY id",
            TABLE));
  }

  private void setVectorization(boolean on) {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('read.parquet.vectorization.enabled'='%s')",
        TABLE, Boolean.toString(on));
  }
}
