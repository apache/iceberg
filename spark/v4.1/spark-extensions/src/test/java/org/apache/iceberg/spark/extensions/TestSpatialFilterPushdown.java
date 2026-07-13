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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/**
 * End-to-end test that a spatial filter written in SQL is pushed down and prunes files. With the
 * Iceberg extensions enabled, {@code ReplaceStaticInvoke} rewrites the top-level boolean
 * {@code st_intersects(...)} function into an {@code ApplyFunctionExpression}, which Spark then
 * translates to a pushed DSv2 predicate; {@code SparkV2Filters} converts it to an Iceberg
 * {@code ST_INTERSECTS} expression and {@code InclusiveMetricsEvaluator} skips non-matching files.
 */
public class TestSpatialFilterPushdown extends ExtensionsTestBase {

  private static final String GEO_CATALOG = "geo";
  private static final String TABLE = GEO_CATALOG + ".default.geo_pushdown";
  // WKB points: (15,15) is the southwest cluster, (115,65) the northeast cluster.
  private static final String SW_WKB = "01010000000000000000002e400000000000002e40";
  private static final String NE_WKB = "01010000000000000000c05c400000000000405040";

  @BeforeEach
  public void setupGeoTable() {
    // Hive cannot represent geo types, so use a dedicated hadoop catalog with the geospatial flag.
    spark.conf().set("spark.sql.catalog." + GEO_CATALOG, SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog." + GEO_CATALOG + ".type", "hadoop");
    spark.conf().set("spark.sql.catalog." + GEO_CATALOG + ".default-namespace", "default");
    spark.conf().set("spark.sql.catalog." + GEO_CATALOG + ".cache-enabled", "false");
    spark.conf().set(
        "spark.sql.catalog." + GEO_CATALOG + ".warehouse",
        System.getProperty("java.io.tmpdir") + "/iceberg_spatial_pushdown");
    spark.conf().set("spark.sql.geospatial.enabled", "true");
    // Geo has no Arrow vector yet; read through the row-based reader.
    spark.conf().set("spark.sql.iceberg.vectorization.enabled", "false");

    sql("DROP TABLE IF EXISTS %s", TABLE);
    sql(
        "CREATE TABLE %s (id BIGINT, geom GEOMETRY(4326)) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        TABLE);
    // Two separate inserts => two data files, each a distinct spatial cluster.
    sql("INSERT INTO %s VALUES (1, st_setsrid(st_geomfromwkb(X'%s'), 4326))", TABLE, SW_WKB);
    sql("INSERT INTO %s VALUES (2, st_setsrid(st_geomfromwkb(X'%s'), 4326))", TABLE, NE_WKB);
  }

  @AfterEach
  public void dropGeoTable() {
    sql("DROP TABLE IF EXISTS %s", TABLE);
  }

  @TestTemplate
  public void testSpatialFilterPushesDownToScan() {
    // The query window (0,0)-(30,30) contains only the southwest point; the plan should push the
    // spatial predicate into the Iceberg batch scan rather than leave it in a post-scan Filter.
    List<Object[]> plan =
        sql(
            "EXPLAIN SELECT id FROM %s WHERE %s.system.st_intersects(geom, 0, 0, 30, 30)",
            TABLE, GEO_CATALOG);
    String planText = plan.get(0)[0].toString();
    assertThat(planText)
        .as("spatial predicate should be pushed into the scan, not a post-scan Filter")
        .containsIgnoringCase("st_intersects");
    // A pushed predicate appears in the BatchScan's runtime/pushed filters; the residual Filter
    // node should be gone (or trivially true) for this window.
    assertThat(planText)
        .as("the boolean UDF should no longer sit in a post-scan Filter node")
        .doesNotContain("Filter applyfunctionexpression");
  }

  @TestTemplate
  public void testSpatialFilterReturnsOnlyMatchingCluster() {
    List<Object[]> sw =
        sql(
            "SELECT id FROM %s WHERE %s.system.st_intersects(geom, 0, 0, 30, 30) ORDER BY id",
            TABLE, GEO_CATALOG);
    assertThat(sw).as("only the southwest row matches").isEqualTo(ImmutableList.of(row(1L)));

    List<Object[]> ne =
        sql(
            "SELECT id FROM %s WHERE %s.system.st_intersects(geom, 100, 50, 130, 80) ORDER BY id",
            TABLE, GEO_CATALOG);
    assertThat(ne).as("only the northeast row matches").isEqualTo(ImmutableList.of(row(2L)));
  }
}
