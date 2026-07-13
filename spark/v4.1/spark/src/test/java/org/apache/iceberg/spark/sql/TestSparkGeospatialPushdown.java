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
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.TestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Investigation harness for pushing a spatial filter down from Spark SQL.
 *
 * <p>The pieces on the Iceberg side are in place: {@code system.st_intersects(geom, minX, minY,
 * maxX, maxY)} is a registered {@link org.apache.iceberg.spark.functions.STIntersectsFunction}, and
 * {@code SparkV2Filters} converts that UDF predicate into an Iceberg {@code ST_INTERSECTS}
 * expression that {@code InclusiveMetricsEvaluator} uses to prune files (see the core PoC and
 * TestSpatialMetricsEvaluator).
 *
 * <p>What this harness records is the remaining Spark-side gap: a bare boolean function used as a
 * filter ({@code WHERE st_intersects(...)}) is left by Spark as an {@code ApplyFunctionExpression}
 * in a post-scan {@code Filter} node and is NOT handed to the DSv2 filter pushdown, so {@code
 * SparkV2Filters.convert} is never called for it. Iceberg's {@code ReplaceStaticInvoke} optimizer
 * rule (spark-extensions) only rewrites a function call inside a comparison / IN ({@code func(col)
 * <op> literal}), not a standalone boolean function. So driving this from SQL needs either the
 * {@code st_intersects(...) = true} comparison form plus the Iceberg extensions, or an extension of
 * that rule to cover boolean predicates. This test documents the current plan shape.
 */
public class TestSparkGeospatialPushdown extends TestBase {

  private static final String CATALOG = "local";
  private static final String TABLE = CATALOG + ".default.geo_pushdown";
  // WKB points: (15,15) is the southwest cluster, (115,65) the northeast cluster.
  private static final String SW_WKB = "01010000000000000000002e400000000000002e40";
  private static final String NE_WKB = "01010000000000000000c05c400000000000405040";

  @BeforeAll
  public static void setupCatalog() {
    spark.conf().set("spark.sql.catalog." + CATALOG, SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog." + CATALOG + ".type", "hadoop");
    spark.conf().set("spark.sql.catalog." + CATALOG + ".default-namespace", "default");
    spark.conf().set("spark.sql.catalog." + CATALOG + ".cache-enabled", "false");
    spark
        .conf()
        .set(
            "spark.sql.catalog." + CATALOG + ".warehouse",
            System.getProperty("java.io.tmpdir") + "/iceberg_spark_geo_pushdown");
    spark.conf().set("spark.sql.geospatial.enabled", "true");
  }

  @BeforeEach
  public void setupTable() {
    sql("DROP TABLE IF EXISTS %s", TABLE);
    sql(
        "CREATE TABLE %s (id BIGINT, geom GEOMETRY(4326)) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        TABLE);
    sql("INSERT INTO %s VALUES (1, st_setsrid(st_geomfromwkb(X'%s'), 4326))", TABLE, SW_WKB);
    sql("INSERT INTO %s VALUES (2, st_setsrid(st_geomfromwkb(X'%s'), 4326))", TABLE, NE_WKB);
  }

  @AfterEach
  public void cleanup() {
    sql("DROP TABLE IF EXISTS %s", TABLE);
  }

  @Test
  public void testFunctionResolvesAndAppliesAsFilter() {
    // The registered function resolves and Spark builds a Filter with the spatial UDF. (It is a
    // post-scan Filter, not a pushed DSv2 predicate - see the class doc and the plan-shape test.)
    List<Object[]> plan =
        sql(
            "EXPLAIN SELECT id FROM %s WHERE %s.system.st_intersects(geom, 0, 0, 30, 30)",
            TABLE, CATALOG);
    String planText = plan.get(0)[0].toString();
    assertThat(planText)
        .as("the spatial function resolves and appears as a filter in the plan")
        .containsIgnoringCase("STIntersects")
        .doesNotContainIgnoringCase("Error occurred");
  }

  @Test
  public void testBareBooleanFunctionIsNotPushedToScan() {
    // Documents the gap: the spatial UDF stays in a post-scan Filter node rather than being pushed
    // into the Iceberg scan. When SQL-level pushdown is wired up (comparison form + extensions, or
    // a ReplaceStaticInvoke extension), this expectation should flip to a pushed scan predicate.
    List<Object[]> plan =
        sql(
            "EXPLAIN SELECT id FROM %s WHERE %s.system.st_intersects(geom, 0, 0, 30, 30)",
            TABLE, CATALOG);
    String planText = plan.get(0)[0].toString();
    assertThat(planText)
        .as("today the boolean UDF is a post-scan Filter, not a pushed scan predicate")
        .contains("Filter")
        .contains("applyfunctionexpression");
  }
}
