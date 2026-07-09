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

import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.TestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestSparkGeospatial extends TestBase {

  private static final String CATALOG = "local";
  private static final String TABLE = CATALOG + ".default.geo";

  // WKB (little-endian) for POINT(30 10), POINT(-71 42) and POINT(100 50).
  private static final String GEOM_WKB = "01010000000000000000003e400000000000002440";
  private static final String GEOG_WKB = "01010000000000000000c051c00000000000004540";
  private static final String OTHER_WKB = "010100000000000000000059400000000000004940";

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
    // Merge-on-read at format version 3 so that DELETE/UPDATE/MERGE produce deletion vectors.
    sql(
        "CREATE TABLE %s (id BIGINT, geom GEOMETRY(4326), geog GEOGRAPHY(4326)) USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version'='3', "
            + "'write.delete.mode'='merge-on-read', "
            + "'write.update.mode'='merge-on-read', "
            + "'write.merge.mode'='merge-on-read')",
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
    // Even with vectorization enabled, geo columns fall back to the non-vectorized reader (there is
    // no Arrow geo vector yet), so both settings read back correctly.
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

  @Test
  public void testDeleteGeospatialMergeOnRead() {
    // Use a dedicated table with a single INSERT and a single DELETE so there is exactly one delete
    // snapshot to inspect. Write both rows into one data file (COALESCE(1)) so deleting one row
    // leaves a survivor in that file, forcing a deletion vector rather than a whole-file removal.
    // Filter on id since Spark has no spatial predicate.
    String deleteTable = CATALOG + ".default.geo_delete";
    sql("DROP TABLE IF EXISTS %s", deleteTable);
    sql(
        "CREATE TABLE %s (id BIGINT, geom GEOMETRY(4326), geog GEOGRAPHY(4326)) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='merge-on-read')",
        deleteTable);
    sql(
        "INSERT INTO %s SELECT /*+ COALESCE(1) */ id, "
            + "CASE WHEN geom_wkb IS NULL THEN NULL "
            + "ELSE st_setsrid(st_geomfromwkb(geom_wkb), 4326) END, "
            + "CASE WHEN geog_wkb IS NULL THEN NULL "
            + "ELSE st_setsrid(st_geogfromwkb(geog_wkb), 4326) END "
            + "FROM VALUES (1, X'%s', X'%s'), (2, CAST(NULL AS BINARY), CAST(NULL AS BINARY)) "
            + "AS v(id, geom_wkb, geog_wkb)",
        deleteTable, GEOM_WKB, GEOG_WKB);

    sql("DELETE FROM %s WHERE id = 1", deleteTable);

    assertEquals(
        "Only the null-geo row should remain after the merge-on-read delete",
        ImmutableList.of(row(2L, null, null)),
        sql(
            "SELECT id, hex(st_asbinary(geom)), hex(st_asbinary(geog)) FROM %s ORDER BY id",
            deleteTable));

    // Deleting a row from a multi-row file writes a deletion vector (format v3, merge-on-read).
    // Select the (single) delete snapshot by operation so the assertion does not depend on
    // committed_at ordering, whose millisecond granularity could tie with the insert snapshot.
    assertThat(
            scalarSql(
                "SELECT summary['added-dvs'] FROM %s.snapshots WHERE operation = 'delete'",
                deleteTable))
        .as("Merge-on-read delete should add a deletion vector")
        .isEqualTo("1");

    sql("DROP TABLE IF EXISTS %s", deleteTable);
  }

  @Test
  public void testUpdateGeospatialMergeOnRead() {
    // Update the geo values of the first row; others are unchanged.
    sql(
        "UPDATE %s SET geom = st_setsrid(st_geomfromwkb(X'%s'), 4326) WHERE id = 1",
        TABLE, OTHER_WKB);

    assertEquals(
        "Only the updated row's geometry should change",
        ImmutableList.of(
            row(1L, OTHER_WKB.toUpperCase(Locale.ROOT), GEOG_WKB.toUpperCase(Locale.ROOT)),
            row(2L, null, null)),
        sql(
            "SELECT id, hex(st_asbinary(geom)), hex(st_asbinary(geog)) FROM %s ORDER BY id",
            TABLE));
  }

  @Test
  public void testMergeGeospatialMergeOnRead() {
    // Source updates id=1 (matched) and inserts id=3 (not matched); geo values flow through both.
    sql(
        "MERGE INTO %s AS t USING ("
            + "SELECT 1 AS id, st_setsrid(st_geomfromwkb(X'%s'), 4326) AS geom, "
            + "st_setsrid(st_geogfromwkb(X'%s'), 4326) AS geog "
            + "UNION ALL "
            + "SELECT 3 AS id, st_setsrid(st_geomfromwkb(X'%s'), 4326) AS geom, "
            + "st_setsrid(st_geogfromwkb(X'%s'), 4326) AS geog) AS s "
            + "ON t.id = s.id "
            + "WHEN MATCHED THEN UPDATE SET t.geom = s.geom, t.geog = s.geog "
            + "WHEN NOT MATCHED THEN INSERT *",
        TABLE, OTHER_WKB, GEOG_WKB, GEOM_WKB, GEOG_WKB);

    assertEquals(
        "Matched row is updated and unmatched row is inserted",
        ImmutableList.of(
            row(1L, OTHER_WKB.toUpperCase(Locale.ROOT), GEOG_WKB.toUpperCase(Locale.ROOT)),
            row(2L, null, null),
            row(3L, GEOM_WKB.toUpperCase(Locale.ROOT), GEOG_WKB.toUpperCase(Locale.ROOT))),
        sql(
            "SELECT id, hex(st_asbinary(geom)), hex(st_asbinary(geog)) FROM %s ORDER BY id",
            TABLE));
  }

  @Test
  public void testDeleteNestedGeometryMergeOnRead() {
    String nestedTable = CATALOG + ".default.geo_nested";
    sql("DROP TABLE IF EXISTS %s", nestedTable);
    sql(
        "CREATE TABLE %s (id BIGINT, complex STRUCT<c1: INT, geom: GEOMETRY(4326)>) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='merge-on-read')",
        nestedTable);
    sql(
        "INSERT INTO %s VALUES "
            + "(1, named_struct('c1', 10, 'geom', st_setsrid(st_geomfromwkb(X'%s'), 4326))), "
            + "(2, named_struct('c1', 20, 'geom', CAST(NULL AS GEOMETRY(4326))))",
        nestedTable, GEOM_WKB);

    // Delete by a nested non-geo field; the surviving nested geometry must still round-trip.
    sql("DELETE FROM %s WHERE complex.c1 = 10", nestedTable);

    assertEquals(
        "Only the row with the null nested geometry should remain",
        ImmutableList.of(row(2L, 20, null)),
        sql(
            "SELECT id, complex.c1, hex(st_asbinary(complex.geom)) FROM %s ORDER BY id",
            nestedTable));

    sql("DROP TABLE IF EXISTS %s", nestedTable);
  }

  @Test
  public void testDeleteGeospatialCopyOnWrite() {
    // Copy-on-write is the default row-level mode: the delete rewrites the surviving rows into a
    // new data file, which exercises reading geo values back out during the rewrite.
    String cowTable = CATALOG + ".default.geo_cow";
    sql("DROP TABLE IF EXISTS %s", cowTable);
    sql(
        "CREATE TABLE %s (id BIGINT, geom GEOMETRY(4326), geog GEOGRAPHY(4326)) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        cowTable);
    sql(
        "INSERT INTO %s VALUES "
            + "(1, st_setsrid(st_geomfromwkb(X'%s'), 4326), st_setsrid(st_geogfromwkb(X'%s'), 4326)), "
            + "(2, NULL, NULL)",
        cowTable, GEOM_WKB, GEOG_WKB);

    sql("DELETE FROM %s WHERE id = 1", cowTable);

    assertEquals(
        "The surviving null-geo row is rewritten intact after the copy-on-write delete",
        ImmutableList.of(row(2L, null, null)),
        sql(
            "SELECT id, hex(st_asbinary(geom)), hex(st_asbinary(geog)) FROM %s ORDER BY id",
            cowTable));

    sql("DROP TABLE IF EXISTS %s", cowTable);
  }

  @Test
  public void testUpdateGeospatialCopyOnWrite() {
    // Copy-on-write update: the row with unchanged geo values is read back and rewritten alongside
    // the updated row.
    String cowTable = CATALOG + ".default.geo_cow";
    sql("DROP TABLE IF EXISTS %s", cowTable);
    sql(
        "CREATE TABLE %s (id BIGINT, geom GEOMETRY(4326), geog GEOGRAPHY(4326)) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        cowTable);
    sql(
        "INSERT INTO %s VALUES "
            + "(1, st_setsrid(st_geomfromwkb(X'%s'), 4326), st_setsrid(st_geogfromwkb(X'%s'), 4326)), "
            + "(2, st_setsrid(st_geomfromwkb(X'%s'), 4326), NULL)",
        cowTable, GEOM_WKB, GEOG_WKB, GEOM_WKB);

    sql(
        "UPDATE %s SET geom = st_setsrid(st_geomfromwkb(X'%s'), 4326) WHERE id = 1",
        cowTable, OTHER_WKB);

    assertEquals(
        "The updated row changes and the untouched row is rewritten intact",
        ImmutableList.of(
            row(1L, OTHER_WKB.toUpperCase(Locale.ROOT), GEOG_WKB.toUpperCase(Locale.ROOT)),
            row(2L, GEOM_WKB.toUpperCase(Locale.ROOT), null)),
        sql(
            "SELECT id, hex(st_asbinary(geom)), hex(st_asbinary(geog)) FROM %s ORDER BY id",
            cowTable));

    sql("DROP TABLE IF EXISTS %s", cowTable);
  }

  private void setVectorization(boolean on) {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('read.parquet.vectorization.enabled'='%s')",
        TABLE, Boolean.toString(on));
  }
}
