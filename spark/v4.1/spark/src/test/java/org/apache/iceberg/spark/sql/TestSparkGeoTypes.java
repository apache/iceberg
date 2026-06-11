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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.TestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.Geography;
import org.apache.spark.sql.types.Geometry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end Spark SQL tests for the geometry and geography types: schema, Parquet round-trip, and
 * deletion vectors.
 *
 * <p>Exercises the geometry/geography path through CREATE / INSERT (built from WKB literals via
 * Spark's stock {@code ST_GeomFromWKB} / {@code ST_GeogFromWKB}), SELECT (verifies WKB and SRID
 * round-trip through the Iceberg Parquet readers/writers) and DELETE on a v3 + merge-on-read table
 * (verifies that deletion vectors are produced for tables containing geo columns).
 *
 * <p>Topological predicates such as {@code ST_Intersects} are not part of stock Spark 4.1; the
 * predicate coverage here is intentionally limited to {@code ST_Srid(...) = ...}, which exercises
 * that the Iceberg reader re-attached the per-row SRID header from the column's CRS.
 */
public class TestSparkGeoTypes extends TestBase {

  private static final String CATALOG = "local";
  private static final String GEOMETRY_TABLE = "default.geo_geom";
  private static final String GEOGRAPHY_TABLE = "default.geo_geog";

  private static final int DEFAULT_SRID = 4326;

  // WKB byte-order octet for little-endian (NDR).
  private static final byte WKB_LE = 0x01;
  // WKB type code for a 2D Point.
  private static final int WKB_POINT = 1;

  @BeforeAll
  public static void setupCatalog() {
    spark.conf().set("spark.sql.catalog." + CATALOG, SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog." + CATALOG + ".type", "hadoop");
    spark.conf().set("spark.sql.catalog." + CATALOG + ".default-namespace", "default");
    spark.conf().set("spark.sql.catalog." + CATALOG + ".cache-enabled", "false");
    String warehouse = System.getProperty("java.io.tmpdir") + "/iceberg_spark_geo_warehouse";
    spark.conf().set("spark.sql.catalog." + CATALOG + ".warehouse", warehouse);

    // Spark 4.1 gates the GEOMETRY/GEOGRAPHY parser and built-in ST_ functions behind this flag.
    spark.conf().set("spark.sql.geospatial.enabled", "true");
  }

  @BeforeEach
  public void cleanupTables() {
    sql("DROP TABLE IF EXISTS %s", qualified(GEOMETRY_TABLE));
    sql("DROP TABLE IF EXISTS %s", qualified(GEOGRAPHY_TABLE));
  }

  @AfterEach
  public void dropTables() {
    sql("DROP TABLE IF EXISTS %s", qualified(GEOMETRY_TABLE));
    sql("DROP TABLE IF EXISTS %s", qualified(GEOGRAPHY_TABLE));
  }

  @Test
  public void testGeometryRoundTrip() {
    sql(
        "CREATE TABLE %s (id BIGINT, geom GEOMETRY(%d)) USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version'='3', "
            + "'read.parquet.vectorization.enabled'='false')",
        qualified(GEOMETRY_TABLE), DEFAULT_SRID);

    insertGeometry(GEOMETRY_TABLE, 1L, point2D(1.0, 2.0));
    insertGeometry(GEOMETRY_TABLE, 2L, point2D(3.0, 4.0));
    insertGeometry(GEOMETRY_TABLE, 3L, point2D(10.0, 20.0));

    List<Row> rows =
        spark.table(qualified(GEOMETRY_TABLE)).select("id", "geom").orderBy("id").collectAsList();
    assertThat(rows).hasSize(3);

    assertGeometryRow(rows.get(0), 1L, point2D(1.0, 2.0), DEFAULT_SRID);
    assertGeometryRow(rows.get(1), 2L, point2D(3.0, 4.0), DEFAULT_SRID);
    assertGeometryRow(rows.get(2), 3L, point2D(10.0, 20.0), DEFAULT_SRID);
  }

  @Test
  public void testGeographyRoundTrip() {
    sql(
        "CREATE TABLE %s (id BIGINT, geog GEOGRAPHY(%d)) USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version'='3', "
            + "'read.parquet.vectorization.enabled'='false')",
        qualified(GEOGRAPHY_TABLE), DEFAULT_SRID);

    insertGeography(GEOGRAPHY_TABLE, 1L, point2D(-122.4194, 37.7749)); // San Francisco
    insertGeography(GEOGRAPHY_TABLE, 2L, point2D(-73.9857, 40.7484)); // New York

    List<Row> rows =
        spark.table(qualified(GEOGRAPHY_TABLE)).select("id", "geog").orderBy("id").collectAsList();
    assertThat(rows).hasSize(2);

    assertGeographyRow(rows.get(0), 1L, point2D(-122.4194, 37.7749), DEFAULT_SRID);
    assertGeographyRow(rows.get(1), 2L, point2D(-73.9857, 40.7484), DEFAULT_SRID);
  }

  @Test
  public void testSridFilterRoundtrip() {
    sql(
        "CREATE TABLE %s (id BIGINT, geom GEOMETRY(%d)) USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version'='3', "
            + "'read.parquet.vectorization.enabled'='false')",
        qualified(GEOMETRY_TABLE), DEFAULT_SRID);

    insertGeometry(GEOMETRY_TABLE, 1L, point2D(1.0, 2.0));
    insertGeometry(GEOMETRY_TABLE, 2L, point2D(3.0, 4.0));

    // ST_Srid validates that the Iceberg reader re-attached the SRID header that we encode in
    // GeometryVal from the column's CRS. If the header were missing, ST_Srid would return 0.
    Dataset<Row> matched =
        spark
            .table(qualified(GEOMETRY_TABLE))
            .where("ST_Srid(geom) = " + DEFAULT_SRID)
            .select("id");
    assertThat(matched.collectAsList())
        .extracting(row -> row.getLong(0))
        .containsExactlyInAnyOrder(1L, 2L);

    Dataset<Row> mismatched =
        spark
            .table(qualified(GEOMETRY_TABLE))
            .where("ST_Srid(geom) <> " + DEFAULT_SRID)
            .select("id");
    assertThat(mismatched.collectAsList()).isEmpty();
  }

  @Test
  public void testDeleteWithDeletionVector() {
    sql(
        "CREATE TABLE %s (id BIGINT, geom GEOMETRY(%d)) USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version'='3', "
            + "'write.delete.mode'='merge-on-read', "
            + "'read.parquet.vectorization.enabled'='false')",
        qualified(GEOMETRY_TABLE), DEFAULT_SRID);

    // Coalesce all rows into a single data file so the DELETE has to record a DV
    // instead of dropping the file outright.
    sql(
        "INSERT INTO %s "
            + "SELECT /*+ COALESCE(1) */ * FROM ("
            + "  SELECT 1L AS id, ST_SetSrid(ST_GeomFromWKB(X'%s'), %d) AS geom UNION ALL "
            + "  SELECT 2L AS id, ST_SetSrid(ST_GeomFromWKB(X'%s'), %d) AS geom UNION ALL "
            + "  SELECT 3L AS id, ST_SetSrid(ST_GeomFromWKB(X'%s'), %d) AS geom"
            + ")",
        qualified(GEOMETRY_TABLE),
        HexFormat.of().formatHex(point2D(1.0, 2.0)),
        DEFAULT_SRID,
        HexFormat.of().formatHex(point2D(3.0, 4.0)),
        DEFAULT_SRID,
        HexFormat.of().formatHex(point2D(5.0, 6.0)),
        DEFAULT_SRID);

    long beforeDelete = spark.table(qualified(GEOMETRY_TABLE)).count();
    assertThat(beforeDelete).isEqualTo(3L);

    sql("DELETE FROM %s WHERE id = 2", qualified(GEOMETRY_TABLE));

    // A v3 merge-on-read DELETE must record a Puffin deletion vector. The summary entries below
    // are the public contract for "this DELETE produced a DV".
    Table table = icebergCatalog().loadTable(TableIdentifier.parse(GEOMETRY_TABLE));
    assertThat(table.currentSnapshot().summary())
        .containsEntry(SnapshotSummary.ADDED_DVS_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_POS_DELETES_PROP, "1");

    List<Row> survivors =
        spark.table(qualified(GEOMETRY_TABLE)).select("id", "geom").orderBy("id").collectAsList();
    assertThat(survivors).hasSize(2);
    assertGeometryRow(survivors.get(0), 1L, point2D(1.0, 2.0), DEFAULT_SRID);
    assertGeometryRow(survivors.get(1), 3L, point2D(5.0, 6.0), DEFAULT_SRID);
  }

  @Test
  public void testNullGeometryValue() {
    sql(
        "CREATE TABLE %s (id BIGINT, geom GEOMETRY(%d)) USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version'='3', "
            + "'read.parquet.vectorization.enabled'='false')",
        qualified(GEOMETRY_TABLE), DEFAULT_SRID);

    // Force everything into a single Parquet file so null/non-null values are
    // co-located in the same column chunk; this exercises definition-level encoding
    // and ParquetMetrics.counts() for the geometry column.
    sql(
        "INSERT INTO %s "
            + "SELECT /*+ COALESCE(1) */ * FROM ("
            + "  SELECT 1L AS id, ST_SetSrid(ST_GeomFromWKB(X'%s'), %d) AS geom UNION ALL "
            + "  SELECT 2L AS id, CAST(NULL AS GEOMETRY(%d)) AS geom UNION ALL "
            + "  SELECT 3L AS id, ST_SetSrid(ST_GeomFromWKB(X'%s'), %d) AS geom"
            + ")",
        qualified(GEOMETRY_TABLE),
        HexFormat.of().formatHex(point2D(1.0, 2.0)),
        DEFAULT_SRID,
        DEFAULT_SRID,
        HexFormat.of().formatHex(point2D(3.0, 4.0)),
        DEFAULT_SRID);

    List<Row> rows =
        spark.table(qualified(GEOMETRY_TABLE)).select("id", "geom").orderBy("id").collectAsList();
    assertThat(rows).hasSize(3);

    assertGeometryRow(rows.get(0), 1L, point2D(1.0, 2.0), DEFAULT_SRID);
    assertThat(rows.get(1).getLong(0)).isEqualTo(2L);
    assertThat(rows.get(1).get(1)).isNull();
    assertGeometryRow(rows.get(2), 3L, point2D(3.0, 4.0), DEFAULT_SRID);
  }

  @Test
  public void testMultipleGeoColumnsInOneTable() {
    // A single table that mixes geometry and geography columns. This validates that the
    // per-column CRS metadata stored on the Parquet logical type annotations is honored
    // independently for each column when reading.
    sql(
        "CREATE TABLE %s ("
            + "  id BIGINT,"
            + "  geom GEOMETRY(%d),"
            + "  geog GEOGRAPHY(%d)"
            + ") USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version'='3', "
            + "'read.parquet.vectorization.enabled'='false')",
        qualified(GEOMETRY_TABLE), DEFAULT_SRID, DEFAULT_SRID);

    byte[] geomWkb = point2D(1.0, 2.0);
    byte[] geogWkb = point2D(-122.4194, 37.7749);
    sql(
        "INSERT INTO %s SELECT 1L, "
            + "ST_SetSrid(ST_GeomFromWKB(X'%s'), %d), "
            + "ST_SetSrid(ST_GeogFromWKB(X'%s'), %d)",
        qualified(GEOMETRY_TABLE),
        HexFormat.of().formatHex(geomWkb),
        DEFAULT_SRID,
        HexFormat.of().formatHex(geogWkb),
        DEFAULT_SRID);

    Row row = spark.table(qualified(GEOMETRY_TABLE)).collectAsList().get(0);
    assertThat(row.getLong(0)).isEqualTo(1L);

    Object geomCell = row.get(1);
    assertThat(geomCell).isInstanceOf(Geometry.class);
    assertThat(((Geometry) geomCell).getBytes()).containsExactly(geomWkb);
    assertThat(((Geometry) geomCell).getSrid()).isEqualTo(DEFAULT_SRID);

    Object geogCell = row.get(2);
    assertThat(geogCell).isInstanceOf(Geography.class);
    assertThat(((Geography) geogCell).getBytes()).containsExactly(geogWkb);
    assertThat(((Geography) geogCell).getSrid()).isEqualTo(DEFAULT_SRID);
  }

  @Test
  public void testStructWithGeometry() {
    // Geometry as a leaf inside a STRUCT. Exercises the StructReader/StructWriter
    // delegation path, where the inner element is our GeometryReader/Writer.
    sql(
        "CREATE TABLE %s ("
            + "  id BIGINT,"
            + "  event STRUCT<eid: BIGINT, loc: GEOMETRY(%d)>"
            + ") USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version'='3', "
            + "'read.parquet.vectorization.enabled'='false')",
        qualified(GEOMETRY_TABLE), DEFAULT_SRID);

    byte[] wkb = point2D(7.0, 8.0);
    sql(
        "INSERT INTO %s SELECT 1L, "
            + "named_struct('eid', 42L, 'loc', ST_SetSrid(ST_GeomFromWKB(X'%s'), %d))",
        qualified(GEOMETRY_TABLE), HexFormat.of().formatHex(wkb), DEFAULT_SRID);

    Row row = spark.table(qualified(GEOMETRY_TABLE)).collectAsList().get(0);
    assertThat(row.getLong(0)).isEqualTo(1L);

    Row event = row.getStruct(1);
    assertThat(event.getLong(0)).isEqualTo(42L);

    Object loc = event.get(1);
    assertThat(loc).isInstanceOf(Geometry.class);
    assertThat(((Geometry) loc).getBytes()).containsExactly(wkb);
    assertThat(((Geometry) loc).getSrid()).isEqualTo(DEFAULT_SRID);
  }

  @Test
  public void testArrayOfGeometry() {
    // ARRAY<GEOMETRY> exercises the RepeatedReader / ArrayDataWriter path that
    // delegates per element to our GeometryReader/Writer. Spark surfaces the column
    // as a java.util.List of Geometry instances at the Row API.
    sql(
        "CREATE TABLE %s ("
            + "  id BIGINT,"
            + "  points ARRAY<GEOMETRY(%d)>"
            + ") USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version'='3', "
            + "'read.parquet.vectorization.enabled'='false')",
        qualified(GEOMETRY_TABLE), DEFAULT_SRID);

    byte[] wkb1 = point2D(1.0, 2.0);
    byte[] wkb2 = point2D(3.0, 4.0);
    sql(
        "INSERT INTO %s SELECT 1L, array("
            + "ST_SetSrid(ST_GeomFromWKB(X'%s'), %d), "
            + "ST_SetSrid(ST_GeomFromWKB(X'%s'), %d))",
        qualified(GEOMETRY_TABLE),
        HexFormat.of().formatHex(wkb1),
        DEFAULT_SRID,
        HexFormat.of().formatHex(wkb2),
        DEFAULT_SRID);

    Row row = spark.table(qualified(GEOMETRY_TABLE)).collectAsList().get(0);
    assertThat(row.getLong(0)).isEqualTo(1L);

    List<Object> elements = row.getList(1);
    assertThat(elements).hasSize(2);
    assertThat(elements.get(0)).isInstanceOf(Geometry.class);
    assertThat(((Geometry) elements.get(0)).getBytes()).containsExactly(wkb1);
    assertThat(((Geometry) elements.get(0)).getSrid()).isEqualTo(DEFAULT_SRID);
    assertThat(elements.get(1)).isInstanceOf(Geometry.class);
    assertThat(((Geometry) elements.get(1)).getBytes()).containsExactly(wkb2);
    assertThat(((Geometry) elements.get(1)).getSrid()).isEqualTo(DEFAULT_SRID);
  }

  @Test
  public void testMapOfGeometry() {
    // MAP<STRING, GEOMETRY> exercises the RepeatedKeyValueReader / MapDataWriter path.
    sql(
        "CREATE TABLE %s ("
            + "  id BIGINT,"
            + "  places MAP<STRING, GEOMETRY(%d)>"
            + ") USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version'='3', "
            + "'read.parquet.vectorization.enabled'='false')",
        qualified(GEOMETRY_TABLE), DEFAULT_SRID);

    byte[] homeWkb = point2D(1.0, 2.0);
    byte[] workWkb = point2D(5.0, 6.0);
    sql(
        "INSERT INTO %s SELECT 1L, map("
            + "'home', ST_SetSrid(ST_GeomFromWKB(X'%s'), %d), "
            + "'work', ST_SetSrid(ST_GeomFromWKB(X'%s'), %d))",
        qualified(GEOMETRY_TABLE),
        HexFormat.of().formatHex(homeWkb),
        DEFAULT_SRID,
        HexFormat.of().formatHex(workWkb),
        DEFAULT_SRID);

    Row row = spark.table(qualified(GEOMETRY_TABLE)).collectAsList().get(0);
    assertThat(row.getLong(0)).isEqualTo(1L);

    Map<Object, Object> places = row.getJavaMap(1);
    assertThat(places).hasSize(2);

    Object home = places.get("home");
    assertThat(home).isInstanceOf(Geometry.class);
    assertThat(((Geometry) home).getBytes()).containsExactly(homeWkb);
    assertThat(((Geometry) home).getSrid()).isEqualTo(DEFAULT_SRID);

    Object work = places.get("work");
    assertThat(work).isInstanceOf(Geometry.class);
    assertThat(((Geometry) work).getBytes()).containsExactly(workWkb);
    assertThat(((Geometry) work).getSrid()).isEqualTo(DEFAULT_SRID);
  }

  @Test
  public void testStructOfArrayOfGeometry() {
    // Two levels of nesting: STRUCT<id, points ARRAY<GEOMETRY>>. Validates that the
    // visitor framework resolves the geometry leaf correctly under nested wrappers.
    sql(
        "CREATE TABLE %s ("
            + "  id BIGINT,"
            + "  trip STRUCT<tid: BIGINT, points: ARRAY<GEOMETRY(%d)>>"
            + ") USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version'='3', "
            + "'read.parquet.vectorization.enabled'='false')",
        qualified(GEOMETRY_TABLE), DEFAULT_SRID);

    byte[] startWkb = point2D(0.0, 0.0);
    byte[] midWkb = point2D(1.0, 1.0);
    byte[] endWkb = point2D(2.0, 2.0);
    sql(
        "INSERT INTO %s SELECT 1L, named_struct("
            + "'tid', 99L, "
            + "'points', array("
            + "ST_SetSrid(ST_GeomFromWKB(X'%s'), %d), "
            + "ST_SetSrid(ST_GeomFromWKB(X'%s'), %d), "
            + "ST_SetSrid(ST_GeomFromWKB(X'%s'), %d)))",
        qualified(GEOMETRY_TABLE),
        HexFormat.of().formatHex(startWkb),
        DEFAULT_SRID,
        HexFormat.of().formatHex(midWkb),
        DEFAULT_SRID,
        HexFormat.of().formatHex(endWkb),
        DEFAULT_SRID);

    Row row = spark.table(qualified(GEOMETRY_TABLE)).collectAsList().get(0);
    assertThat(row.getLong(0)).isEqualTo(1L);

    Row trip = row.getStruct(1);
    assertThat(trip.getLong(0)).isEqualTo(99L);

    List<Object> points = trip.getList(1);
    assertThat(points).hasSize(3);
    assertThat(((Geometry) points.get(0)).getBytes()).containsExactly(startWkb);
    assertThat(((Geometry) points.get(0)).getSrid()).isEqualTo(DEFAULT_SRID);
    assertThat(((Geometry) points.get(1)).getBytes()).containsExactly(midWkb);
    assertThat(((Geometry) points.get(2)).getBytes()).containsExactly(endWkb);
  }

  @Test
  public void testDeleteWithDeletionVectorOnNestedGeometry() {
    // Verify that wrapping the geometry inside a STRUCT does not break the row-level
    // DELETE / DV path: writes still succeed, the file is reused, and a Puffin DV is
    // produced instead of a copy-on-write rewrite.
    sql(
        "CREATE TABLE %s ("
            + "  id BIGINT,"
            + "  event STRUCT<eid: BIGINT, loc: GEOMETRY(%d)>"
            + ") USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version'='3', "
            + "'write.delete.mode'='merge-on-read', "
            + "'read.parquet.vectorization.enabled'='false')",
        qualified(GEOMETRY_TABLE), DEFAULT_SRID);

    sql(
        "INSERT INTO %s "
            + "SELECT /*+ COALESCE(1) */ * FROM ("
            + "  SELECT 1L AS id, named_struct("
            + "    'eid', 10L,"
            + "    'loc', ST_SetSrid(ST_GeomFromWKB(X'%s'), %d)) AS event UNION ALL "
            + "  SELECT 2L AS id, named_struct("
            + "    'eid', 20L,"
            + "    'loc', ST_SetSrid(ST_GeomFromWKB(X'%s'), %d)) AS event UNION ALL "
            + "  SELECT 3L AS id, named_struct("
            + "    'eid', 30L,"
            + "    'loc', ST_SetSrid(ST_GeomFromWKB(X'%s'), %d)) AS event"
            + ")",
        qualified(GEOMETRY_TABLE),
        HexFormat.of().formatHex(point2D(1.0, 2.0)),
        DEFAULT_SRID,
        HexFormat.of().formatHex(point2D(3.0, 4.0)),
        DEFAULT_SRID,
        HexFormat.of().formatHex(point2D(5.0, 6.0)),
        DEFAULT_SRID);

    sql("DELETE FROM %s WHERE id = 2", qualified(GEOMETRY_TABLE));

    Table table = icebergCatalog().loadTable(TableIdentifier.parse(GEOMETRY_TABLE));
    assertThat(table.currentSnapshot().summary())
        .containsEntry(SnapshotSummary.ADDED_DVS_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_POS_DELETES_PROP, "1");

    List<Row> survivors = spark.table(qualified(GEOMETRY_TABLE)).orderBy("id").collectAsList();
    assertThat(survivors).hasSize(2);
    assertThat(survivors.get(0).getLong(0)).isEqualTo(1L);
    assertThat(((Geometry) survivors.get(0).getStruct(1).get(1)).getBytes())
        .containsExactly(point2D(1.0, 2.0));
    assertThat(survivors.get(1).getLong(0)).isEqualTo(3L);
    assertThat(((Geometry) survivors.get(1).getStruct(1).get(1)).getBytes())
        .containsExactly(point2D(5.0, 6.0));
  }

  // ---- helpers ----

  private static String qualified(String name) {
    return CATALOG + "." + name;
  }

  private void insertGeometry(String name, long id, byte[] wkb) {
    sql(
        "INSERT INTO %s SELECT %d AS id, ST_SetSrid(ST_GeomFromWKB(X'%s'), %d)",
        qualified(name), id, HexFormat.of().formatHex(wkb), DEFAULT_SRID);
  }

  private void insertGeography(String name, long id, byte[] wkb) {
    sql(
        "INSERT INTO %s SELECT %d AS id, ST_SetSrid(ST_GeogFromWKB(X'%s'), %d)",
        qualified(name), id, HexFormat.of().formatHex(wkb), DEFAULT_SRID);
  }

  private static void assertGeometryRow(Row row, long expectedId, byte[] expectedWkb, int srid) {
    assertThat(row.getLong(0)).isEqualTo(expectedId);
    Object cell = row.get(1);
    assertThat(cell).isInstanceOf(Geometry.class);
    Geometry value = (Geometry) cell;
    assertThat(value.getBytes()).containsExactly(expectedWkb);
    assertThat(value.getSrid()).isEqualTo(srid);
  }

  private static void assertGeographyRow(Row row, long expectedId, byte[] expectedWkb, int srid) {
    assertThat(row.getLong(0)).isEqualTo(expectedId);
    Object cell = row.get(1);
    assertThat(cell).isInstanceOf(Geography.class);
    Geography value = (Geography) cell;
    assertThat(value.getBytes()).containsExactly(expectedWkb);
    assertThat(value.getSrid()).isEqualTo(srid);
  }

  private static byte[] point2D(double x, double y) {
    ByteBuffer buffer =
        ByteBuffer.allocate(1 + Integer.BYTES + 2 * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    buffer.put(WKB_LE);
    buffer.putInt(WKB_POINT);
    buffer.putDouble(x);
    buffer.putDouble(y);
    return buffer.array();
  }

  private static Catalog icebergCatalog() {
    return ((SparkCatalog) spark.sessionState().catalogManager().catalog(CATALOG)).icebergCatalog();
  }
}
