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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestAggregatePushDown extends SparkCatalogTestBase {

  public TestAggregatePushDown(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @BeforeClass
  public static void startMetastoreAndSpark() {
    SparkTestBase.metastore = new TestHiveMetastore();
    metastore.start();
    SparkTestBase.hiveConf = metastore.hiveConf();

    SparkTestBase.spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.sql.iceberg.aggregate_pushdown", "true")
            .enableHiveSupport()
            .getOrCreate();

    SparkTestBase.catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);

    try {
      catalog.createNamespace(Namespace.of("default"));
    } catch (AlreadyExistsException ignored) {
      // the default namespace already exists. ignore the create error
    }
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testDifferentDataTypesAggregatePushDownInPartitionedTable() {
    testDifferentDataTypesAggregatePushDown(true);
  }

  @Test
  public void testDifferentDataTypesAggregatePushDownInNonPartitionedTable() {
    testDifferentDataTypesAggregatePushDown(false);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private void testDifferentDataTypesAggregatePushDown(boolean hasPartitionCol) {
    String createTable;
    if (hasPartitionCol) {
      createTable =
          "CREATE TABLE %s (id LONG, intData INT, booleanData BOOLEAN, floatData FLOAT, doubleData DOUBLE, "
              + "decimalData DECIMAL(14, 2), binaryData binary) USING iceberg PARTITIONED BY (id)";
    } else {
      createTable =
          "CREATE TABLE %s (id LONG, intData INT, booleanData BOOLEAN, floatData FLOAT, doubleData DOUBLE, "
              + "decimalData DECIMAL(14, 2), binaryData binary) USING iceberg";
    }
    sql(createTable, tableName);
    sql(
        "INSERT INTO TABLE %s VALUES "
            + "(1, null, false, null, null, 11.11, X'1111'),"
            + " (1, null, true, 2.222, 2.222222, 22.22, X'2222'),"
            + " (2, 33, false, 3.333, 3.333333, 33.33, X'3333'),"
            + " (2, 44, true, null, 4.444444, 44.44, X'4444'),"
            + " (3, 55, false, 5.555, 5.555555, 55.55, X'5555'),"
            + " (3, null, true, null, 6.666666, 66.66, null) ",
        tableName);

    String select =
        "SELECT count(*), max(id), min(id), count(id), "
            + "max(intData), min(intData), count(intData), "
            + "max(booleanData), min(booleanData), count(booleanData), "
            + "max(floatData), min(floatData), count(floatData), "
            + "max(doubleData), min(doubleData), count(doubleData), "
            + "max(decimalData), min(decimalData), count(decimalData), "
            + "max(binaryData), min(binaryData), count(binaryData) FROM %s";

    List<Object[]> explain = sql("EXPLAIN " + select, tableName);
    String explainString = explain.get(0)[0].toString();
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("count(*)")
        && explainString.contains("max(id)")
        && explainString.contains("min(id)")
        && explainString.contains("count(id)")
        && explainString.contains("max(intData)")
        && explainString.contains("min(intData)")
        && explainString.contains("count(intData)")
        && explainString.contains("max(booleanData)")
        && explainString.contains("min(booleanData)")
        && explainString.contains("count(booleanData)")
        && explainString.contains("max(floatData)")
        && explainString.contains("min(floatData)")
        && explainString.contains("count(floatData)")
        && explainString.contains("max(doubleData)")
        && explainString.contains("min(doubleData)")
        && explainString.contains("count(doubleData)")
        && explainString.contains("max(decimalData)")
        && explainString.contains("min(decimalData)")
        && explainString.contains("count(decimalData)")
        && explainString.contains("max(binaryData)")
        && explainString.contains("min(binaryData)")
        && explainString.contains("count(binaryData)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertTrue(
        "explain should contain the pushed down aggregates", explainContainsPushDownAggregates);

    List<Object[]> actual = sql(select, tableName);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(
        new Object[] {
          6L,
          3L,
          1L,
          6L,
          55,
          33,
          3L,
          true,
          false,
          6L,
          5.555f,
          2.222f,
          3L,
          6.666666,
          2.222222,
          5L,
          new BigDecimal("66.66"),
          new BigDecimal("11.11"),
          6L,
          new byte[] {85, 85},
          new byte[] {17, 17},
          5L
        });
    assertEquals("min/max/count push down", expected, actual);
  }

  @Test
  public void testDateAndTimestampWithPartition() {
    sql(
        "CREATE TABLE %s (id bigint, data string, d date, ts timestamp) USING iceberg PARTITIONED BY (id)",
        tableName);
    sql(
        "INSERT INTO %s VALUES (1, '1', date('2021-11-10'), null),"
            + "(1, '2', date('2021-11-11'), timestamp('2021-11-11 22:22:22')), "
            + "(2, '3', date('2021-11-12'), timestamp('2021-11-12 22:22:22')), "
            + "(2, '4', date('2021-11-13'), timestamp('2021-11-13 22:22:22')), "
            + "(3, '5', null, timestamp('2021-11-14 22:22:22')), "
            + "(3, '6', date('2021-11-14'), null)",
        tableName);
    String select = "SELECT max(d), min(d), count(d), max(ts), min(ts), count(ts) FROM %s";

    List<Object[]> explain = sql("EXPLAIN " + select, tableName);
    String explainString = explain.get(0)[0].toString();
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("max(d)")
        && explainString.contains("min(d)")
        && explainString.contains("count(d)")
        && explainString.contains("max(ts)")
        && explainString.contains("min(ts)")
        && explainString.contains("count(ts)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertTrue(
        "explain should contain the pushed down aggregates", explainContainsPushDownAggregates);

    List<Object[]> actual = sql(select, tableName);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(
        new Object[] {
          Date.valueOf("2021-11-14"),
          Date.valueOf("2021-11-10"),
          5L,
          Timestamp.valueOf("2021-11-14 22:22:22.0"),
          Timestamp.valueOf("2021-11-11 22:22:22.0"),
          4L
        });
    assertEquals("min/max/count push down", expected, actual);
  }

  @Test
  public void testAggregateNotPushDownIfOneCantPushDown() {
    sql("CREATE TABLE %s (id LONG, data DOUBLE) USING iceberg", tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, 1111), (1, 2222), (2, 3333), (2, 4444), (3, 5555), (3, 6666) ",
        tableName);
    String select = "SELECT COUNT(data), SUM(data) FROM %s";

    List<Object[]> explain = sql("EXPLAIN " + select, tableName);
    String explainString = explain.get(0)[0].toString();
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("COUNT(data)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertFalse(
        "explain should not contain the pushed down aggregates", explainContainsPushDownAggregates);

    List<Object[]> actual = sql(select, tableName);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {6L, 23331.0});
    assertEquals("expected and actual should equal", expected, actual);
  }

  @Test
  public void testAggregatePushDownWithMetricsMode() {
    sql("CREATE TABLE %s (id LONG, data DOUBLE) USING iceberg", tableName);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "id", "counts");
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "data", "none");
    sql(
        "INSERT INTO TABLE %s VALUES (1, 1111), (1, 2222), (2, 3333), (2, 4444), (3, 5555), (3, 6666)",
        tableName);

    String select1 = "SELECT COUNT(data) FROM %s";

    List<Object[]> explain1 = sql("EXPLAIN " + select1, tableName);
    String explainString1 = explain1.get(0)[0].toString();
    boolean explainContainsPushDownAggregates = false;
    if (explainString1.contains("count(data)")) {
      explainContainsPushDownAggregates = true;
    }

    // count(data) is not pushed down because the metrics mode is `none`
    Assert.assertFalse(
        "explain should not contain the pushed down aggregates", explainContainsPushDownAggregates);

    List<Object[]> actual1 = sql(select1, tableName);
    List<Object[]> expected1 = Lists.newArrayList();
    expected1.add(new Object[] {6L});
    assertEquals("expected and actual should equal", expected1, actual1);

    String select2 = "SELECT COUNT(id) FROM %s";
    List<Object[]> explain2 = sql("EXPLAIN " + select2, tableName);
    String explainString2 = explain2.get(0)[0].toString();
    if (explainString2.contains("count(id)")) {
      explainContainsPushDownAggregates = true;
    }

    // count(id) is pushed down because the metrics mode is `counts`
    Assert.assertTrue(
        "explain should contain the pushed down aggregates", explainContainsPushDownAggregates);

    List<Object[]> actual2 = sql(select2, tableName);
    List<Object[]> expected2 = Lists.newArrayList();
    expected2.add(new Object[] {6L});
    assertEquals("expected and actual should equal", expected2, actual2);

    String select3 = "SELECT COUNT(id), MAX(id) FROM %s";
    explainContainsPushDownAggregates = false;
    List<Object[]> explain3 = sql("EXPLAIN " + select3, tableName);
    String explainString3 = explain3.get(0)[0].toString();
    if (explainString3.contains("count(id)")) {
      explainContainsPushDownAggregates = true;
    }

    // COUNT(id), MAX(id) are not pushed down because MAX(id) is not pushed down (metrics mode is
    // `counts`)
    Assert.assertFalse(
        "explain should not contain the pushed down aggregates", explainContainsPushDownAggregates);

    List<Object[]> actual3 = sql(select3, tableName);
    List<Object[]> expected3 = Lists.newArrayList();
    expected3.add(new Object[] {6L, 3L});
    assertEquals("expected and actual should equal", expected3, actual3);
  }

  @Test
  public void testAggregateNotPushDownForStringType() {
    sql("CREATE TABLE %s (id LONG, data STRING) USING iceberg", tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, '1111'), (1, '2222'), (2, '3333'), (2, '4444'), (3, '5555'), (3, '6666') ",
        tableName);

    String select1 = "SELECT MAX(id), MAX(data) FROM %s";

    List<Object[]> explain1 = sql("EXPLAIN " + select1, tableName);
    String explainString1 = explain1.get(0)[0].toString();
    boolean explainContainsPushDownAggregates = false;
    if (explainString1.contains("max(id)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertFalse(
        "explain should not contain the pushed down aggregates", explainContainsPushDownAggregates);

    List<Object[]> actual1 = sql(select1, tableName);
    List<Object[]> expected1 = Lists.newArrayList();
    expected1.add(new Object[] {3L, "6666"});
    assertEquals("expected and actual should equal", expected1, actual1);

    String select2 = "SELECT COUNT(data) FROM %s";
    List<Object[]> explain2 = sql("EXPLAIN " + select2, tableName);
    String explainString2 = explain2.get(0)[0].toString();
    if (explainString2.contains("count(data)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertTrue(
        "explain should contain the pushed down aggregates", explainContainsPushDownAggregates);

    List<Object[]> actual2 = sql(select2, tableName);
    List<Object[]> expected2 = Lists.newArrayList();
    expected2.add(new Object[] {6L});
    assertEquals("min/max/count push down", expected2, actual2);

    explainContainsPushDownAggregates = false;
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, TableProperties.DEFAULT_WRITE_METRICS_MODE, "full");
    String select3 = "SELECT count(data), max(data) FROM %s";
    List<Object[]> explain3 = sql("EXPLAIN " + select3, tableName);
    String explainString3 = explain3.get(0)[0].toString();
    if (explainString3.contains("count(data)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertTrue(
        "explain should contain the pushed down aggregates", explainContainsPushDownAggregates);

    List<Object[]> actual3 = sql(select3, tableName);
    List<Object[]> expected3 = Lists.newArrayList();
    expected3.add(new Object[] {6L, "6666"});
    assertEquals("min/max/count push down", expected3, actual3);
  }

  @Test
  public void testAggregateWithComplexType() {
    sql("CREATE TABLE %s (id INT, complex STRUCT<c1:INT,c2:STRING>) USING iceberg", tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, named_struct(\"c1\", 3, \"c2\", \"v1\")),"
            + "(2, named_struct(\"c1\", 2, \"c2\", \"v2\")), (3, null)",
        tableName);
    String select = "SELECT count(complex) FROM %s";
    List<Object[]> explain = sql("EXPLAIN " + select, tableName);
    String explainString = explain.get(0)[0].toString();
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("count(complex)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertFalse(
        "count not pushed down for complex types", explainContainsPushDownAggregates);

    List<Object[]> actual = sql(select, tableName);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {2L});
    assertEquals("count not push down", actual, expected);
  }

  @Test
  public void testAggregatePushDownInDeleteCopyOnWrite() {
    sql("CREATE TABLE %s (id LONG, data INT) USING iceberg", tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, 1111), (1, 2222), (2, 3333), (2, 4444), (3, 5555), (3, 6666) ",
        tableName);
    sql("DELETE FROM %s WHERE data = 1111", tableName);
    String select = "SELECT max(data), min(data), count(data) FROM %s";

    List<Object[]> explain = sql("EXPLAIN " + select, tableName);
    String explainString = explain.get(0)[0].toString();
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("max(data)")
        && explainString.contains("min(data)")
        && explainString.contains("count(data)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertTrue("min/max/count pushed down for deleted", explainContainsPushDownAggregates);

    List<Object[]> actual = sql(select, tableName);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {6666, 2222, 5L});
    assertEquals("min/max/count push down", expected, actual);
  }

  @Ignore
  public void testAggregatePushDownInDeleteMergeOnRead() {
    sql("CREATE TABLE %s (id LONG, data INT) USING iceberg", tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, 1111), (1, 2222), (2, 3333), (2, 4444), (3, 5555), (3, 6666) ",
        tableName);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, TableProperties.FORMAT_VERSION, "2");
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, TableProperties.DELETE_MODE, "merge-on-read");

    sql("DELETE FROM %s WHERE data = 1111", tableName);
    String select = "SELECT max(data), min(data), count(data) FROM %s";

    List<Object[]> explain = sql("EXPLAIN " + select, tableName);
    String explainString = explain.get(0)[0].toString();
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("MAX(data)")
        && explainString.contains("MIN(data)")
        && explainString.contains("COUNT(data)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertFalse(
        "min/max/count not pushed down for deleted", explainContainsPushDownAggregates);

    List<Object[]> actual = sql(select, tableName);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {6666, 2222, 5L});
    assertEquals("min/max/count push down", expected, actual);
  }

  @Test
  public void testAggregatePushDownForTimeTravel() {
    sql("CREATE TABLE %s (id LONG, data INT) USING iceberg", tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, 1111), (1, 2222), (2, 3333), (2, 4444), (3, 5555), (3, 6666) ",
        tableName);

    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    List<Object[]> expected1 = sql("SELECT count(id) FROM %s", tableName);

    sql("INSERT INTO %s VALUES (4, 7777), (5, 8888)", tableName);
    List<Object[]> expected2 = sql("SELECT count(id) FROM %s", tableName);

    List<Object[]> explain1 =
        sql("EXPLAIN SELECT count(id) FROM %s VERSION AS OF %s", tableName, snapshotId);
    String explainString1 = explain1.get(0)[0].toString();
    boolean explainContainsPushDownAggregates1 = false;
    if (explainString1.contains("count(id)")) {
      explainContainsPushDownAggregates1 = true;
    }
    Assert.assertTrue("count pushed down", explainContainsPushDownAggregates1);

    List<Object[]> actual1 =
        sql("SELECT count(id) FROM %s VERSION AS OF %s", tableName, snapshotId);
    assertEquals("count push down", expected1, actual1);

    List<Object[]> explain2 = sql("EXPLAIN SELECT count(id) FROM %s", tableName);
    String explainString2 = explain2.get(0)[0].toString();
    boolean explainContainsPushDownAggregates2 = false;
    if (explainString2.contains("count(id)")) {
      explainContainsPushDownAggregates2 = true;
    }

    Assert.assertTrue("count pushed down", explainContainsPushDownAggregates2);

    List<Object[]> actual2 = sql("SELECT count(id) FROM %s", tableName);
    assertEquals("count push down", expected2, actual2);
  }

  @Test
  public void testAllNull() {
    sql("CREATE TABLE %s (id int, data int) USING iceberg PARTITIONED BY (id)", tableName);
    sql(
        "INSERT INTO %s VALUES (1, null),"
            + "(1, null), "
            + "(2, null), "
            + "(2, null), "
            + "(3, null), "
            + "(3, null)",
        tableName);
    String select = "SELECT count(*), max(data), min(data), count(data) FROM %s";

    List<Object[]> explain = sql("EXPLAIN " + select, tableName);
    String explainString = explain.get(0)[0].toString();
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("max(data)")
        && explainString.contains("min(data)")
        && explainString.contains("count(data)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertTrue(
        "explain should contain the pushed down aggregates", explainContainsPushDownAggregates);

    List<Object[]> actual = sql(select, tableName);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {6L, null, null, 0L});
    assertEquals("min/max/count push down", expected, actual);
  }

  @Test
  public void testAllNaN() {
    sql("CREATE TABLE %s (id int, data float) USING iceberg PARTITIONED BY (id)", tableName);
    sql(
        "INSERT INTO %s VALUES (1, float('nan')),"
            + "(1, float('nan')), "
            + "(2, float('nan')), "
            + "(2, float('nan')), "
            + "(3, float('nan')), "
            + "(3, float('nan'))",
        tableName);
    String select = "SELECT count(*), max(data), min(data), count(data) FROM %s";

    List<Object[]> explain = sql("EXPLAIN " + select, tableName);
    String explainString = explain.get(0)[0].toString();
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("max(data)")
        && explainString.contains("min(data)")
        && explainString.contains("count(data)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertTrue(
        "explain should contain the pushed down aggregates", explainContainsPushDownAggregates);

    List<Object[]> actual = sql(select, tableName);
    List<Object[]> expected = Lists.newArrayList();
    //    expected.add(
    //            new Object[] {6L, Float.NaN, Float.NaN, 6L});
    expected.add(new Object[] {6L, null, null, 6L});
    assertEquals("min/max/count push down", expected, actual);
  }

  @Test
  public void testNaN() {
    sql("CREATE TABLE %s (id int, data float) USING iceberg PARTITIONED BY (id)", tableName);
    sql(
        "INSERT INTO %s VALUES (1, float('nan')),"
            + "(1, float('nan')), "
            + "(2, 2), "
            + "(2, float('nan')), "
            + "(3, float('nan')), "
            + "(3, 1)",
        tableName);
    String select = "SELECT count(*), max(data), min(data), count(data) FROM %s";

    List<Object[]> explain = sql("EXPLAIN " + select, tableName);
    String explainString = explain.get(0)[0].toString();
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("max(data)")
        && explainString.contains("min(data)")
        && explainString.contains("count(data)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertTrue(
        "explain should contain the pushed down aggregates", explainContainsPushDownAggregates);

    List<Object[]> actual = sql(select, tableName);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {6L, 2.0f, 1.0f, 6L});
    assertEquals("min/max/count push down", expected, actual);
  }

  @Test
  public void testInfinity() {
    sql(
        "CREATE TABLE %s (id int, data1 float, data2 double, data3 double) USING iceberg PARTITIONED BY (id)",
        tableName);
    sql(
        "INSERT INTO %s VALUES (1, float('-infinity'), double('infinity'), 1.23), "
            + "(1, float('-infinity'), double('infinity'), -1.23), "
            + "(1, float('-infinity'), double('infinity'), double('infinity')), "
            + "(1, float('-infinity'), double('infinity'), 2.23), "
            + "(1, float('-infinity'), double('infinity'), double('-infinity')), "
            + "(1, float('-infinity'), double('infinity'), -2.23)",
        tableName);
    String select =
        "SELECT count(*), max(data1), min(data1), count(data1), max(data2), min(data2), count(data2), max(data3), min(data3), count(data3) FROM %s";

    List<Object[]> explain = sql("EXPLAIN " + select, tableName);
    String explainString = explain.get(0)[0].toString();
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("max(data1)")
        && explainString.contains("min(data1)")
        && explainString.contains("count(data1)")
        && explainString.contains("max(data2)")
        && explainString.contains("min(data2)")
        && explainString.contains("count(data2)")
        && explainString.contains("max(data3)")
        && explainString.contains("min(data3)")
        && explainString.contains("count(data3)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertTrue(
        "explain should contain the pushed down aggregates", explainContainsPushDownAggregates);

    List<Object[]> actual = sql(select, tableName);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(
        new Object[] {
          6L,
          Float.NEGATIVE_INFINITY,
          Float.NEGATIVE_INFINITY,
          6L,
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          6L,
          Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY,
          6L
        });
    assertEquals("min/max/count push down", expected, actual);
  }
}
