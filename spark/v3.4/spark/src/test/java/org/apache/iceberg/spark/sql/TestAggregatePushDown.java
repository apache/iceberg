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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
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
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ExplainMode;
import org.apache.spark.sql.functions;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
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
          "CREATE TABLE %s (id LONG, int_data INT, boolean_data BOOLEAN, float_data FLOAT, double_data DOUBLE, "
              + "decimal_data DECIMAL(14, 2), binary_data binary) USING iceberg PARTITIONED BY (id)";
    } else {
      createTable =
          "CREATE TABLE %s (id LONG, int_data INT, boolean_data BOOLEAN, float_data FLOAT, double_data DOUBLE, "
              + "decimal_data DECIMAL(14, 2), binary_data binary) USING iceberg";
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
            + "max(int_data), min(int_data), count(int_data), "
            + "max(boolean_data), min(boolean_data), count(boolean_data), "
            + "max(float_data), min(float_data), count(float_data), "
            + "max(double_data), min(double_data), count(double_data), "
            + "max(decimal_data), min(decimal_data), count(decimal_data), "
            + "max(binary_data), min(binary_data), count(binary_data) FROM %s";

    List<Object[]> explain = sql("EXPLAIN " + select, tableName);
    String explainString = explain.get(0)[0].toString().toLowerCase(Locale.ROOT);
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("count(*)")
        && explainString.contains("max(id)")
        && explainString.contains("min(id)")
        && explainString.contains("count(id)")
        && explainString.contains("max(int_data)")
        && explainString.contains("min(int_data)")
        && explainString.contains("count(int_data)")
        && explainString.contains("max(boolean_data)")
        && explainString.contains("min(boolean_data)")
        && explainString.contains("count(boolean_data)")
        && explainString.contains("max(float_data)")
        && explainString.contains("min(float_data)")
        && explainString.contains("count(float_data)")
        && explainString.contains("max(double_data)")
        && explainString.contains("min(double_data)")
        && explainString.contains("count(double_data)")
        && explainString.contains("max(decimal_data)")
        && explainString.contains("min(decimal_data)")
        && explainString.contains("count(decimal_data)")
        && explainString.contains("max(binary_data)")
        && explainString.contains("min(binary_data)")
        && explainString.contains("count(binary_data)")) {
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
    String explainString = explain.get(0)[0].toString().toLowerCase(Locale.ROOT);
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
    String explainString = explain.get(0)[0].toString().toLowerCase(Locale.ROOT);
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("count(data)")) {
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
    String explainString1 = explain1.get(0)[0].toString().toLowerCase(Locale.ROOT);
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
    String explainString2 = explain2.get(0)[0].toString().toLowerCase(Locale.ROOT);
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
    String explainString3 = explain3.get(0)[0].toString().toLowerCase(Locale.ROOT);
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
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, TableProperties.DEFAULT_WRITE_METRICS_MODE, "truncate(16)");

    String select1 = "SELECT MAX(id), MAX(data) FROM %s";

    List<Object[]> explain1 = sql("EXPLAIN " + select1, tableName);
    String explainString1 = explain1.get(0)[0].toString().toLowerCase(Locale.ROOT);
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
    String explainString2 = explain2.get(0)[0].toString().toLowerCase(Locale.ROOT);
    if (explainString2.contains("count(data)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertTrue(
        "explain should contain the pushed down aggregates", explainContainsPushDownAggregates);

    List<Object[]> actual2 = sql(select2, tableName);
    List<Object[]> expected2 = Lists.newArrayList();
    expected2.add(new Object[] {6L});
    assertEquals("expected and actual should equal", expected2, actual2);

    explainContainsPushDownAggregates = false;
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, TableProperties.DEFAULT_WRITE_METRICS_MODE, "full");
    String select3 = "SELECT count(data), max(data) FROM %s";
    List<Object[]> explain3 = sql("EXPLAIN " + select3, tableName);
    String explainString3 = explain3.get(0)[0].toString().toLowerCase(Locale.ROOT);
    if (explainString3.contains("count(data)") && explainString3.contains("max(data)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertTrue(
        "explain should contain the pushed down aggregates", explainContainsPushDownAggregates);

    List<Object[]> actual3 = sql(select3, tableName);
    List<Object[]> expected3 = Lists.newArrayList();
    expected3.add(new Object[] {6L, "6666"});
    assertEquals("expected and actual should equal", expected3, actual3);
  }

  @Test
  public void testAggregatePushDownWithDataFilter() {
    testAggregatePushDownWithFilter(false);
  }

  @Test
  public void testAggregatePushDownWithPartitionFilter() {
    testAggregatePushDownWithFilter(true);
  }

  private void testAggregatePushDownWithFilter(boolean partitionFilerOnly) {
    String createTable;
    if (!partitionFilerOnly) {
      createTable = "CREATE TABLE %s (id LONG, data INT) USING iceberg";
    } else {
      createTable = "CREATE TABLE %s (id LONG, data INT) USING iceberg PARTITIONED BY (id)";
    }

    sql(createTable, tableName);

    sql(
        "INSERT INTO TABLE %s VALUES"
            + " (1, 11),"
            + " (1, 22),"
            + " (2, 33),"
            + " (2, 44),"
            + " (3, 55),"
            + " (3, 66) ",
        tableName);

    String select = "SELECT MIN(data) FROM %s WHERE id > 1";

    List<Object[]> explain = sql("EXPLAIN " + select, tableName);
    String explainString = explain.get(0)[0].toString().toLowerCase(Locale.ROOT);
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("min(data)")) {
      explainContainsPushDownAggregates = true;
    }

    if (!partitionFilerOnly) {
      // Filters are not completely pushed down, we can't push down aggregates
      Assert.assertFalse(
          "explain should not contain the pushed down aggregates",
          explainContainsPushDownAggregates);
    } else {
      // Filters are not completely pushed down, we can push down aggregates
      Assert.assertTrue(
          "explain should contain the pushed down aggregates", explainContainsPushDownAggregates);
    }

    List<Object[]> actual = sql(select, tableName);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {33});
    assertEquals("expected and actual should equal", expected, actual);
  }

  @Test
  public void testAggregateWithComplexType() {
    sql("CREATE TABLE %s (id INT, complex STRUCT<c1:INT,c2:STRING>) USING iceberg", tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, named_struct(\"c1\", 3, \"c2\", \"v1\")),"
            + "(2, named_struct(\"c1\", 2, \"c2\", \"v2\")), (3, null)",
        tableName);
    String select1 = "SELECT count(complex), count(id) FROM %s";
    List<Object[]> explain = sql("EXPLAIN " + select1, tableName);
    String explainString = explain.get(0)[0].toString().toLowerCase(Locale.ROOT);
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("count(complex)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertFalse(
        "count not pushed down for complex types", explainContainsPushDownAggregates);

    List<Object[]> actual = sql(select1, tableName);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {2L, 3L});
    assertEquals("count not push down", actual, expected);

    String select2 = "SELECT max(complex) FROM %s";
    explain = sql("EXPLAIN " + select2, tableName);
    explainString = explain.get(0)[0].toString().toLowerCase(Locale.ROOT);
    explainContainsPushDownAggregates = false;
    if (explainString.contains("max(complex)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertFalse("max not pushed down for complex types", explainContainsPushDownAggregates);
  }

  @Test
  public void testAggregationPushdownStructInteger() {
    sql("CREATE TABLE %s (id BIGINT, struct_with_int STRUCT<c1:BIGINT>) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, named_struct(\"c1\", NULL))", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, named_struct(\"c1\", 2))", tableName);
    sql("INSERT INTO TABLE %s VALUES (3, named_struct(\"c1\", 3))", tableName);

    String query = "SELECT COUNT(%s), MAX(%s), MIN(%s) FROM %s";
    String aggField = "struct_with_int.c1";
    assertAggregates(sql(query, aggField, aggField, aggField, tableName), 2L, 3L, 2L);
    assertExplainContains(
        sql("EXPLAIN " + query, aggField, aggField, aggField, tableName),
        "count(struct_with_int.c1)",
        "max(struct_with_int.c1)",
        "min(struct_with_int.c1)");
  }

  @Test
  public void testAggregationPushdownNestedStruct() {
    sql(
        "CREATE TABLE %s (id BIGINT, struct_with_int STRUCT<c1:STRUCT<c2:STRUCT<c3:STRUCT<c4:BIGINT>>>>) USING iceberg",
        tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, named_struct(\"c1\", named_struct(\"c2\", named_struct(\"c3\", named_struct(\"c4\", NULL)))))",
        tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (2, named_struct(\"c1\", named_struct(\"c2\", named_struct(\"c3\", named_struct(\"c4\", 2)))))",
        tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (3, named_struct(\"c1\", named_struct(\"c2\", named_struct(\"c3\", named_struct(\"c4\", 3)))))",
        tableName);

    String query = "SELECT COUNT(%s), MAX(%s), MIN(%s) FROM %s";
    String aggField = "struct_with_int.c1.c2.c3.c4";

    assertAggregates(sql(query, aggField, aggField, aggField, tableName), 2L, 3L, 2L);

    assertExplainContains(
        sql("EXPLAIN " + query, aggField, aggField, aggField, tableName),
        "count(struct_with_int.c1.c2.c3.c4)",
        "max(struct_with_int.c1.c2.c3.c4)",
        "min(struct_with_int.c1.c2.c3.c4)");
  }

  @Test
  public void testAggregationPushdownStructTimestamp() {
    sql(
        "CREATE TABLE %s (id BIGINT, struct_with_ts STRUCT<c1:TIMESTAMP>) USING iceberg",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (1, named_struct(\"c1\", NULL))", tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (2, named_struct(\"c1\", timestamp('2023-01-30T22:22:22Z')))",
        tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (3, named_struct(\"c1\", timestamp('2023-01-30T22:23:23Z')))",
        tableName);

    String query = "SELECT COUNT(%s), MAX(%s), MIN(%s) FROM %s";
    String aggField = "struct_with_ts.c1";

    assertAggregates(
        sql(query, aggField, aggField, aggField, tableName),
        2L,
        new Timestamp(1675117403000L),
        new Timestamp(1675117342000L));

    assertExplainContains(
        sql("EXPLAIN " + query, aggField, aggField, aggField, tableName),
        "count(struct_with_ts.c1)",
        "max(struct_with_ts.c1)",
        "min(struct_with_ts.c1)");
  }

  @Test
  public void testAggregationPushdownOnBucketedColumn() {
    sql(
        "CREATE TABLE %s (id BIGINT, struct_with_int STRUCT<c1:INT>) USING iceberg PARTITIONED BY (bucket(8, id))",
        tableName);

    sql("INSERT INTO TABLE %s VALUES (1, named_struct(\"c1\", NULL))", tableName);
    sql("INSERT INTO TABLE %s VALUES (null, named_struct(\"c1\", 2))", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, named_struct(\"c1\", 3))", tableName);

    String query = "SELECT COUNT(%s), MAX(%s), MIN(%s) FROM %s";
    String aggField = "id";
    assertAggregates(sql(query, aggField, aggField, aggField, tableName), 2L, 2L, 1L);
    assertExplainContains(
        sql("EXPLAIN " + query, aggField, aggField, aggField, tableName),
        "count(id)",
        "max(id)",
        "min(id)");
  }

  private void assertAggregates(
      List<Object[]> actual, Object expectedCount, Object expectedMax, Object expectedMin) {
    Object actualCount = actual.get(0)[0];
    Object actualMax = actual.get(0)[1];
    Object actualMin = actual.get(0)[2];

    assertThat(actualCount).as("Expected and actual count should equal").isEqualTo(expectedCount);
    assertThat(actualMax).as("Expected and actual max should equal").isEqualTo(expectedMax);
    assertThat(actualMin).as("Expected and actual min should equal").isEqualTo(expectedMin);
  }

  private void assertExplainContains(List<Object[]> explain, String... expectedFragments) {
    String explainString = explain.get(0)[0].toString().toLowerCase(Locale.ROOT);
    Arrays.stream(expectedFragments)
        .forEach(
            fragment ->
                assertThat(explainString)
                    .as("Expected to find plan fragment in explain plan")
                    .contains(fragment));
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
    String explainString = explain.get(0)[0].toString().toLowerCase(Locale.ROOT);
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
    String explainString1 = explain1.get(0)[0].toString().toLowerCase(Locale.ROOT);
    boolean explainContainsPushDownAggregates1 = false;
    if (explainString1.contains("count(id)")) {
      explainContainsPushDownAggregates1 = true;
    }
    Assert.assertTrue("count pushed down", explainContainsPushDownAggregates1);

    List<Object[]> actual1 =
        sql("SELECT count(id) FROM %s VERSION AS OF %s", tableName, snapshotId);
    assertEquals("count push down", expected1, actual1);

    List<Object[]> explain2 = sql("EXPLAIN SELECT count(id) FROM %s", tableName);
    String explainString2 = explain2.get(0)[0].toString().toLowerCase(Locale.ROOT);
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
    String explainString = explain.get(0)[0].toString().toLowerCase(Locale.ROOT);
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
    String explainString = explain.get(0)[0].toString().toLowerCase(Locale.ROOT);
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("max(data)")
        || explainString.contains("min(data)")
        || explainString.contains("count(data)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertFalse(
        "explain should not contain the pushed down aggregates", explainContainsPushDownAggregates);

    List<Object[]> actual = sql(select, tableName);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {6L, Float.NaN, Float.NaN, 6L});
    assertEquals("expected and actual should equal", expected, actual);
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
    String explainString = explain.get(0)[0].toString().toLowerCase(Locale.ROOT);
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("max(data)")
        || explainString.contains("min(data)")
        || explainString.contains("count(data)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertFalse(
        "explain should not contain the pushed down aggregates", explainContainsPushDownAggregates);

    List<Object[]> actual = sql(select, tableName);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {6L, Float.NaN, 1.0F, 6L});
    assertEquals("expected and actual should equal", expected, actual);
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
    String explainString = explain.get(0)[0].toString().toLowerCase(Locale.ROOT);
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

  @Test
  public void testAggregatePushDownForIncrementalScan() {
    sql("CREATE TABLE %s (id LONG, data INT) USING iceberg", tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, 1111), (1, 2222), (2, 3333), (2, 4444), (3, 5555), (3, 6666) ",
        tableName);
    long snapshotId1 = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    sql("INSERT INTO %s VALUES (4, 7777), (5, 8888)", tableName);
    long snapshotId2 = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    sql("INSERT INTO %s VALUES (6, -7777), (7, 8888)", tableName);
    long snapshotId3 = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    sql("INSERT INTO %s VALUES (8, 7777), (9, 9999)", tableName);

    Dataset<Row> pushdownDs =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.START_SNAPSHOT_ID, snapshotId2)
            .option(SparkReadOptions.END_SNAPSHOT_ID, snapshotId3)
            .load(tableName)
            .agg(functions.min("data"), functions.max("data"), functions.count("data"));
    String explain1 = pushdownDs.queryExecution().explainString(ExplainMode.fromString("simple"));
    assertThat(explain1).contains("LocalTableScan", "min(data)", "max(data)", "count(data)");

    List<Object[]> expected1 = Lists.newArrayList();
    expected1.add(new Object[] {-7777, 8888, 2L});
    assertEquals("min/max/count push down", expected1, rowsToJava(pushdownDs.collectAsList()));

    Dataset<Row> unboundedPushdownDs =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.START_SNAPSHOT_ID, snapshotId1)
            .load(tableName)
            .agg(functions.min("data"), functions.max("data"), functions.count("data"));
    String explain2 =
        unboundedPushdownDs.queryExecution().explainString(ExplainMode.fromString("simple"));
    assertThat(explain2).contains("LocalTableScan", "min(data)", "max(data)", "count(data)");

    List<Object[]> expected2 = Lists.newArrayList();
    expected2.add(new Object[] {-7777, 9999, 6L});
    assertEquals(
        "min/max/count push down", expected2, rowsToJava(unboundedPushdownDs.collectAsList()));
  }
}
