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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestAggregatePushDown extends SparkCatalogTestBase {

  public TestAggregatePushDown(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
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
          "CREATE TABLE %s (id LONG, intData INT, booleanData BOOLEAN, floatData FLOAT, doubleData DOUBLE, stringData STRING, "
              + "decimalData DECIMAL(14, 2), binaryData binary) USING iceberg PARTITIONED BY (id)";
    } else {
      createTable =
          "CREATE TABLE %s (id LONG, intData INT, booleanData BOOLEAN, floatData FLOAT, doubleData DOUBLE, stringData STRING, "
              + "decimalData DECIMAL(14, 2), binaryData binary) USING iceberg";
    }
    sql(createTable, tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, null, false, null, null, 'gggg', 11.11, X'1111'),"
            + " (1, null, true, 2.222, 2.222222, null, 22.22, X'2222'),"
            + " (2, 33, false, 3.333, 3.333333, 'iiii', 33.33, X'3333'),"
            + " (2, 44, true, null, 4.444444, 'dddd', 44.44, X'4444'),"
            + " (3, 55, false, 5.555, 5.555555, 'eeee', 55.55, X'5555'),"
            + " (3, null, true, null, 6.666666, null, 66.66, null) ",
        tableName);

    String select =
        "SELECT count(*), max(id), min(id), count(id), "
            + "max(stringData), min(stringData), count(stringData), "
            + "max(intData), min(intData), count(intData), "
            + "max(booleanData), min(booleanData), count(booleanData), "
            + "max(floatData), min(floatData), count(floatData), "
            + "max(doubleData), min(doubleData), count(doubleData), "
            + "max(decimalData), min(decimalData), count(decimalData), "
            + "max(binaryData), min(binaryData), count(binaryData) FROM %s";

    List<Object[]> explain = sql("EXPLAIN " + select, tableName);
    String explainString = explain.get(0)[0].toString();
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("COUNT(*)")
        && explainString.contains("MAX(id)")
        && explainString.contains("MIN(id)")
        && explainString.contains("COUNT(id)")
        && explainString.contains("MAX(stringData)")
        && explainString.contains("MIN(stringData)")
        && explainString.contains("COUNT(stringData)")
        && explainString.contains("MAX(intData)")
        && explainString.contains("MIN(intData)")
        && explainString.contains("COUNT(intData)")
        && explainString.contains("MAX(booleanData)")
        && explainString.contains("MIN(booleanData)")
        && explainString.contains("COUNT(booleanData)")
        && explainString.contains("MAX(floatData)")
        && explainString.contains("MIN(floatData)")
        && explainString.contains("COUNT(floatData)")
        && explainString.contains("MAX(doubleData)")
        && explainString.contains("MIN(doubleData)")
        && explainString.contains("COUNT(doubleData)")
        && explainString.contains("MAX(decimalData)")
        && explainString.contains("MIN(decimalData)")
        && explainString.contains("COUNT(decimalData)")
        && explainString.contains("MAX(binaryData)")
        && explainString.contains("MIN(binaryData)")
        && explainString.contains("COUNT(binaryData)")) {
      explainContainsPushDownAggregates = true;
    }
    Assert.assertTrue(
        "explain should contain the pushed down aggregates", explainContainsPushDownAggregates);

    List<Object[]> actual = sql(select, tableName);

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, TableProperties.AGGREGATE_PUSHDOWN_ENABLED, "false");
    List<Object[]> expected = sql(select, tableName);
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
    if (explainString.contains("MAX(d)")
        && explainString.contains("MIN(d)")
        && explainString.contains("COUNT(d)")
        && explainString.contains("MAX(ts)")
        && explainString.contains("MIN(ts)")
        && explainString.contains("COUNT(ts)")) {
      explainContainsPushDownAggregates = true;
    }
    Assert.assertTrue(
        "explain should contain the pushed down aggregates", explainContainsPushDownAggregates);

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, TableProperties.AGGREGATE_PUSHDOWN_ENABLED, "false");
    List<Object[]> expected = sql(select, tableName);
    List<Object[]> actual = sql(select, tableName);
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
        "explain should contain the pushed down aggregates", explainContainsPushDownAggregates);
  }

  @Test
  public void testAggregateWithComplexTypeNotPushDown() {
    sql("CREATE TABLE %s (id INT, complex STRUCT<c1:INT,c2:STRING>) USING iceberg", tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, named_struct(\"c1\", 3, \"c2\", \"v1\")),"
            + "(2, named_struct(\"c1\", 2, \"c2\", \"v2\"))",
        tableName);
    String select = "SELECT max(complex), min(complex), count(complex) FROM %s";

    List<Object[]> explain = sql("EXPLAIN " + select, tableName);
    String explainString = explain.get(0)[0].toString();
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("MAX(complex)")
        || explainString.contains("MIN(complex)")
        || explainString.contains("COUNT(complex)")) {
      explainContainsPushDownAggregates = true;
    }
    Assert.assertFalse(
        "min/max/count not pushed down for complex types", explainContainsPushDownAggregates);
  }

  @Test
  public void testAggregratePushDownInDeleteCopyOnWrite() {
    sql("CREATE TABLE %s (id LONG, data INT) USING iceberg", tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, 1111), (1, 2222), (2, 3333), (2, 4444), (3, 5555), (3, 6666) ",
        tableName);
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
    Assert.assertTrue(
        "min/max/count not pushed down for deleted", explainContainsPushDownAggregates);

    List<Object[]> actual = sql(select, tableName);

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, TableProperties.AGGREGATE_PUSHDOWN_ENABLED, "false");
    List<Object[]> expected = sql(select, tableName);
    assertEquals("min/max/count push down", expected, actual);
  }

  @Test
  public void testAggregrateWithGroupByNotPushDown() {
    sql("CREATE TABLE %s (id LONG, data INT) USING iceberg PARTITIONED BY (id)", tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, 1111), (1, 2222), (2, 3333), (2, 4444), (3, 5555), (3, 6666) ",
        tableName);
    String select = "SELECT max(data), min(data) FROM %s GROUP BY id";

    List<Object[]> explain = sql("EXPLAIN " + select, tableName);
    String explainString = explain.get(0)[0].toString();
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("MAX(data)") || explainString.contains("MIN(data)")) {
      explainContainsPushDownAggregates = true;
    }
    Assert.assertFalse("min/max not pushed down", explainContainsPushDownAggregates);
  }
}
