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
package org.apache.iceberg.flink.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.TestBase;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestFlinkTableSource extends TestBase {

  private static final String CATALOG_NAME = "test_catalog";
  private static final String DATABASE_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";
  private final FileFormat format = FileFormat.AVRO;

  private int scanEventCount = 0;
  private ScanEvent lastScanEvent = null;

  @Override
  protected TableEnvironment getTableEnv() {
    super.getTableEnv().getConfig().getConfiguration().set(CoreOptions.DEFAULT_PARALLELISM, 1);
    return super.getTableEnv();
  }

  @BeforeEach
  public void before() throws IOException {
    // register a scan event listener to validate pushdown
    Listeners.register(
        event -> {
          scanEventCount += 1;
          lastScanEvent = event;
        },
        ScanEvent.class);

    File warehouseFile = File.createTempFile("junit", null, temporaryDirectory.toFile());
    assertThat(warehouseFile.delete()).isTrue();
    String warehouse = String.format("file:%s", warehouseFile);

    sql(
        "CREATE CATALOG %s WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')",
        CATALOG_NAME, warehouse);
    sql("USE CATALOG %s", CATALOG_NAME);
    sql("CREATE DATABASE %s", DATABASE_NAME);
    sql("USE %s", DATABASE_NAME);
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR,d DOUBLE) WITH ('write.format.default'='%s')",
        TABLE_NAME, format.name());
    sql(
        "INSERT INTO %s VALUES (1,'iceberg',10),(2,'b',20),(3,CAST(NULL AS VARCHAR),30)",
        TABLE_NAME);

    this.scanEventCount = 0;
    this.lastScanEvent = null;
  }

  @AfterEach
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, TABLE_NAME);
    sql("DROP DATABASE IF EXISTS %s", DATABASE_NAME);
    dropCatalog(CATALOG_NAME, true);
  }

  @Test
  public void testLimitPushDown() {

    assertThatThrownBy(() -> sql("SELECT * FROM %s LIMIT -1", TABLE_NAME))
        .isInstanceOf(SqlParserException.class)
        .hasMessageStartingWith("SQL parse failed.");

    assertThat(sql("SELECT * FROM %s LIMIT 0", TABLE_NAME)).isEmpty();

    String sqlLimitExceed = String.format("SELECT * FROM %s LIMIT 4", TABLE_NAME);
    List<Row> resultExceed = sql(sqlLimitExceed);
    assertThat(resultExceed).hasSize(3);
    List<Row> expectedList =
        Lists.newArrayList(Row.of(1, "iceberg", 10.0), Row.of(2, "b", 20.0), Row.of(3, null, 30.0));
    assertSameElements(expectedList, resultExceed);

    String querySql = String.format("SELECT * FROM %s LIMIT 1", TABLE_NAME);
    String explain = getTableEnv().explainSql(querySql);
    String expectedExplain = "limit=[1]";
    assertThat(explain).as("Explain should contain LimitPushDown").contains(expectedExplain);
    List<Row> result = sql(querySql);
    assertThat(result).hasSize(1);
    assertThat(result).containsAnyElementsOf(expectedList);

    String sqlMixed = String.format("SELECT * FROM %s WHERE id = 1 LIMIT 2", TABLE_NAME);
    List<Row> mixedResult = sql(sqlMixed);
    assertThat(mixedResult).hasSize(1).first().isEqualTo(Row.of(1, "iceberg", 10.0));
  }

  @Test
  public void testNoFilterPushDown() {
    String sql = String.format("SELECT * FROM %s ", TABLE_NAME);
    List<Row> result = sql(sql);
    List<Row> expectedRecords =
        Lists.newArrayList(Row.of(1, "iceberg", 10.0), Row.of(2, "b", 20.0), Row.of(3, null, 30.0));
    assertSameElements(expectedRecords, result);
    assertThat(lastScanEvent.filter())
        .as("Should not push down a filter")
        .isEqualTo(Expressions.alwaysTrue());
  }

  @Test
  public void testFilterPushDownEqual() {
    String sqlLiteralRight = String.format("SELECT * FROM %s WHERE id = 1 ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") == 1";

    List<Row> result = sql(sqlLiteralRight);
    assertThat(result).hasSize(1).first().isEqualTo(Row.of(1, "iceberg", 10.0));
    assertThat(scanEventCount).isEqualTo(1);
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedFilter);
  }

  @Test
  public void testFilterPushDownEqualNull() {
    String sqlEqualNull = String.format("SELECT * FROM %s WHERE data = NULL ", TABLE_NAME);

    List<Row> result = sql(sqlEqualNull);
    assertThat(result).isEmpty();
    assertThat(lastScanEvent).as("Should not push down a filter").isNull();
  }

  @Test
  public void testFilterPushDownEqualLiteralOnLeft() {
    String sqlLiteralLeft = String.format("SELECT * FROM %s WHERE 1 = id ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") == 1";

    List<Row> resultLeft = sql(sqlLiteralLeft);
    assertThat(resultLeft).hasSize(1).first().isEqualTo(Row.of(1, "iceberg", 10.0));
    assertThat(scanEventCount).isEqualTo(1);
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedFilter);
  }

  @Test
  public void testFilterPushDownNoEqual() {
    String sqlNE = String.format("SELECT * FROM %s WHERE id <> 1 ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") != 1";

    List<Row> resultNE = sql(sqlNE);
    assertThat(resultNE).hasSize(2);

    List<Row> expectedNE = Lists.newArrayList(Row.of(2, "b", 20.0), Row.of(3, null, 30.0));
    assertSameElements(expectedNE, resultNE);
    assertThat(scanEventCount).isEqualTo(1);
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedFilter);
  }

  @Test
  public void testFilterPushDownNoEqualNull() {
    String sqlNotEqualNull = String.format("SELECT * FROM %s WHERE data <> NULL ", TABLE_NAME);

    List<Row> resultNE = sql(sqlNotEqualNull);
    assertThat(resultNE).isEmpty();
    assertThat(lastScanEvent).as("Should not push down a filter").isNull();
  }

  @Test
  public void testFilterPushDownAnd() {
    String sqlAnd =
        String.format("SELECT * FROM %s WHERE id = 1 AND data = 'iceberg' ", TABLE_NAME);

    List<Row> resultAnd = sql(sqlAnd);
    assertThat(resultAnd).hasSize(1).first().isEqualTo(Row.of(1, "iceberg", 10.0));
    assertThat(scanEventCount).isEqualTo(1);
    String expected = "(ref(name=\"id\") == 1 and ref(name=\"data\") == \"iceberg\")";
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expected);
  }

  @Test
  public void testFilterPushDownOr() {
    String sqlOr = String.format("SELECT * FROM %s WHERE id = 1 OR data = 'b' ", TABLE_NAME);
    String expectedFilter = "(ref(name=\"id\") == 1 or ref(name=\"data\") == \"b\")";

    List<Row> resultOr = sql(sqlOr);
    assertThat(resultOr).hasSize(2);

    List<Row> expectedOR = Lists.newArrayList(Row.of(1, "iceberg", 10.0), Row.of(2, "b", 20.0));
    assertSameElements(expectedOR, resultOr);

    assertThat(scanEventCount).isEqualTo(1);
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedFilter);
  }

  @Test
  public void testFilterPushDownGreaterThan() {
    String sqlGT = String.format("SELECT * FROM %s WHERE id > 1 ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") > 1";

    List<Row> resultGT = sql(sqlGT);
    assertThat(resultGT).hasSize(2);

    List<Row> expectedGT = Lists.newArrayList(Row.of(2, "b", 20.0), Row.of(3, null, 30.0));
    assertSameElements(expectedGT, resultGT);

    assertThat(scanEventCount).isEqualTo(1);
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedFilter);
  }

  @Test
  public void testFilterPushDownGreaterThanNull() {
    String sqlGT = String.format("SELECT * FROM %s WHERE data > null ", TABLE_NAME);

    List<Row> resultGT = sql(sqlGT);
    assertThat(resultGT).isEmpty();
    assertThat(lastScanEvent).as("Should not push down a filter").isNull();
  }

  @Test
  public void testFilterPushDownGreaterThanLiteralOnLeft() {
    String sqlGT = String.format("SELECT * FROM %s WHERE 3 > id ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") < 3";

    List<Row> resultGT = sql(sqlGT);
    assertThat(resultGT).hasSize(2);

    List<Row> expectedGT = Lists.newArrayList(Row.of(1, "iceberg", 10.0), Row.of(2, "b", 20.0));
    assertSameElements(expectedGT, resultGT);

    assertThat(scanEventCount).isEqualTo(1);
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedFilter);
  }

  @Test
  public void testFilterPushDownGreaterThanEqual() {
    String sqlGTE = String.format("SELECT * FROM %s WHERE id >= 2 ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") >= 2";

    List<Row> resultGTE = sql(sqlGTE);
    assertThat(resultGTE).hasSize(2);

    List<Row> expectedGTE = Lists.newArrayList(Row.of(2, "b", 20.0), Row.of(3, null, 30.0));
    assertSameElements(expectedGTE, resultGTE);

    assertThat(scanEventCount).isEqualTo(1);
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedFilter);
  }

  @Test
  public void testFilterPushDownGreaterThanEqualNull() {
    String sqlGTE = String.format("SELECT * FROM %s WHERE data >= null ", TABLE_NAME);

    List<Row> resultGT = sql(sqlGTE);
    assertThat(resultGT).isEmpty();
    assertThat(lastScanEvent).as("Should not push down a filter").isNull();
  }

  @Test
  public void testFilterPushDownGreaterThanEqualLiteralOnLeft() {
    String sqlGTE = String.format("SELECT * FROM %s WHERE 2 >= id ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") <= 2";

    List<Row> resultGTE = sql(sqlGTE);
    assertThat(resultGTE).hasSize(2);

    List<Row> expectedGTE = Lists.newArrayList(Row.of(1, "iceberg", 10.0), Row.of(2, "b", 20.0));
    assertSameElements(expectedGTE, resultGTE);

    assertThat(scanEventCount).isEqualTo(1);
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedFilter);
  }

  @Test
  public void testFilterPushDownLessThan() {
    String sqlLT = String.format("SELECT * FROM %s WHERE id < 2 ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") < 2";

    List<Row> resultLT = sql(sqlLT);
    assertThat(resultLT).hasSize(1).first().isEqualTo(Row.of(1, "iceberg", 10.0));

    assertThat(scanEventCount).isEqualTo(1);
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedFilter);
  }

  @Test
  public void testFilterPushDownLessThanNull() {
    String sqlLT = String.format("SELECT * FROM %s WHERE data < null ", TABLE_NAME);

    List<Row> resultGT = sql(sqlLT);
    assertThat(resultGT).isEmpty();
    assertThat(lastScanEvent).as("Should not push down a filter").isNull();
  }

  @Test
  public void testFilterPushDownLessThanLiteralOnLeft() {
    String sqlLT = String.format("SELECT * FROM %s WHERE 2 < id ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") > 2";

    List<Row> resultLT = sql(sqlLT);
    assertThat(resultLT).hasSize(1).first().isEqualTo(Row.of(3, null, 30.0));

    assertThat(scanEventCount).isEqualTo(1);
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedFilter);
  }

  @Test
  public void testFilterPushDownLessThanEqual() {
    String sqlLTE = String.format("SELECT * FROM %s WHERE id <= 1 ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") <= 1";

    List<Row> resultLTE = sql(sqlLTE);
    assertThat(resultLTE).hasSize(1).first().isEqualTo(Row.of(1, "iceberg", 10.0));

    assertThat(scanEventCount).isEqualTo(1);
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedFilter);
  }

  @Test
  public void testFilterPushDownLessThanEqualNull() {
    String sqlLTE = String.format("SELECT * FROM %s WHERE data <= null ", TABLE_NAME);

    List<Row> resultGT = sql(sqlLTE);
    assertThat(resultGT).isEmpty();
    assertThat(lastScanEvent).as("Should not push down a filter").isNull();
  }

  @Test
  public void testFilterPushDownLessThanEqualLiteralOnLeft() {
    String sqlLTE = String.format("SELECT * FROM %s WHERE 3 <= id  ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") >= 3";

    List<Row> resultLTE = sql(sqlLTE);
    assertThat(resultLTE).hasSize(1).first().isEqualTo(Row.of(3, null, 30.0));

    assertThat(scanEventCount).isEqualTo(1);
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedFilter);
  }

  @Test
  public void testFilterPushDownIn() {
    String sqlIN = String.format("SELECT * FROM %s WHERE id IN (1,2) ", TABLE_NAME);
    String expectedFilter = "(ref(name=\"id\") == 1 or ref(name=\"id\") == 2)";
    List<Row> resultIN = sql(sqlIN);
    assertThat(resultIN).hasSize(2);

    List<Row> expectedIN = Lists.newArrayList(Row.of(1, "iceberg", 10.0), Row.of(2, "b", 20.0));
    assertSameElements(expectedIN, resultIN);
    assertThat(scanEventCount).isEqualTo(1);
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedFilter);
  }

  @Test
  public void testFilterPushDownInNull() {
    String sqlInNull =
        String.format("SELECT * FROM %s WHERE data IN ('iceberg',NULL) ", TABLE_NAME);

    List<Row> result = sql(sqlInNull);
    assertThat(result).hasSize(1).first().isEqualTo(Row.of(1, "iceberg", 10.0));

    // In SQL, null check can only be done as IS NULL or IS NOT NULL, so it's correct to ignore it
    // and push the rest down.
    String expectedScan = "ref(name=\"data\") == \"iceberg\"";
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedScan);
  }

  @Test
  public void testFilterPushDownNotIn() {
    String sqlNotIn = String.format("SELECT * FROM %s WHERE id NOT IN (3,2) ", TABLE_NAME);

    List<Row> resultNotIn = sql(sqlNotIn);
    assertThat(resultNotIn).hasSize(1).first().isEqualTo(Row.of(1, "iceberg", 10.0));
    assertThat(scanEventCount).isEqualTo(1);
    String expectedScan = "(ref(name=\"id\") != 2 and ref(name=\"id\") != 3)";
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedScan);
  }

  @Test
  public void testFilterPushDownNotInNull() {
    String sqlNotInNull = String.format("SELECT * FROM %s WHERE id NOT IN (1,2,NULL) ", TABLE_NAME);
    List<Row> resultGT = sql(sqlNotInNull);
    assertThat(resultGT).isEmpty();
    assertThat(lastScanEvent)
        .as(
            "As the predicate pushdown filter out all rows, Flink did not create scan plan, so it doesn't publish any ScanEvent.")
        .isNull();
  }

  @Test
  public void testFilterPushDownIsNotNull() {
    String sqlNotNull = String.format("SELECT * FROM %s WHERE data IS NOT NULL", TABLE_NAME);
    String expectedFilter = "not_null(ref(name=\"data\"))";

    List<Row> resultNotNull = sql(sqlNotNull);
    assertThat(resultNotNull).hasSize(2);

    List<Row> expected = Lists.newArrayList(Row.of(1, "iceberg", 10.0), Row.of(2, "b", 20.0));
    assertSameElements(expected, resultNotNull);

    assertThat(scanEventCount).isEqualTo(1);
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedFilter);
  }

  @Test
  public void testFilterPushDownIsNull() {
    String sqlNull = String.format("SELECT * FROM %s WHERE data IS  NULL", TABLE_NAME);
    String expectedFilter = "is_null(ref(name=\"data\"))";

    List<Row> resultNull = sql(sqlNull);
    assertThat(resultNull).hasSize(1).first().isEqualTo(Row.of(3, null, 30.0));

    assertThat(scanEventCount).isEqualTo(1);
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedFilter);
  }

  @Test
  public void testFilterPushDownNot() {
    String sqlNot = String.format("SELECT * FROM %s WHERE NOT (id = 1 OR id = 2 ) ", TABLE_NAME);

    List<Row> resultNot = sql(sqlNot);
    assertThat(resultNot).hasSize(1).first().isEqualTo(Row.of(3, null, 30.0));

    assertThat(scanEventCount).isEqualTo(1);
    String expectedFilter = "(ref(name=\"id\") != 1 and ref(name=\"id\") != 2)";
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedFilter);
  }

  @Test
  public void testFilterPushDownBetween() {
    String sqlBetween = String.format("SELECT * FROM %s WHERE id BETWEEN 1 AND 2 ", TABLE_NAME);

    List<Row> resultBetween = sql(sqlBetween);
    assertThat(resultBetween).hasSize(2);

    List<Row> expectedBetween =
        Lists.newArrayList(Row.of(1, "iceberg", 10.0), Row.of(2, "b", 20.0));
    assertSameElements(expectedBetween, resultBetween);

    assertThat(scanEventCount).isEqualTo(1);
    String expected = "(ref(name=\"id\") >= 1 and ref(name=\"id\") <= 2)";
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expected);
  }

  @Test
  public void testFilterPushDownNotBetween() {
    String sqlNotBetween =
        String.format("SELECT * FROM %s WHERE id  NOT BETWEEN 2 AND 3 ", TABLE_NAME);
    String expectedFilter = "(ref(name=\"id\") < 2 or ref(name=\"id\") > 3)";

    List<Row> resultNotBetween = sql(sqlNotBetween);
    assertThat(resultNotBetween).hasSize(1).first().isEqualTo(Row.of(1, "iceberg", 10.0));

    assertThat(scanEventCount).isEqualTo(1);
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedFilter);
  }

  @Test
  public void testFilterPushDownLike() {
    String expectedFilter = "ref(name=\"data\") startsWith \"\"ice\"\"";

    String sqlLike = "SELECT * FROM " + TABLE_NAME + " WHERE data LIKE 'ice%%' ";
    List<Row> resultLike = sql(sqlLike);
    assertThat(resultLike).hasSize(1).first().isEqualTo(Row.of(1, "iceberg", 10.0));
    assertThat(scanEventCount).isEqualTo(1);
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedFilter);

    // %% won't match the row with null value
    sqlLike = "SELECT * FROM  " + TABLE_NAME + "  WHERE data LIKE '%%' ";
    resultLike = sql(sqlLike);
    assertThat(resultLike).hasSize(2);
    List<Row> expectedRecords =
        Lists.newArrayList(Row.of(1, "iceberg", 10.0), Row.of(2, "b", 20.0));
    assertSameElements(expectedRecords, resultLike);
    String expectedScan = "not_null(ref(name=\"data\"))";
    assertThat(lastScanEvent.filter())
        .as("Should contain the push down filter")
        .asString()
        .isEqualTo(expectedScan);
  }

  @Test
  public void testFilterNotPushDownLike() {
    Row expectRecord = Row.of(1, "iceberg", 10.0);
    String sqlNoPushDown = "SELECT * FROM " + TABLE_NAME + " WHERE data LIKE '%%i' ";
    List<Row> resultLike = sql(sqlNoPushDown);
    assertThat(resultLike).isEmpty();
    assertThat(lastScanEvent.filter())
        .as("Should not push down a filter")
        .isEqualTo(Expressions.alwaysTrue());

    sqlNoPushDown = "SELECT * FROM " + TABLE_NAME + " WHERE data LIKE '%%i%%' ";
    resultLike = sql(sqlNoPushDown);
    assertThat(resultLike).hasSize(1).first().isEqualTo(expectRecord);
    assertThat(lastScanEvent.filter())
        .as("Should not push down a filter")
        .isEqualTo(Expressions.alwaysTrue());

    sqlNoPushDown = "SELECT * FROM  " + TABLE_NAME + "  WHERE data LIKE '%%ice%%g' ";
    resultLike = sql(sqlNoPushDown);
    assertThat(resultLike).hasSize(1).first().isEqualTo(expectRecord);
    assertThat(lastScanEvent.filter())
        .as("Should not push down a filter")
        .isEqualTo(Expressions.alwaysTrue());

    sqlNoPushDown = "SELECT * FROM  " + TABLE_NAME + "  WHERE data LIKE 'iceber_' ";
    resultLike = sql(sqlNoPushDown);
    assertThat(resultLike).hasSize(1).first().isEqualTo(expectRecord);
    assertThat(lastScanEvent.filter())
        .as("Should not push down a filter")
        .isEqualTo(Expressions.alwaysTrue());

    sqlNoPushDown = "SELECT * FROM  " + TABLE_NAME + "  WHERE data LIKE 'i%%g' ";
    resultLike = sql(sqlNoPushDown);
    assertThat(resultLike).hasSize(1).first().isEqualTo(expectRecord);
    assertThat(lastScanEvent.filter())
        .as("Should not push down a filter")
        .isEqualTo(Expressions.alwaysTrue());
  }

  @Test
  public void testFilterPushDown2Literal() {
    String sql2Literal = String.format("SELECT * FROM %s WHERE 1 > 0 ", TABLE_NAME);
    List<Row> result = sql(sql2Literal);
    List<Row> expectedRecords =
        Lists.newArrayList(Row.of(1, "iceberg", 10.0), Row.of(2, "b", 20.0), Row.of(3, null, 30.0));
    assertSameElements(expectedRecords, result);
    assertThat(lastScanEvent.filter())
        .as("Should not push down a filter")
        .isEqualTo(Expressions.alwaysTrue());
  }

  @Test
  public void testSqlParseNaN() {
    // todo add some test case to test NaN
  }
}
