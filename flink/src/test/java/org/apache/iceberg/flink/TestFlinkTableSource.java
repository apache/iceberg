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

package org.apache.iceberg.flink;

import java.util.List;
import org.apache.flink.table.api.SqlParserException;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.ExpressionsUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestFlinkTableSource extends FlinkCatalogTestBase {
  private static final String TABLE_NAME = "test_table";

  private final String expectedFilterPushDownExplain = "FilterPushDown";
  private final FileFormat format;

  private int scanEventCount = 0;
  private ScanEvent lastScanEvent = null;

  @Parameterized.Parameters(name = "catalogName={0}, baseNamespace={1}, format={2}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format : new FileFormat[] {FileFormat.ORC, FileFormat.AVRO, FileFormat.PARQUET}) {
      for (Object[] catalogParams : FlinkCatalogTestBase.parameters()) {
        String catalogName = (String) catalogParams[0];
        Namespace baseNamespace = (Namespace) catalogParams[1];
        parameters.add(new Object[] {catalogName, baseNamespace, format});
      }
    }
    return parameters;
  }

  public TestFlinkTableSource(String catalogName, Namespace baseNamespace, FileFormat format) {
    super(catalogName, baseNamespace);
    this.format = format;

    // register a scan event listener to validate pushdown
    Listeners.register(event -> {
      scanEventCount += 1;
      lastScanEvent = event;
    }, ScanEvent.class);
  }

  @Before
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    sql("CREATE TABLE %s (id INT, data VARCHAR) WITH ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s  VALUES (1,'a'),(2,'b'),(3,CAST(NULL AS VARCHAR))", TABLE_NAME);

    this.scanEventCount = 0;
    this.lastScanEvent = null;
  }

  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  @Test
  public void testLimitPushDown() {
    String querySql = String.format("SELECT * FROM %s LIMIT 1", TABLE_NAME);
    String explain = getTableEnv().explainSql(querySql);
    String expectedExplain = "LimitPushDown : 1";
    assertTrue("explain should contains LimitPushDown", explain.contains(expectedExplain));
    List<Object[]> result = sql(querySql);
    assertEquals("should have 1 record", 1, result.size());
    assertArrayEquals("Should produce the expected records", result.get(0), new Object[] {1, "a"});

    AssertHelpers.assertThrows("Invalid limit number: -1 ", SqlParserException.class,
        () -> sql("SELECT * FROM %s LIMIT -1", TABLE_NAME));

    assertEquals("should have 0 record", 0, sql("SELECT * FROM %s LIMIT 0", TABLE_NAME).size());

    String sqlLimitExceed = String.format("SELECT * FROM %s LIMIT 4", TABLE_NAME);
    List<Object[]> resultExceed = sql(sqlLimitExceed);
    assertEquals("should have 3 record", 3, resultExceed.size());
    List<Object[]> expectedList = Lists.newArrayList();
    expectedList.add(new Object[] {1, "a"});
    expectedList.add(new Object[] {2, "b"});
    expectedList.add(new Object[] {3, null});
    assertArrayEquals("Should produce the expected records", resultExceed.toArray(), expectedList.toArray());

    String sqlMixed = String.format("SELECT * FROM %s WHERE id = 1 LIMIT 2", TABLE_NAME);
    List<Object[]> mixedResult = sql(sqlMixed);
    assertEquals("should have 1 record", 1, mixedResult.size());
    assertArrayEquals("Should produce the expected records", mixedResult.get(0), new Object[] {1, "a"});
  }

  @Test
  public void testNoFilterPushDown() {
    String sql = String.format("SELECT * FROM %s ", TABLE_NAME);
    String explain = getTableEnv().explainSql(sql);
    assertFalse("explain should no contains FilterPushDown", explain.contains(expectedFilterPushDownExplain));
  }

  @Test
  public void testFilterPushDownEqual() {
    String sqlLiteralRight = String.format("SELECT * FROM %s WHERE id = 1 ", TABLE_NAME);
    String explain = getTableEnv().explainSql(sqlLiteralRight);
    String explainExpected = "FilterPushDown,the filters :ref(name=\"id\") == 1]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explain.contains(explainExpected));

    List<Object[]> result = sql(sqlLiteralRight);
    assertEquals("should have 1 record", 1, result.size());
    assertArrayEquals("Should produce the expected record", result.get(0), new Object[] {1, "a"});

    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "id = 1", ExpressionsUtil.describe(lastScanEvent.filter()));

    // filter not push down
    String sqlEqualNull = String.format("SELECT * FROM %s WHERE data = NULL ", TABLE_NAME);
    String explainEqualNull = getTableEnv().explainSql(sqlEqualNull);
    assertFalse("explain should not contains FilterPushDown", explainEqualNull.contains(expectedFilterPushDownExplain));
  }

  @Test
  public void testFilterPushDownEqualLiteralOnLeft() {
    String sqlLiteralLeft = String.format("SELECT * FROM %s WHERE 1 = id ", TABLE_NAME);
    String explainLeft = getTableEnv().explainSql(sqlLiteralLeft);
    String explainExpected = "FilterPushDown,the filters :ref(name=\"id\") == 1]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainLeft.contains(explainExpected));

    List<Object[]> resultLeft = sql(sqlLiteralLeft);
    assertEquals("should have 1 record", 1, resultLeft.size());
    assertArrayEquals("Should produce the expected record", resultLeft.get(0), new Object[] {1, "a"});

    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "id = 1", ExpressionsUtil.describe(lastScanEvent.filter()));
  }

  @Test
  public void testFilterPushDownNoEqual() {
    String sqlNE = String.format("SELECT * FROM %s WHERE id <> 1 ", TABLE_NAME);
    String explainNE = getTableEnv().explainSql(sqlNE);
    String explainExpected = "FilterPushDown,the filters :ref(name=\"id\") != 1]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainNE.contains(explainExpected));

    List<Object[]> resultNE = sql(sqlNE);
    assertEquals("should have 2 record", 2, resultNE.size());

    List<Object[]> expectedNE = Lists.newArrayList();
    expectedNE.add(new Object[] {2, "b"});
    expectedNE.add(new Object[] {3, null});
    assertArrayEquals("Should produce the expected record", resultNE.toArray(), expectedNE.toArray());
    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "id != 1", ExpressionsUtil.describe(lastScanEvent.filter()));

    String sqlNotEqualNull = String.format("SELECT * FROM %s WHERE data <> NULL ", TABLE_NAME);
    String explainNotEqualNull = getTableEnv().explainSql(sqlNotEqualNull);
    assertFalse("explain should not contains FilterPushDown", explainNotEqualNull.contains(
        expectedFilterPushDownExplain));
  }

  @Test
  public void testFilterPushDownAnd() {
    String sqlAnd = String.format("SELECT * FROM %s WHERE id = 1 AND data = 'a' ", TABLE_NAME);
    String explainAnd = getTableEnv().explainSql(sqlAnd);
    String explainExpected =
        "FilterPushDown,the filters :ref(name=\"id\") == 1,ref(name=\"data\") == \"a\"]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainAnd.contains(explainExpected));

    List<Object[]> resultAnd = sql(sqlAnd);
    assertEquals("should have 1 record", 1, resultAnd.size());
    assertArrayEquals("Should produce the expected record", resultAnd.get(0), new Object[] {1, "a"});

    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "(id = 1 AND data = 'a')",
        ExpressionsUtil.describe(lastScanEvent.filter()));
  }

  @Test
  public void testFilterPushDownOr() {
    String sqlOr = String.format("SELECT * FROM %s WHERE id = 1 OR data = 'b' ", TABLE_NAME);
    String explainOr = getTableEnv().explainSql(sqlOr);
    String explainExpected =
        "FilterPushDown,the filters :(ref(name=\"id\") == 1 or ref(name=\"data\") == \"b\")]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainOr.contains(explainExpected));

    List<Object[]> resultOr = sql(sqlOr);
    assertEquals("should have 2 record", 2, resultOr.size());

    List<Object[]> expectedOR = Lists.newArrayList();
    expectedOR.add(new Object[] {1, "a"});
    expectedOR.add(new Object[] {2, "b"});
    assertArrayEquals("Should produce the expected record", resultOr.toArray(), expectedOR.toArray());

    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "(id = 1 OR data = 'b')",
        ExpressionsUtil.describe(lastScanEvent.filter()));
  }

  @Test
  public void testFilterPushDownGreaterThan() {
    String sqlGT = String.format("SELECT * FROM %s WHERE id > 1 ", TABLE_NAME);
    String explainGT = getTableEnv().explainSql(sqlGT);
    String explainExpected = "FilterPushDown,the filters :ref(name=\"id\") > 1]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainGT.contains(explainExpected));

    List<Object[]> resultGT = sql(sqlGT);
    assertEquals("should have 2 record", 2, resultGT.size());

    List<Object[]> expectedGT = Lists.newArrayList();
    expectedGT.add(new Object[] {2, "b"});
    expectedGT.add(new Object[] {3, null});
    assertArrayEquals("Should produce the expected record", resultGT.toArray(), expectedGT.toArray());

    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "id > 1", ExpressionsUtil.describe(lastScanEvent.filter()));
  }

  @Test
  public void testFilterPushDownGreaterThanLiteralOnLeft() {
    String sqlGT = String.format("SELECT * FROM %s WHERE 3 > id ", TABLE_NAME);
    String explainGT = getTableEnv().explainSql(sqlGT);
    String explainExpected = "FilterPushDown,the filters :ref(name=\"id\") < 3]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainGT.contains(explainExpected));

    List<Object[]> resultGT = sql(sqlGT);
    assertEquals("should have 2 record", 2, resultGT.size());

    List<Object[]> expectedGT = Lists.newArrayList();
    expectedGT.add(new Object[] {1, "a"});
    expectedGT.add(new Object[] {2, "b"});
    assertArrayEquals("Should produce the expected record", resultGT.toArray(), expectedGT.toArray());

    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "id < 3", ExpressionsUtil.describe(lastScanEvent.filter()));
  }

  @Test
  public void testFilterPushDownGreaterThanEqual() {
    String sqlGTE = String.format("SELECT * FROM %s WHERE id >= 2 ", TABLE_NAME);
    String explainGTE = getTableEnv().explainSql(sqlGTE);
    String explainExpected = "FilterPushDown,the filters :ref(name=\"id\") >= 2]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainGTE.contains(explainExpected));

    List<Object[]> resultGTE = sql(sqlGTE);
    assertEquals("should have 2 records", 2, resultGTE.size());

    List<Object[]> expectedGTE = Lists.newArrayList();
    expectedGTE.add(new Object[] {2, "b"});
    expectedGTE.add(new Object[] {3, null});
    assertArrayEquals("Should produce the expected record", resultGTE.toArray(), expectedGTE.toArray());

    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "id >= 2", ExpressionsUtil.describe(lastScanEvent.filter()));
  }

  @Test
  public void testFilterPushDownGreaterThanEqualLiteralOnLeft() {
    String sqlGTE = String.format("SELECT * FROM %s WHERE 2 >= id ", TABLE_NAME);
    String explainGTE = getTableEnv().explainSql(sqlGTE);
    String explainExpected = "FilterPushDown,the filters :ref(name=\"id\") <= 2]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainGTE.contains(explainExpected));

    List<Object[]> resultGTE = sql(sqlGTE);
    assertEquals("should have 2 records", 2, resultGTE.size());

    List<Object[]> expectedGTE = Lists.newArrayList();
    expectedGTE.add(new Object[] {1, "a"});
    expectedGTE.add(new Object[] {2, "b"});
    assertArrayEquals("Should produce the expected record", resultGTE.toArray(), expectedGTE.toArray());

    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "id <= 2", ExpressionsUtil.describe(lastScanEvent.filter()));
  }

  @Test
  public void testFilterPushDownLessThan() {
    String sqlLT = String.format("SELECT * FROM %s WHERE id < 2 ", TABLE_NAME);
    String explainLT = getTableEnv().explainSql(sqlLT);
    String explainExpected = "FilterPushDown,the filters :ref(name=\"id\") < 2]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainLT.contains(explainExpected));

    List<Object[]> resultLT = sql(sqlLT);
    assertEquals("should have 1 record", 1, resultLT.size());
    assertArrayEquals("Should produce the expected record", resultLT.get(0), new Object[] {1, "a"});

    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "id < 2", ExpressionsUtil.describe(lastScanEvent.filter()));
  }

  @Test
  public void testFilterPushDownLessThanLiteralOnLeft() {
    String sqlLT = String.format("SELECT * FROM %s WHERE 2 < id ", TABLE_NAME);
    String explainLT = getTableEnv().explainSql(sqlLT);
    String explainExpected = "FilterPushDown,the filters :ref(name=\"id\") > 2]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainLT.contains(explainExpected));

    List<Object[]> resultLT = sql(sqlLT);
    assertEquals("should have 1 record", 1, resultLT.size());
    assertArrayEquals("Should produce the expected record", resultLT.get(0), new Object[] {3, null});

    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "id > 2", ExpressionsUtil.describe(lastScanEvent.filter()));
  }

  @Test
  public void testFilterPushDownLessThanEqual() {
    String sqlLTE = String.format("SELECT * FROM %s WHERE id <= 1 ", TABLE_NAME);
    String explainLTE = getTableEnv().explainSql(sqlLTE);
    String explainExpected = "FilterPushDown,the filters :ref(name=\"id\") <= 1]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainLTE.contains(explainExpected));

    List<Object[]> resultLTE = sql(sqlLTE);
    assertEquals("should have 1 record", 1, resultLTE.size());
    assertArrayEquals("Should produce the expected record", resultLTE.get(0), new Object[] {1, "a"});

    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "id <= 1", ExpressionsUtil.describe(lastScanEvent.filter()));
  }

  @Test
  public void testFilterPushDownLessThanEqualLiteralOnLeft() {
    String sqlLTE = String.format("SELECT * FROM %s WHERE 3 <= id  ", TABLE_NAME);
    String explainLTE = getTableEnv().explainSql(sqlLTE);
    String explainExpected = "FilterPushDown,the filters :ref(name=\"id\") >= 3]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainLTE.contains(explainExpected));

    List<Object[]> resultLTE = sql(sqlLTE);
    assertEquals("should have 1 record", 1, resultLTE.size());
    assertArrayEquals("Should produce the expected record", resultLTE.get(0), new Object[] {3, null});

    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "id >= 3", ExpressionsUtil.describe(lastScanEvent.filter()));
  }

  @Test
  public void testFilterPushDownIn() {
    String sqlIN = String.format("SELECT * FROM %s WHERE id IN (1,2) ", TABLE_NAME);
    String explainIN = getTableEnv().explainSql(sqlIN);
    String explainExpected =
        "FilterPushDown,the filters :(ref(name=\"id\") == 1 or ref(name=\"id\") == 2)]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainIN.contains(explainExpected));
    List<Object[]> resultIN = sql(sqlIN);
    assertEquals("should have 2 records", 2, resultIN.size());

    List<Object[]> expectedIN = Lists.newArrayList();
    expectedIN.add(new Object[] {1, "a"});
    expectedIN.add(new Object[] {2, "b"});
    assertArrayEquals("Should produce the expected record", resultIN.toArray(), expectedIN.toArray());
    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "(id = 1 OR id = 2)",
        ExpressionsUtil.describe(lastScanEvent.filter()));

    // in with null will not push down
    String sqlInNull = String.format("SELECT * FROM %s WHERE id IN (1,2,NULL) ", TABLE_NAME);
    String explainInNull = getTableEnv().explainSql(sqlInNull);
    assertFalse("explain should not contains FilterPushDown", explainInNull.contains(expectedFilterPushDownExplain));
  }

  @Test
  public void testFilterPushDownNotIn() {
    String sqlNotIn = String.format("SELECT * FROM %s WHERE id NOT IN (3,2) ", TABLE_NAME);
    String explainNotIn = getTableEnv().explainSql(sqlNotIn);
    String explainExpected =
        "FilterPushDown,the filters :ref(name=\"id\") != 3,ref(name=\"id\") != 2]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainNotIn.contains(explainExpected));

    List<Object[]> resultNotIn = sql(sqlNotIn);
    assertEquals("should have 1 record", 1, resultNotIn.size());
    assertArrayEquals("Should produce the expected record", resultNotIn.get(0), new Object[] {1, "a"});
    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "(id != 3 AND id != 2)",
        ExpressionsUtil.describe(lastScanEvent.filter()));

    String sqlNotInNull = String.format("SELECT * FROM %s WHERE id NOT IN (1,2,NULL) ", TABLE_NAME);
    String explainNotInNull = getTableEnv().explainSql(sqlNotInNull);
    assertFalse("explain should not contains FilterPushDown", explainNotInNull.contains(expectedFilterPushDownExplain));
  }

  @Test
  public void testFilterPushDownIsNotNull() {
    String sqlNotNull = String.format("SELECT * FROM %s WHERE data IS NOT NULL", TABLE_NAME);
    String explainNotNull = getTableEnv().explainSql(sqlNotNull);
    String explainExpected = "FilterPushDown,the filters :not_null(ref(name=\"data\"))]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainNotNull.contains(explainExpected));

    List<Object[]> resultNotNull = sql(sqlNotNull);
    assertEquals("should have 2 record", 2, resultNotNull.size());

    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {1, "a"});
    expected.add(new Object[] {2, "b"});
    assertArrayEquals("Should produce the expected record", resultNotNull.toArray(), expected.toArray());

    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "data IS NOT NULL",
        ExpressionsUtil.describe(lastScanEvent.filter()));
  }

  @Test
  public void testFilterPushDownIsNull() {
    String sqlNull = String.format("SELECT * FROM %s WHERE data IS  NULL", TABLE_NAME);
    String explainNull = getTableEnv().explainSql(sqlNull);
    String explainExpected = "FilterPushDown,the filters :is_null(ref(name=\"data\"))]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainNull.contains(explainExpected));

    List<Object[]> resultNull = sql(sqlNull);
    assertEquals("should have 1 record", 1, resultNull.size());
    assertArrayEquals("Should produce the expected record", resultNull.get(0), new Object[] {3, null});

    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "data IS NULL", ExpressionsUtil.describe(lastScanEvent.filter()));
  }

  @Test
  public void testFilterPushDownNot() {
    String sqlNot = String.format("SELECT * FROM %s WHERE NOT id = 1 ", TABLE_NAME);
    String explainNot = getTableEnv().explainSql(sqlNot);
    String explainExpected = "FilterPushDown,the filters :ref(name=\"id\") != 1]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainNot.contains(explainExpected));

    List<Object[]> resultNot = sql(sqlNot);
    assertEquals("should have 2 record", 2, resultNot.size());

    List<Object[]> expectedNot = Lists.newArrayList();
    expectedNot.add(new Object[] {2, "b"});
    expectedNot.add(new Object[] {3, null});
    assertArrayEquals("Should produce the expected record", resultNot.toArray(), expectedNot.toArray());

    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "id != 1", ExpressionsUtil.describe(lastScanEvent.filter()));
  }

  @Test
  public void testFilterPushDownBetween() {
    String sqlBetween = String.format("SELECT * FROM %s WHERE id BETWEEN 1 AND 2 ", TABLE_NAME);
    String explainBetween = getTableEnv().explainSql(sqlBetween);
    String explainExpected =
        "FilterPushDown,the filters :ref(name=\"id\") >= 1,ref(name=\"id\") <= 2]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainBetween.contains(explainExpected));

    List<Object[]> resultBetween = sql(sqlBetween);
    assertEquals("should have 2 record", 2, resultBetween.size());

    List<Object[]> expectedBetween = Lists.newArrayList();
    expectedBetween.add(new Object[] {1, "a"});
    expectedBetween.add(new Object[] {2, "b"});
    assertArrayEquals("Should produce the expected record", resultBetween.toArray(), expectedBetween.toArray());

    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "(id >= 1 AND id <= 2)",
        ExpressionsUtil.describe(lastScanEvent.filter()));
  }

  @Test
  public void testFilterPushDownNotBetween() {
    String sqlNotBetween = String.format("SELECT * FROM %s WHERE id  NOT BETWEEN 2 AND 3 ", TABLE_NAME);
    String explainNotBetween = getTableEnv().explainSql(sqlNotBetween);
    String explainExpected =
        "FilterPushDown,the filters :(ref(name=\"id\") < 2 or ref(name=\"id\") > 3)]]], fields=[id, data])";
    assertTrue("explain should contains FilterPushDown", explainNotBetween.contains(explainExpected));

    List<Object[]> resultNotBetween = sql(sqlNotBetween);
    assertEquals("should have 1 record", 1, resultNotBetween.size());
    assertArrayEquals("the not between should produce the expected record", resultNotBetween.get(0),
        new Object[] {1, "a"});

    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "(id < 2 OR id > 3)",
        ExpressionsUtil.describe(lastScanEvent.filter()));
  }

  @Test
  public void testFilterPushDownLike() {
    String sqlLike = "SELECT * FROM " + TABLE_NAME + " WHERE data LIKE 'a%' ";
    String explainLike = getTableEnv().explainSql(sqlLike);
    String explainExpected =
        "FilterPushDown,the filters :ref(name=\"data\") startsWith \"\"a\"\"]]], fields=[id, data])";
    assertTrue("the like sql explain should contains FilterPushDown", explainLike.contains(explainExpected));

    List<Object[]> resultLike = sql(sqlLike);
    assertEquals("should have 1 record", 1, resultLike.size());
    assertArrayEquals("the like result should produce the expected record", resultLike.get(0), new Object[] {1, "a"});
    assertEquals("Should create only one scan", 1, scanEventCount);
    assertEquals("Should push down expected filter", "data LIKE '\"a\"%'",
        ExpressionsUtil.describe(lastScanEvent.filter()));

    // not push down
    String sqlNoPushDown = "SELECT * FROM " + TABLE_NAME + " WHERE data LIKE '%a%' ";
    String explainNoPushDown = getTableEnv().explainSql(sqlNoPushDown);
    assertFalse("explain should not contains FilterPushDown",
        explainNoPushDown.contains(expectedFilterPushDownExplain));
  }

  @Test
  public void testFilterPushDown2Literal() {
    String sql2Literal = String.format("SELECT * FROM %s WHERE 1 > 0 ", TABLE_NAME);
    String explain2Literal = getTableEnv().explainSql(sql2Literal);
    assertFalse("explain should not contains FilterPushDown", explain2Literal.contains(expectedFilterPushDownExplain));
  }
}
