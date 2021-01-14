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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
    Assert.assertTrue("explain should contains LimitPushDown", explain.contains(expectedExplain));
    List<Object[]> result = sql(querySql);
    Assert.assertEquals("should have 1 record", 1, result.size());
    Assert.assertArrayEquals("Should produce the expected records", result.get(0), new Object[] {1, "a"});

    AssertHelpers.assertThrows("Invalid limit number: -1 ", SqlParserException.class,
        () -> sql("SELECT * FROM %s LIMIT -1", TABLE_NAME));

    Assert.assertEquals("should have 0 record", 0, sql("SELECT * FROM %s LIMIT 0", TABLE_NAME).size());

    String sqlLimitExceed = String.format("SELECT * FROM %s LIMIT 4", TABLE_NAME);
    List<Object[]> resultExceed = sql(sqlLimitExceed);
    Assert.assertEquals("should have 3 record", 3, resultExceed.size());
    List<Object[]> expectedList = Lists.newArrayList();
    expectedList.add(new Object[] {1, "a"});
    expectedList.add(new Object[] {2, "b"});
    expectedList.add(new Object[] {3, null});
    Assert.assertArrayEquals("Should produce the expected records", resultExceed.toArray(), expectedList.toArray());

    String sqlMixed = String.format("SELECT * FROM %s WHERE id = 1 LIMIT 2", TABLE_NAME);
    List<Object[]> mixedResult = sql(sqlMixed);
    Assert.assertEquals("should have 1 record", 1, mixedResult.size());
    Assert.assertArrayEquals("Should produce the expected records", mixedResult.get(0), new Object[] {1, "a"});
  }

  @Test
  public void testNoFilterPushDown() {
    String sql = String.format("SELECT * FROM %s ", TABLE_NAME);
    String explain = getTableEnv().explainSql(sql);
    Assert.assertFalse("explain should no contains FilterPushDown", explain.contains(expectedFilterPushDownExplain));
  }

  @Test
  public void testFilterPushDownEqual() {
    String sqlLiteralRight = String.format("SELECT * FROM %s WHERE id = 1 ", TABLE_NAME);
    String explain = getTableEnv().explainSql(sqlLiteralRight);
    String expectedFilter = "ref(name=\"id\") == 1";
    Assert.assertTrue("explain should contains the push down filter", explain.contains(expectedFilter));

    List<Object[]> result = sql(sqlLiteralRight);
    Assert.assertEquals("should have 1 record", 1, result.size());
    Assert.assertArrayEquals("Should produce the expected record", result.get(0), new Object[] {1, "a"});

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", lastScanEvent.filter().toString(), expectedFilter);

    // filter not push down
    String sqlEqualNull = String.format("SELECT * FROM %s WHERE data = NULL ", TABLE_NAME);
    String explainEqualNull = getTableEnv().explainSql(sqlEqualNull);
    Assert.assertFalse("explain should not contains FilterPushDown",
        explainEqualNull.contains(expectedFilterPushDownExplain));
  }

  @Test
  public void testFilterPushDownEqualLiteralOnLeft() {
    String sqlLiteralLeft = String.format("SELECT * FROM %s WHERE 1 = id ", TABLE_NAME);
    String explainLeft = getTableEnv().explainSql(sqlLiteralLeft);
    String expectedFilter = "ref(name=\"id\") == 1";
    Assert.assertTrue("explain should contains the push down filter", explainLeft.contains(expectedFilter));

    List<Object[]> resultLeft = sql(sqlLiteralLeft);
    Assert.assertEquals("should have 1 record", 1, resultLeft.size());
    Assert.assertArrayEquals("Should produce the expected record", resultLeft.get(0), new Object[] {1, "a"});

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", lastScanEvent.filter().toString(), expectedFilter);
  }

  @Test
  public void testFilterPushDownNoEqual() {
    String sqlNE = String.format("SELECT * FROM %s WHERE id <> 1 ", TABLE_NAME);
    String explainNE = getTableEnv().explainSql(sqlNE);
    String expectedFilter = "ref(name=\"id\") != 1";
    Assert.assertTrue("explain should contains the push down filter", explainNE.contains(expectedFilter));

    List<Object[]> resultNE = sql(sqlNE);
    Assert.assertEquals("should have 2 record", 2, resultNE.size());

    List<Object[]> expectedNE = Lists.newArrayList();
    expectedNE.add(new Object[] {2, "b"});
    expectedNE.add(new Object[] {3, null});
    Assert.assertArrayEquals("Should produce the expected record", resultNE.toArray(), expectedNE.toArray());
    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", lastScanEvent.filter().toString(), expectedFilter);

    String sqlNotEqualNull = String.format("SELECT * FROM %s WHERE data <> NULL ", TABLE_NAME);
    String explainNotEqualNull = getTableEnv().explainSql(sqlNotEqualNull);
    Assert.assertFalse("explain should not contains FilterPushDown", explainNotEqualNull.contains(
        expectedFilterPushDownExplain));
  }

  @Test
  public void testFilterPushDownAnd() {
    String sqlAnd = String.format("SELECT * FROM %s WHERE id = 1 AND data = 'a' ", TABLE_NAME);
    String explainAnd = getTableEnv().explainSql(sqlAnd);
    String expectedFilter = "ref(name=\"id\") == 1,ref(name=\"data\") == \"a\"";
    Assert.assertTrue("explain should contains the push down filter", explainAnd.contains(expectedFilter));

    List<Object[]> resultAnd = sql(sqlAnd);
    Assert.assertEquals("should have 1 record", 1, resultAnd.size());
    Assert.assertArrayEquals("Should produce the expected record", resultAnd.get(0), new Object[] {1, "a"});

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert
        .assertEquals("should contains the push down filter", "(ref(name=\"id\") == 1 and ref(name=\"data\") == \"a\")",
            lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownOr() {
    String sqlOr = String.format("SELECT * FROM %s WHERE id = 1 OR data = 'b' ", TABLE_NAME);
    String explainOr = getTableEnv().explainSql(sqlOr);
    String expectedFilter = "(ref(name=\"id\") == 1 or ref(name=\"data\") == \"b\")";
    Assert.assertTrue("explain should contains the push down filter", explainOr.contains(expectedFilter));

    List<Object[]> resultOr = sql(sqlOr);
    Assert.assertEquals("should have 2 record", 2, resultOr.size());

    List<Object[]> expectedOR = Lists.newArrayList();
    expectedOR.add(new Object[] {1, "a"});
    expectedOR.add(new Object[] {2, "b"});
    Assert.assertArrayEquals("Should produce the expected record", resultOr.toArray(), expectedOR.toArray());

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", lastScanEvent.filter().toString(), expectedFilter);
  }

  @Test
  public void testFilterPushDownGreaterThan() {
    String sqlGT = String.format("SELECT * FROM %s WHERE id > 1 ", TABLE_NAME);
    String explainGT = getTableEnv().explainSql(sqlGT);
    String expectedFilter = "ref(name=\"id\") > 1";
    Assert.assertTrue("explain should contains the push down filter", explainGT.contains(expectedFilter));

    List<Object[]> resultGT = sql(sqlGT);
    Assert.assertEquals("should have 2 record", 2, resultGT.size());

    List<Object[]> expectedGT = Lists.newArrayList();
    expectedGT.add(new Object[] {2, "b"});
    expectedGT.add(new Object[] {3, null});
    Assert.assertArrayEquals("Should produce the expected record", resultGT.toArray(), expectedGT.toArray());

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", lastScanEvent.filter().toString(), expectedFilter);
  }

  @Test
  public void testFilterPushDownGreaterThanLiteralOnLeft() {
    String sqlGT = String.format("SELECT * FROM %s WHERE 3 > id ", TABLE_NAME);
    String explainGT = getTableEnv().explainSql(sqlGT);
    String expectedFilter = "ref(name=\"id\") < 3";
    Assert.assertTrue("explain should contains the push down filter", explainGT.contains(expectedFilter));

    List<Object[]> resultGT = sql(sqlGT);
    Assert.assertEquals("should have 2 record", 2, resultGT.size());

    List<Object[]> expectedGT = Lists.newArrayList();
    expectedGT.add(new Object[] {1, "a"});
    expectedGT.add(new Object[] {2, "b"});
    Assert.assertArrayEquals("Should produce the expected record", resultGT.toArray(), expectedGT.toArray());

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", lastScanEvent.filter().toString(), expectedFilter);
  }

  @Test
  public void testFilterPushDownGreaterThanEqual() {
    String sqlGTE = String.format("SELECT * FROM %s WHERE id >= 2 ", TABLE_NAME);
    String explainGTE = getTableEnv().explainSql(sqlGTE);
    String expectedFilter = "ref(name=\"id\") >= 2";
    Assert.assertTrue("explain should contains the push down filter", explainGTE.contains(expectedFilter));

    List<Object[]> resultGTE = sql(sqlGTE);
    Assert.assertEquals("should have 2 records", 2, resultGTE.size());

    List<Object[]> expectedGTE = Lists.newArrayList();
    expectedGTE.add(new Object[] {2, "b"});
    expectedGTE.add(new Object[] {3, null});
    Assert.assertArrayEquals("Should produce the expected record", resultGTE.toArray(), expectedGTE.toArray());

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", lastScanEvent.filter().toString(), expectedFilter);
  }

  @Test
  public void testFilterPushDownGreaterThanEqualLiteralOnLeft() {
    String sqlGTE = String.format("SELECT * FROM %s WHERE 2 >= id ", TABLE_NAME);
    String explainGTE = getTableEnv().explainSql(sqlGTE);
    String expectedFilter = "ref(name=\"id\") <= 2";
    Assert.assertTrue("explain should contains the push down filter", explainGTE.contains(expectedFilter));

    List<Object[]> resultGTE = sql(sqlGTE);
    Assert.assertEquals("should have 2 records", 2, resultGTE.size());

    List<Object[]> expectedGTE = Lists.newArrayList();
    expectedGTE.add(new Object[] {1, "a"});
    expectedGTE.add(new Object[] {2, "b"});
    Assert.assertArrayEquals("Should produce the expected record", resultGTE.toArray(), expectedGTE.toArray());

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", lastScanEvent.filter().toString(), expectedFilter);
  }

  @Test
  public void testFilterPushDownLessThan() {
    String sqlLT = String.format("SELECT * FROM %s WHERE id < 2 ", TABLE_NAME);
    String explainLT = getTableEnv().explainSql(sqlLT);
    String expectedFilter = "ref(name=\"id\") < 2";
    Assert.assertTrue("explain should contains the push down filter", explainLT.contains(expectedFilter));

    List<Object[]> resultLT = sql(sqlLT);
    Assert.assertEquals("should have 1 record", 1, resultLT.size());
    Assert.assertArrayEquals("Should produce the expected record", resultLT.get(0), new Object[] {1, "a"});

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", lastScanEvent.filter().toString(), expectedFilter);
  }

  @Test
  public void testFilterPushDownLessThanLiteralOnLeft() {
    String sqlLT = String.format("SELECT * FROM %s WHERE 2 < id ", TABLE_NAME);
    String explainLT = getTableEnv().explainSql(sqlLT);
    String expectedFilter = "ref(name=\"id\") > 2";
    Assert.assertTrue("explain should contains the push down filter", explainLT.contains(expectedFilter));

    List<Object[]> resultLT = sql(sqlLT);
    Assert.assertEquals("should have 1 record", 1, resultLT.size());
    Assert.assertArrayEquals("Should produce the expected record", resultLT.get(0), new Object[] {3, null});

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", lastScanEvent.filter().toString(), expectedFilter);
  }

  @Test
  public void testFilterPushDownLessThanEqual() {
    String sqlLTE = String.format("SELECT * FROM %s WHERE id <= 1 ", TABLE_NAME);
    String explainLTE = getTableEnv().explainSql(sqlLTE);
    String expectedFilter = "ref(name=\"id\") <= 1";
    Assert.assertTrue("explain should contains the push down filter", explainLTE.contains(expectedFilter));

    List<Object[]> resultLTE = sql(sqlLTE);
    Assert.assertEquals("should have 1 record", 1, resultLTE.size());
    Assert.assertArrayEquals("Should produce the expected record", resultLTE.get(0), new Object[] {1, "a"});

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", lastScanEvent.filter().toString(), expectedFilter);
  }

  @Test
  public void testFilterPushDownLessThanEqualLiteralOnLeft() {
    String sqlLTE = String.format("SELECT * FROM %s WHERE 3 <= id  ", TABLE_NAME);
    String explainLTE = getTableEnv().explainSql(sqlLTE);
    String expectedFilter = "ref(name=\"id\") >= 3";
    Assert.assertTrue("explain should contains the push down filter", explainLTE.contains(expectedFilter));

    List<Object[]> resultLTE = sql(sqlLTE);
    Assert.assertEquals("should have 1 record", 1, resultLTE.size());
    Assert.assertArrayEquals("Should produce the expected record", resultLTE.get(0), new Object[] {3, null});

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", lastScanEvent.filter().toString(), expectedFilter);
  }

  @Test
  public void testFilterPushDownIn() {
    String sqlIN = String.format("SELECT * FROM %s WHERE id IN (1,2) ", TABLE_NAME);
    String explainIN = getTableEnv().explainSql(sqlIN);
    String expectedFilter = "(ref(name=\"id\") == 1 or ref(name=\"id\") == 2)";
    Assert.assertTrue("explain should contains the push down filter", explainIN.contains(expectedFilter));
    List<Object[]> resultIN = sql(sqlIN);
    Assert.assertEquals("should have 2 records", 2, resultIN.size());

    List<Object[]> expectedIN = Lists.newArrayList();
    expectedIN.add(new Object[] {1, "a"});
    expectedIN.add(new Object[] {2, "b"});
    Assert.assertArrayEquals("Should produce the expected record", resultIN.toArray(), expectedIN.toArray());
    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", lastScanEvent.filter().toString(), expectedFilter);

    // in with null will not push down
    String sqlInNull = String.format("SELECT * FROM %s WHERE id IN (1,2,NULL) ", TABLE_NAME);
    String explainInNull = getTableEnv().explainSql(sqlInNull);
    Assert.assertFalse("explain should not contains FilterPushDown",
        explainInNull.contains(expectedFilterPushDownExplain));
  }

  @Test
  public void testFilterPushDownNotIn() {
    String sqlNotIn = String.format("SELECT * FROM %s WHERE id NOT IN (3,2) ", TABLE_NAME);
    String explainNotIn = getTableEnv().explainSql(sqlNotIn);
    String expectedFilter = "ref(name=\"id\") != 3,ref(name=\"id\") != 2";
    Assert.assertTrue("explain should contains the push down filter", explainNotIn.contains(expectedFilter));

    List<Object[]> resultNotIn = sql(sqlNotIn);
    Assert.assertEquals("should have 1 record", 1, resultNotIn.size());
    Assert.assertArrayEquals("Should produce the expected record", resultNotIn.get(0), new Object[] {1, "a"});
    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", "(ref(name=\"id\") != 3 and ref(name=\"id\") != 2)",
        lastScanEvent.filter().toString());

    String sqlNotInNull = String.format("SELECT * FROM %s WHERE id NOT IN (1,2,NULL) ", TABLE_NAME);
    String explainNotInNull = getTableEnv().explainSql(sqlNotInNull);
    Assert.assertFalse("explain should not contains FilterPushDown",
        explainNotInNull.contains(expectedFilterPushDownExplain));
  }

  @Test
  public void testFilterPushDownIsNotNull() {
    String sqlNotNull = String.format("SELECT * FROM %s WHERE data IS NOT NULL", TABLE_NAME);
    String explainNotNull = getTableEnv().explainSql(sqlNotNull);
    String expectedFilter = "not_null(ref(name=\"data\"))";
    Assert.assertTrue("explain should contains the push down filter", explainNotNull.contains(expectedFilter));

    List<Object[]> resultNotNull = sql(sqlNotNull);
    Assert.assertEquals("should have 2 record", 2, resultNotNull.size());

    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {1, "a"});
    expected.add(new Object[] {2, "b"});
    Assert.assertArrayEquals("Should produce the expected record", resultNotNull.toArray(), expected.toArray());

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", lastScanEvent.filter().toString(), expectedFilter);
  }

  @Test
  public void testFilterPushDownIsNull() {
    String sqlNull = String.format("SELECT * FROM %s WHERE data IS  NULL", TABLE_NAME);
    String explainNull = getTableEnv().explainSql(sqlNull);
    String expectedFilter = "is_null(ref(name=\"data\"))";
    Assert.assertTrue("explain should contains the push down filter", explainNull.contains(expectedFilter));

    List<Object[]> resultNull = sql(sqlNull);
    Assert.assertEquals("should have 1 record", 1, resultNull.size());
    Assert.assertArrayEquals("Should produce the expected record", resultNull.get(0), new Object[] {3, null});

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", lastScanEvent.filter().toString(), expectedFilter);
  }

  @Test
  public void testFilterPushDownNot() {
    String sqlNot = String.format("SELECT * FROM %s WHERE NOT id = 1 ", TABLE_NAME);
    String explainNot = getTableEnv().explainSql(sqlNot);
    String expectedFilter = "ref(name=\"id\") != 1";
    Assert.assertTrue("explain should contains the push down filter", explainNot.contains(expectedFilter));

    List<Object[]> resultNot = sql(sqlNot);
    Assert.assertEquals("should have 2 record", 2, resultNot.size());

    List<Object[]> expectedNot = Lists.newArrayList();
    expectedNot.add(new Object[] {2, "b"});
    expectedNot.add(new Object[] {3, null});
    Assert.assertArrayEquals("Should produce the expected record", resultNot.toArray(), expectedNot.toArray());

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", lastScanEvent.filter().toString(), expectedFilter);
  }

  @Test
  public void testFilterPushDownBetween() {
    String sqlBetween = String.format("SELECT * FROM %s WHERE id BETWEEN 1 AND 2 ", TABLE_NAME);
    String explainBetween = getTableEnv().explainSql(sqlBetween);
    String expectedFilter = "ref(name=\"id\") >= 1,ref(name=\"id\") <= 2";
    Assert.assertTrue("explain should contains the push down filter", explainBetween.contains(expectedFilter));

    List<Object[]> resultBetween = sql(sqlBetween);
    Assert.assertEquals("should have 2 record", 2, resultBetween.size());

    List<Object[]> expectedBetween = Lists.newArrayList();
    expectedBetween.add(new Object[] {1, "a"});
    expectedBetween.add(new Object[] {2, "b"});
    Assert.assertArrayEquals("Should produce the expected record", resultBetween.toArray(), expectedBetween.toArray());

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", "(ref(name=\"id\") >= 1 and ref(name=\"id\") <= 2)",
        lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownNotBetween() {
    String sqlNotBetween = String.format("SELECT * FROM %s WHERE id  NOT BETWEEN 2 AND 3 ", TABLE_NAME);
    String explainNotBetween = getTableEnv().explainSql(sqlNotBetween);
    String expectedFilter = "(ref(name=\"id\") < 2 or ref(name=\"id\") > 3)";
    Assert.assertTrue("explain should contains the push down filter", explainNotBetween.contains(expectedFilter));

    List<Object[]> resultNotBetween = sql(sqlNotBetween);
    Assert.assertEquals("should have 1 record", 1, resultNotBetween.size());
    Assert.assertArrayEquals("the not between should produce the expected record", resultNotBetween.get(0),
        new Object[] {1, "a"});

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", lastScanEvent.filter().toString(), expectedFilter);
  }

  @Test
  public void testFilterPushDownLike() {
    String sqlLike = "SELECT * FROM " + TABLE_NAME + " WHERE data LIKE 'a%' ";
    String explainLike = getTableEnv().explainSql(sqlLike);
    String expectedFilter = "ref(name=\"data\") startsWith \"\"a\"\"";
    Assert
        .assertTrue("the like sql explain should contains the push down filter", explainLike.contains(expectedFilter));

    List<Object[]> resultLike = sql(sqlLike);
    Assert.assertEquals("should have 1 record", 1, resultLike.size());
    Assert.assertArrayEquals("the like result should produce the expected record", resultLike.get(0),
        new Object[] {1, "a"});
    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("should contains the push down filter", lastScanEvent.filter().toString(), expectedFilter);

    // not push down
    String sqlNoPushDown = "SELECT * FROM " + TABLE_NAME + " WHERE data LIKE '%a%' ";
    String explainNoPushDown = getTableEnv().explainSql(sqlNoPushDown);
    Assert.assertFalse("explain should not contains FilterPushDown",
        explainNoPushDown.contains(expectedFilterPushDownExplain));
  }

  @Test
  public void testFilterPushDown2Literal() {
    String sql2Literal = String.format("SELECT * FROM %s WHERE 1 > 0 ", TABLE_NAME);
    String explain2Literal = getTableEnv().explainSql(sql2Literal);
    Assert.assertFalse("explain should not contains FilterPushDown",
        explain2Literal.contains(expectedFilterPushDownExplain));
  }
}
