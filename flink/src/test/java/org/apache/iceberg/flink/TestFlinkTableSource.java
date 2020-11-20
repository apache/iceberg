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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestFlinkTableSource extends FlinkCatalogTestBase {
  private static final String TABLE_NAME = "test_table";

  private final FileFormat format;

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
  }

  @Before
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    sql("CREATE TABLE %s (id INT, data VARCHAR) WITH ('write.format.default'='%s')", TABLE_NAME, format.name());
  }

  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  @Test
  public void testLimitPushDown() {
    sql("INSERT INTO %s  VALUES (1,'a'),(2,'b')", TABLE_NAME);

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

    String sqlLimitExceed = String.format("SELECT * FROM %s LIMIT 3", TABLE_NAME);
    List<Object[]> resultExceed = sql(sqlLimitExceed);
    Assert.assertEquals("should have 2 record", 2, resultExceed.size());
    List<Object[]> expectedList = Lists.newArrayList();
    expectedList.add(new Object[] {1, "a"});
    expectedList.add(new Object[] {2, "b"});
    Assert.assertArrayEquals("Should produce the expected records", resultExceed.toArray(), expectedList.toArray());

    String sqlMixed = String.format("SELECT * FROM %s WHERE id = 1 LIMIT 2", TABLE_NAME);
    List<Object[]> mixedResult = sql(sqlMixed);
    Assert.assertEquals("should have 1 record", 1, mixedResult.size());
    Assert.assertArrayEquals("Should produce the expected records", mixedResult.get(0), new Object[] {1, "a"});
  }

  @Test
  public void testFilterPushDown() {
    sql("INSERT INTO %s  VALUES (1,'a'),(2,'b'),(3,CAST(null AS VARCHAR))", TABLE_NAME);

    // equal
    String sqlLiteralRight = String.format("SELECT * FROM %s WHERE id = 1 ", TABLE_NAME);
    String explain = getTableEnv().explainSql(sqlLiteralRight);
    String expectedExplain = "FilterPushDown";
    assertTrue("explain should contains FilterPushDown", explain.contains(expectedExplain));

    List<Object[]> result = sql(sqlLiteralRight);
    Assert.assertEquals("should have 1 record", 1, result.size());
    Assert.assertArrayEquals("Should produce the expected record", result.get(0), new Object[] {1, "a"});

    // equal Literal on Left
    String sqlLiteralLeft = String.format("SELECT * FROM %s WHERE 1 = id ", TABLE_NAME);
    String explainLeft = getTableEnv().explainSql(sqlLiteralLeft);
    assertTrue("explain should contains FilterPushDown", explainLeft.contains(expectedExplain));

    List<Object[]> resultLeft = sql(sqlLiteralLeft);
    Assert.assertEquals("should have 1 record", 1, resultLeft.size());
    Assert.assertArrayEquals("Should produce the expected record", resultLeft.get(0), new Object[] {1, "a"});

    // not equal
    String sqlNE = String.format("SELECT * FROM %s WHERE id <> 1 ", TABLE_NAME);
    String explainNE = getTableEnv().explainSql(sqlNE);
    assertTrue("explain should contains FilterPushDown", explainNE.contains(expectedExplain));

    List<Object[]> resultNE = sql(sqlNE);
    Assert.assertEquals("should have 2 record", 2, resultNE.size());
    List<Object[]> expectedNE = Lists.newArrayList();
    expectedNE.add(new Object[] {2, "b"});
    expectedNE.add(new Object[] {3, null});
    Assert.assertArrayEquals("Should produce the expected record", resultNE.toArray(), expectedNE.toArray());

    // and
    String sqlAnd = String.format("SELECT * FROM %s WHERE id = 1 AND data = 'a' ", TABLE_NAME);
    String explainAnd = getTableEnv().explainSql(sqlAnd);
    assertTrue("explain should contains FilterPushDown", explainAnd.contains(expectedExplain));

    List<Object[]> resultAnd = sql(sqlAnd);
    Assert.assertEquals("should have 1 record", 1, resultAnd.size());
    Assert.assertArrayEquals("Should produce the expected record", resultAnd.get(0), new Object[] {1, "a"});

    // or
    String sqlOr = String.format("SELECT * FROM %s WHERE id = 1 OR data = 'b' ", TABLE_NAME);
    String explainOr = getTableEnv().explainSql(sqlOr);
    assertTrue("explain should contains FilterPushDown", explainOr.contains(expectedExplain));

    List<Object[]> resultOr = sql(sqlOr);
    Assert.assertEquals("should have 2 record", 2, resultOr.size());
    List<Object[]> expectedOR = Lists.newArrayList();
    expectedOR.add(new Object[] {1, "a"});
    expectedOR.add(new Object[] {2, "b"});
    Assert.assertArrayEquals("Should produce the expected record", resultOr.toArray(), expectedOR.toArray());

    // GREATER_THAN
    String sqlGT = String.format("SELECT * FROM %s WHERE id > 1 ", TABLE_NAME);
    String explainGT = getTableEnv().explainSql(sqlGT);
    assertTrue("explain should contains FilterPushDown", explainGT.contains(expectedExplain));

    List<Object[]> resultGT = sql(sqlGT);
    Assert.assertEquals("should have 2 record", 2, resultGT.size());
    List<Object[]> expectedGT = Lists.newArrayList();
    expectedGT.add(new Object[] {2, "b"});
    expectedGT.add(new Object[] {3, null});
    Assert.assertArrayEquals("Should produce the expected record", resultGT.toArray(), expectedGT.toArray());

    // GREATER_THAN_EQUAL
    String sqlGTE = String.format("SELECT * FROM %s WHERE id >= 2 ", TABLE_NAME);
    String explainGTE = getTableEnv().explainSql(sqlGTE);
    assertTrue("explain should contains FilterPushDown", explainGTE.contains(expectedExplain));

    List<Object[]> resultGTE = sql(sqlGT);
    Assert.assertEquals("should have 2 records", 2, resultGTE.size());
    List<Object[]> expectedGTE = Lists.newArrayList();
    expectedGTE.add(new Object[] {2, "b"});
    expectedGTE.add(new Object[] {3, null});
    Assert.assertArrayEquals("Should produce the expected record", resultGTE.toArray(), expectedGTE.toArray());

    // less_than
    String sqlLT = String.format("SELECT * FROM %s WHERE id < 2 ", TABLE_NAME);
    String explainLT = getTableEnv().explainSql(sqlLT);
    assertTrue("explain should contains FilterPushDown", explainLT.contains(expectedExplain));

    List<Object[]> resultLT = sql(sqlLT);
    Assert.assertEquals("should have 1 record", 1, resultLT.size());
    Assert.assertArrayEquals("Should produce the expected record", resultLT.get(0), new Object[] {1, "a"});

    // less_than_equal
    String sqlLTE = String.format("SELECT * FROM %s WHERE id <= 1 ", TABLE_NAME);
    String explainLTE = getTableEnv().explainSql(sqlLTE);
    assertTrue("explain should contains FilterPushDown", explainLTE.contains(expectedExplain));

    List<Object[]> resultLTE = sql(sqlLTE);
    Assert.assertEquals("should have 1 record", 1, resultLTE.size());
    Assert.assertArrayEquals("Should produce the expected record", resultLTE.get(0), new Object[] {1, "a"});

    // IN
    String sqlIN = String.format("SELECT * FROM %s WHERE id IN (1,2) ", TABLE_NAME);
    String explainIN = getTableEnv().explainSql(sqlIN);
    assertTrue("explain should contains FilterPushDown", explainIN.contains(expectedExplain));

    List<Object[]> resultIN = sql(sqlIN);
    Assert.assertEquals("should have 2 records", 2, resultIN.size());
    List<Object[]> expectedIN = Lists.newArrayList();
    expectedIN.add(new Object[] {1, "a"});
    expectedIN.add(new Object[] {2, "b"});
    Assert.assertArrayEquals("Should produce the expected record", resultIN.toArray(), expectedIN.toArray());

    // is not null
    String sqlNotNull = String.format("SELECT * FROM %s WHERE data IS NOT NULL", TABLE_NAME);
    String explainNotNull = getTableEnv().explainSql(sqlNotNull);
    assertTrue("explain should contains FilterPushDown", explainNotNull.contains(expectedExplain));

    List<Object[]> resultNotNull = sql(sqlNotNull);
    Assert.assertEquals("should have 2 record", 2, resultNotNull.size());
    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {1, "a"});
    expected.add(new Object[] {2, "b"});
    Assert.assertArrayEquals("Should produce the expected record", resultNotNull.toArray(), expected.toArray());

    // is  null
    String sqlNull = String.format("SELECT * FROM %s WHERE data IS  NULL", TABLE_NAME);
    String explainNull = getTableEnv().explainSql(sqlNull);
    assertTrue("explain should contains FilterPushDown", explainNull.contains(expectedExplain));

    List<Object[]> resultNull = sql(sqlNull);
    Assert.assertEquals("should have 1 record", 1, resultNull.size());
    Assert.assertArrayEquals("Should produce the expected record", resultNull.get(0), new Object[] {3, null});

    // not
    String sqlNot = String.format("SELECT * FROM %s WHERE NOT id = 1 ", TABLE_NAME);
    String explainNot = getTableEnv().explainSql(sqlNot);
    assertTrue("explain should contains FilterPushDown", explainNot.contains(expectedExplain));

    List<Object[]> resultNot = sql(sqlNot);
    Assert.assertEquals("should have 2 record", 2, resultNot.size());
    List<Object[]> expectedNot = Lists.newArrayList();
    expectedNot.add(new Object[] {2, "b"});
    expectedNot.add(new Object[] {3, null});
    Assert.assertArrayEquals("Should produce the expected record", resultNot.toArray(), expectedNot.toArray());
  }
}
