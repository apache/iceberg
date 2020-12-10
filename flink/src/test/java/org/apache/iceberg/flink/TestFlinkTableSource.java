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

  private final FileFormat format;

  @Parameterized.Parameters(name = "catalogName={0}, baseNamespace={1}, format={2}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format : new FileFormat[] {FileFormat.ORC, FileFormat.AVRO, FileFormat.PARQUET}) {
      for (Object[] catalogParams : FlinkCatalogTestBase.parameters()) {
        String catalogName = (String) catalogParams[0];
        String[] baseNamespace = (String[]) catalogParams[1];
        parameters.add(new Object[] {catalogName, baseNamespace, format});
      }
    }
    return parameters;
  }

  public TestFlinkTableSource(String catalogName, String[] baseNamespace, FileFormat format) {
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
    List expectedList = Lists.newArrayList();
    expectedList.add(new Object[] {1, "a"});
    expectedList.add(new Object[] {2, "b"});
    Assert.assertArrayEquals("Should produce the expected records", resultExceed.toArray(), expectedList.toArray());

    String sqlMixed = String.format("SELECT * FROM %s WHERE id = 1 LIMIT 2", TABLE_NAME);
    List<Object[]> mixedResult = sql(sqlMixed);
    Assert.assertEquals("should have 1 record", 1, mixedResult.size());
    Assert.assertArrayEquals("Should produce the expected records", mixedResult.get(0), new Object[] {1, "a"});
  }
}
