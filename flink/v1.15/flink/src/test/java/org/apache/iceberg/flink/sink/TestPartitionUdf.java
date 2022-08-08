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
package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestPartitionUdf {

  public static TableEnvironment tEnv;

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void before() throws IOException {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().getConfiguration().set(TableConfigOptions.LOCAL_TIME_ZONE, "Asia/Shanghai");
    tEnv.executeSql(
        String.format(
            "CREATE CATALOG %s "
                + "WITH "
                + "('type'='iceberg', "
                + "'catalog-type'='hadoop', "
                + "'warehouse'='file://%s')",
            "iceberg_catalog", TEMPORARY_FOLDER.newFolder()));
  }

  protected List<Row> sql(String query, Object... args) {
    TableResult tableResult = tEnv.executeSql(String.format(query, args));
    try (CloseableIterator<Row> it = tableResult.collect()) {
      return Lists.newArrayList(it);
    } catch (Exception e) {
      throw new RuntimeException("Failed to collect table result", e);
    }
  }

  public Object getSqlResult(String query, Object... args) {
    List<Row> ret = sql(query, args);
    return ret.get(0).getField(0);
  }

  public List<String> getFunctionList() {
    List<Row> functions = sql("show functions");
    return functions.stream().map(e -> (String) e.getField(0)).collect(Collectors.toList());
  }

  @Test
  public void testFunctionWithDefaultCatalog() {

    functionWithDefaultCatalog("buckets", PartitionTransformUdf.Bucket.class, this::bucketCase);

    functionWithDefaultCatalog(
        "truncates", PartitionTransformUdf.Truncate.class, this::truncateCase);
  }

  @Test
  public void testFunctionWithIcebergCatalog() {
    bucketCase("`iceberg_catalog`.`system`.`buckets`");
    truncateCase("`iceberg_catalog`.`system`.`truncates`");
  }

  public void functionWithDefaultCatalog(
      String funcName,
      Class<? extends UserDefinedFunction> funcClass,
      Consumer<String> functionCase) {

    tEnv.executeSql("use default_catalog.default_database");

    List<String> functions = getFunctionList();
    Assert.assertFalse(functions.contains(funcName));

    // add bucket function
    tEnv.createTemporarySystemFunction(funcName, funcClass);

    List<String> functions2 = getFunctionList();
    Assert.assertEquals(functions.size() + 1, functions2.size());
    Assert.assertTrue(functions2.contains(funcName));

    functionCase.accept(funcName);

    // delete bucket function
    tEnv.dropTemporarySystemFunction(funcName);

    List<String> functions3 = getFunctionList();
    Assert.assertEquals(functions.size(), functions3.size());
    Assert.assertFalse(functions3.contains(funcName));
  }

  public void bucketCase(String funcName) {
    // int type
    Assert.assertEquals(428, getSqlResult("SELECT %s(1000, 10)", funcName));

    // long type
    Assert.assertEquals(525, getSqlResult("SELECT %s(1000, 456294967296)", funcName));

    // date type
    Assert.assertEquals(51, getSqlResult("SELECT %s(1000, DATE '2022-05-20')", funcName));

    // timestamp_ltz type
    Assert.assertEquals(
        483,
        getSqlResult(
            "select %s(1000, ts) from "
                + "(select cast(TIMESTAMP '2022-05-20 10:12:55.038194' as timestamp_ltz(6)) as ts)",
            funcName));

    // timestamp type
    Assert.assertEquals(
        441,
        getSqlResult(
            "select %s(1000, ts) from "
                + "(select cast(TIMESTAMP '2022-05-20 10:12:55.038194' as timestamp(6)) as ts)",
            funcName));

    // time type
    Assert.assertEquals(440, getSqlResult("SELECT %s(1000, TIME '14:08:59')", funcName));

    // string type
    Assert.assertEquals(489, getSqlResult("SELECT %s(1000, 'this is a string')", funcName));

    // decimal type
    Assert.assertEquals(
        825, getSqlResult("select %s(1000, cast(6.12345 as decimal(6,5)))", funcName));

    // binary type
    Assert.assertEquals(798, getSqlResult("SELECT %s(1000, x'010203040506')", funcName));

    // boolean type, unsupported
    AssertHelpers.assertThrows(
        "unsupported boolean type",
        ValidationException.class,
        () -> sql("SELECT %s(1000, true)", funcName));
  }

  public void truncateCase(String funcName) {
    // int type
    Assert.assertEquals(10, getSqlResult("SELECT %s(10, 15)", funcName));

    // long type
    Assert.assertEquals(456294967000L, getSqlResult("SELECT %s(1000, 456294967296)", funcName));

    // string type
    Assert.assertEquals("this is a ", getSqlResult("SELECT %s(10, 'this is a string')", funcName));

    // decimal type
    Assert.assertEquals(
        BigDecimal.valueOf(612000, 5),
        getSqlResult("select %s(1000, cast(6.12345 as decimal(6, 5)) )", funcName));

    // binary type
    Assert.assertArrayEquals(
        new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
        (byte[]) getSqlResult("SELECT %s(10, x'0102030405060708090a0b0c0d0e0f')", funcName));

    // timestamp type, unsupported
    AssertHelpers.assertThrows(
        "unsupported timestamp type",
        ValidationException.class,
        () -> sql("SELECT %s(10, TIMESTAMP '2022-05-20 10:12:55.038194')", funcName));

    // date type, unsupported
    AssertHelpers.assertThrows(
        "unsupported date type",
        ValidationException.class,
        () -> sql("SELECT %s(10, DATE '2022-05-20')", funcName));

    // time type, unsupported
    AssertHelpers.assertThrows(
        "unsupported time type",
        ValidationException.class,
        () -> sql("SELECT %s(10, TIME '14:08:59')", funcName));

    // boolean type, unsupported
    AssertHelpers.assertThrows(
        "unsupported boolean type",
        ValidationException.class,
        () -> sql("SELECT %s(10, true)", funcName));
  }
}
