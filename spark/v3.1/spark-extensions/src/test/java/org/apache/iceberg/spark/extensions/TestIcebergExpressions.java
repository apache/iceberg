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
package org.apache.iceberg.spark.extensions;

import java.math.BigDecimal;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.IcebergBucketTransform;
import org.apache.spark.sql.catalyst.expressions.IcebergTruncateTransform;
import org.junit.After;
import org.junit.Test;

public class TestIcebergExpressions extends SparkExtensionsTestBase {

  public TestIcebergExpressions(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP VIEW IF EXISTS emp");
    sql("DROP VIEW IF EXISTS v");
  }

  @Test
  public void testTruncateExpressions() {
    sql(
        "CREATE TABLE %s ( "
            + "  int_c INT, long_c LONG, dec_c DECIMAL(4, 2), str_c STRING, binary_c BINARY "
            + ") USING iceberg",
        tableName);

    sql(
        "CREATE TEMPORARY VIEW emp "
            + "AS SELECT * FROM VALUES (101, 10001, 10.65, '101-Employee', CAST('1234' AS BINARY)) "
            + "AS EMP(int_c, long_c, dec_c, str_c, binary_c)");

    sql("INSERT INTO %s SELECT * FROM emp", tableName);

    Dataset<Row> df = spark.sql("SELECT * FROM " + tableName);
    df.select(
            new Column(new IcebergTruncateTransform(df.col("int_c").expr(), 2)).as("int_c"),
            new Column(new IcebergTruncateTransform(df.col("long_c").expr(), 2)).as("long_c"),
            new Column(new IcebergTruncateTransform(df.col("dec_c").expr(), 50)).as("dec_c"),
            new Column(new IcebergTruncateTransform(df.col("str_c").expr(), 2)).as("str_c"),
            new Column(new IcebergTruncateTransform(df.col("binary_c").expr(), 2)).as("binary_c"))
        .createOrReplaceTempView("v");

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(100, 10000L, new BigDecimal("10.50"), "10", "12")),
        sql("SELECT int_c, long_c, dec_c, str_c, CAST(binary_c AS STRING) FROM v"));
  }

  @Test
  public void testBucketExpressions() {
    sql(
        "CREATE TABLE %s ( "
            + "  int_c INT, long_c LONG, dec_c DECIMAL(4, 2), str_c STRING, binary_c BINARY "
            + ") USING iceberg",
        tableName);

    sql(
        "CREATE TEMPORARY VIEW emp "
            + "AS SELECT * FROM VALUES (101, 10001, 10.65, '101-Employee', CAST('1234' AS BINARY)) "
            + "AS EMP(int_c, long_c, dec_c, str_c, binary_c)");

    sql("INSERT INTO %s SELECT * FROM emp", tableName);

    Dataset<Row> df = spark.sql("SELECT * FROM " + tableName);
    df.select(
            new Column(new IcebergBucketTransform(2, df.col("int_c").expr())).as("int_c"),
            new Column(new IcebergBucketTransform(3, df.col("long_c").expr())).as("long_c"),
            new Column(new IcebergBucketTransform(4, df.col("dec_c").expr())).as("dec_c"),
            new Column(new IcebergBucketTransform(5, df.col("str_c").expr())).as("str_c"),
            new Column(new IcebergBucketTransform(6, df.col("binary_c").expr())).as("binary_c"))
        .createOrReplaceTempView("v");

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(0, 2, 0, 4, 1)),
        sql("SELECT int_c, long_c, dec_c, str_c, binary_c FROM v"));
  }
}
