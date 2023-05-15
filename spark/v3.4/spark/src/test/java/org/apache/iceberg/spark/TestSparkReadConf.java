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
package org.apache.iceberg.spark;

import static org.apache.iceberg.TableProperties.SPLIT_SIZE;

import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSparkReadConf extends SparkTestBaseWithCatalog {

  @Before
  public void before() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);
  }

  @After
  public void after() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testSparkReadConfSplitSizeDefault() {
    Table table = validationCatalog.loadTable(tableIdent);

    SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());

    Assert.assertEquals(TableProperties.SPLIT_SIZE_DEFAULT, readConf.splitSize());
    Assert.assertNull(readConf.splitSizeOption());
  }

  @Test
  public void testSparkReadConfSplitSizeWithWriteOption() {
    Table table = validationCatalog.loadTable(tableIdent);

    Map<String, String> readOptions = ImmutableMap.of(SparkReadOptions.SPLIT_SIZE, "1024");

    SparkReadConf readConf = new SparkReadConf(spark, table, readOptions);

    Assert.assertEquals(1024, readConf.splitSize());
    Assert.assertEquals(1024, readConf.splitSizeOption().longValue());
  }

  @Test
  public void testSparkReadConfSplitSizeWithSessionConfig() {
    withSQLConf(
        ImmutableMap.of(String.format(SparkSQLProperties.TEMPLATED_SPLIT_SIZE, tableIdent.toString()), "1024"),
        () -> {
          Table table = validationCatalog.loadTable(tableIdent);

          SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());

          Assert.assertEquals(1024, readConf.splitSize());
          Assert.assertEquals(1024, readConf.splitSizeOption().longValue());
        });
  }

  @Test
  public void testSparkReadConfSplitSizeWithTableProperties() {
    Table table = validationCatalog.loadTable(tableIdent);

    table.updateProperties().set(SPLIT_SIZE, "1024").commit();

    SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
    Assert.assertEquals(1024, readConf.splitSize());
    Assert.assertEquals(null, readConf.splitSizeOption());
  }

  @Test
  public void testSparkReadConfSplitSizeWithTblPropAndSessionConfig() {
    withSQLConf(
        ImmutableMap.of(String.format(SparkSQLProperties.TEMPLATED_SPLIT_SIZE, tableIdent.toString()), "2048"),
        () -> {
          Table table = validationCatalog.loadTable(tableIdent);

          table.updateProperties().set(SPLIT_SIZE, "1024").commit();

          SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
          // session config overwrite the table properties
          Assert.assertEquals(2048, readConf.splitSize());
          Assert.assertEquals(2048, readConf.splitSizeOption().longValue());
        });
  }

  @Test
  public void testSparkReadConfSplitSizeWithWriteOptionAndSessionConfig() {
    withSQLConf(
        ImmutableMap.of(String.format(SparkSQLProperties.TEMPLATED_SPLIT_SIZE, tableIdent.toString()), "1024"),
        () -> {
          Table table = validationCatalog.loadTable(tableIdent);

          Map<String, String> writeOptions = ImmutableMap.of(SparkReadOptions.SPLIT_SIZE, "2048");

          SparkReadConf readConf = new SparkReadConf(spark, table, writeOptions);
          // read options overwrite the session config
          Assert.assertEquals(2048, readConf.splitSize());
          Assert.assertEquals(2048, readConf.splitSizeOption().longValue());
        });
  }

  @Test
  public void testSparkReadConfSplitSizeWithEverything() {
    withSQLConf(
        ImmutableMap.of(String.format(SparkSQLProperties.TEMPLATED_SPLIT_SIZE, tableIdent.toString()), "1024"),
        () -> {
          Table table = validationCatalog.loadTable(tableIdent);

          Map<String, String> writeOptions = ImmutableMap.of(SparkReadOptions.SPLIT_SIZE, "2048");

          table.updateProperties().set(SPLIT_SIZE, "512").commit();

          SparkReadConf readConf = new SparkReadConf(spark, table, writeOptions);
          // read options take the highest priority
          Assert.assertEquals(2048, readConf.splitSize());
          Assert.assertEquals(2048, readConf.splitSizeOption().longValue());
        });
  }
}
