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
import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestCachingTableWithMetaTableName extends CatalogTestBase {
  private static final String TABLE_NAME = "history";

  @Parameter(index = 3)
  private int cacheExpirationInterval;

  @Parameters(
      name = "catalogName = {0}, implementation = {1}, config = {2}, cacheExpirationInterval = {3}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.SPARK.catalogName(),
        SparkCatalogConfig.SPARK.implementation(),
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default",
            "parquet-enabled", "true",
            "cache-enabled", "true"),
        1
      },
      {
        SparkCatalogConfig.SPARK.catalogName(),
        SparkCatalogConfig.SPARK.implementation(),
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default",
            "parquet-enabled", "true",
            "cache-enabled", "true"),
        -1
      }
    };
  }

  @BeforeEach
  public void createTables() {
    spark
        .conf()
        .set(
            "spark.sql.catalog.spark_catalog.cache.expiration-interval-ms",
            cacheExpirationInterval);
    sql("CREATE TABLE %s (id bigint, data string, float float) USING iceberg", TABLE_NAME);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", TABLE_NAME);
  }

  @TestTemplate
  public void testLoadTableAfterCacheExpires() throws InterruptedException {
    Thread.sleep(10); // wait for cache to expire
    sql("INSERT INTO %s VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', float('NaN'))", TABLE_NAME);
    List<Object[]> expected =
        ImmutableList.of(row(1L, "a", 1.0F), row(2L, "b", 2.0F), row(3L, "c", Float.NaN));

    assertEquals("Should return all expected rows", expected, sql("SELECT * FROM %s", TABLE_NAME));
  }
}
