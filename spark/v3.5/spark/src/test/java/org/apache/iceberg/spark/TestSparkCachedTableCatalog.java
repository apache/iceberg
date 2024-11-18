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

import org.apache.iceberg.Parameters;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;

public class TestSparkCachedTableCatalog extends TestBaseWithCatalog {

  private static final SparkTableCache TABLE_CACHE = SparkTableCache.get();

  @BeforeAll
  public static void setupCachedTableCatalog() {
    spark.conf().set("spark.sql.catalog.testcache", SparkCachedTableCatalog.class.getName());
  }

  @AfterAll
  public static void unsetCachedTableCatalog() {
    spark.conf().unset("spark.sql.catalog.testcache");
  }

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties()
      },
    };
  }

  @TestTemplate
  public void testTimeTravel() {
    sql("CREATE TABLE %s (id INT, dep STRING) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);

    table.refresh();
    Snapshot firstSnapshot = table.currentSnapshot();
    waitUntilAfter(firstSnapshot.timestampMillis());

    sql("INSERT INTO TABLE %s VALUES (2, 'hr')", tableName);

    table.refresh();
    Snapshot secondSnapshot = table.currentSnapshot();
    waitUntilAfter(secondSnapshot.timestampMillis());

    sql("INSERT INTO TABLE %s VALUES (3, 'hr')", tableName);

    table.refresh();

    try {
      TABLE_CACHE.add("key", table);

      assertEquals(
          "Should have expected rows in 3rd snapshot",
          ImmutableList.of(row(1, "hr"), row(2, "hr"), row(3, "hr")),
          sql("SELECT * FROM testcache.key ORDER BY id"));

      assertEquals(
          "Should have expected rows in 2nd snapshot",
          ImmutableList.of(row(1, "hr"), row(2, "hr")),
          sql(
              "SELECT * FROM testcache.`key#at_timestamp_%s` ORDER BY id",
              secondSnapshot.timestampMillis()));

      assertEquals(
          "Should have expected rows in 1st snapshot",
          ImmutableList.of(row(1, "hr")),
          sql(
              "SELECT * FROM testcache.`key#snapshot_id_%d` ORDER BY id",
              firstSnapshot.snapshotId()));

    } finally {
      TABLE_CACHE.remove("key");
    }
  }
}
