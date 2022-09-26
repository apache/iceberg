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
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.Spark3VersionUtil;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class TestRefreshTable extends SparkCatalogTestBase {

  public TestRefreshTable(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void createTables() {
    sql("CREATE TABLE %s (key int, value int) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1,1)", tableName);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testRefreshCommand() {
    Assume.assumeFalse(
        "Spark 3.0 Spark Session Catalog does not use V2 Catalogs so Iceberg refresh is impossible",
        Spark3VersionUtil.isSpark30() && catalogName.equals("spark_catalog"));

    // We are not allowed to change the session catalog after it has been initialized, so build a
    // new one
    if (catalogName.equals("spark_catalog")) {
      spark.conf().set("spark.sql.catalog." + catalogName + ".cache-enabled", true);
      spark = spark.cloneSession();
    }

    List<Object[]> originalExpected = ImmutableList.of(row(1, 1));
    List<Object[]> originalActual = sql("SELECT * FROM %s", tableName);
    assertEquals("Table should start as expected", originalExpected, originalActual);

    // Modify table outside of spark, it should be cached so Spark should see the same value after
    // mutation
    Table table = validationCatalog.loadTable(tableIdent);
    DataFile file = table.currentSnapshot().addedDataFiles(table.io()).iterator().next();
    table.newDelete().deleteFile(file).commit();

    List<Object[]> cachedActual = sql("SELECT * FROM %s", tableName);
    assertEquals("Cached table should be unchanged", originalExpected, cachedActual);

    // Refresh the Spark catalog, should be empty
    sql("REFRESH TABLE %s", tableName);
    List<Object[]> refreshedExpected = ImmutableList.of();
    List<Object[]> refreshedActual = sql("SELECT * FROM %s", tableName);
    assertEquals("Refreshed table should be empty", refreshedExpected, refreshedActual);
  }
}
