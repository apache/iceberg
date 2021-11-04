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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestDropTable extends SparkCatalogTestBase {
  private Map<String, String> config = null;
  private String implementation = null;
  private SparkSession session = null;

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][]{
        {"testhive", CustomSparkCatalog.class.getName(),
            ImmutableMap.of(
                "type", "hive",
                "default-namespace", "default"
            )},
        {"testhadoop", CustomSparkCatalog.class.getName(),
            ImmutableMap.of(
                "type", "hadoop"
            )},
        {"spark_catalog", CustomSparkSessionCatalog.class.getName(),
            ImmutableMap.of(
                "type", "hive",
                "default-namespace", "default",
                "parquet-enabled", "true",
                "cache-enabled", "false" // Spark will delete tables using v1, leaving the cache out of sync
            )}
    };
  }

  public TestDropTable(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    this.config = config;
    this.implementation = implementation;
  }

  @Before
  public void createTable() {
    // Spark CatalogManager cached the loaded catalog, here we use new SparkSession to force it load the catalog again
    session = spark.newSession();
    session.conf().set("spark.sql.catalog." + catalogName, this.implementation);
    config.forEach((key, value) -> session.conf().set("spark.sql.catalog." + catalogName + "." + key, value));

    if (config.get("type").equalsIgnoreCase("hadoop")) {
      session.conf().set("spark.sql.catalog." + catalogName + ".warehouse", "file:" + warehouse);
    }
    sql(session, "CREATE NAMESPACE IF NOT EXISTS default");
    sql(session, "CREATE TABLE %s (id INT, name STRING) USING iceberg", tableName);
    sql(session, "INSERT INTO %s VALUES (1, 'test')", tableName);
  }

  @After
  public void removeTable() {
    if (validationCatalog.tableExists(tableIdent)) {
      validationCatalog.dropTable(tableIdent, true);
    }
    sql(session, "DROP TABLE IF EXISTS %s PURGE", tableName);
    session = null;
  }

  @Test
  public void testDropTable() {
    assertEquals("Should have expected rows",
        ImmutableList.of(row(1, "test")), sql(session, "SELECT * FROM %s", tableName));

    sql(session, "DROP TABLE %s", tableName);
    Assert.assertFalse("Table should not exist", validationCatalog.tableExists(tableIdent));

    Identifier identifier = Identifier.of(tableIdent.namespace().levels(), tableIdent.name());
    if (!"spark_catalog".equals(catalogName)) {
      CustomSparkCatalog catalog = (CustomSparkCatalog) session.sessionState().catalogManager().catalog(catalogName);
      Assert.assertTrue("dropTable should be called", catalog.isDropTableCalled());
      Assert.assertFalse("purgeTable should not be called", catalog.isPurgeTableCalled());
      Assert.assertEquals(identifier, catalog.droppedIdentifier());
    } else {
      CustomSparkSessionCatalog catalog =
          (CustomSparkSessionCatalog) session.sessionState().catalogManager().catalog(catalogName);
      Assert.assertTrue("dropTable should be called", catalog.isDropTableCalled());
      Assert.assertFalse("purgeTable should not be called", catalog.isPurgeTableCalled());
      Assert.assertEquals(identifier, catalog.droppedIdentifier());
    }
  }

  @Test
  public void testPurgeTable() {
    assertEquals("Should have expected rows",
        ImmutableList.of(row(1, "test")),
        sql(session, "SELECT * FROM %s", tableName));

    sql(session, "DROP TABLE %s PURGE", tableName);
    Assert.assertFalse("Table should not exist", validationCatalog.tableExists(tableIdent));

    Identifier identifier = Identifier.of(tableIdent.namespace().levels(), tableIdent.name());
    if (!"spark_catalog".equals(catalogName)) {
      CustomSparkCatalog catalog = (CustomSparkCatalog) session.sessionState().catalogManager().catalog(catalogName);
      Assert.assertTrue("purgeTable should be called", catalog.isPurgeTableCalled());
      Assert.assertFalse("dropTable should not be called", catalog.isDropTableCalled());
      Assert.assertEquals(identifier, catalog.droppedIdentifier());
    } else {
      CustomSparkSessionCatalog catalog =
          (CustomSparkSessionCatalog) session.sessionState().catalogManager().catalog(catalogName);
      Assert.assertTrue("purgeTable should be called", catalog.isPurgeTableCalled());
      Assert.assertFalse("dropTable should not be called", catalog.isDropTableCalled());
      Assert.assertEquals(identifier, catalog.droppedIdentifier());
    }
  }

  private List<Object[]> sql(SparkSession spark, String query, Object... args) {
    List<Row> rows = spark.sql(String.format(query, args)).collectAsList();
    if (rows.size() < 1) {
      return ImmutableList.of();
    }

    return rowsToJava(rows);
  }

  public static class CustomSparkSessionCatalog extends SparkSessionCatalog {
    private boolean dropTableCalled = false;
    private boolean purgeTableCalled = false;
    private Identifier identifier = null;

    public boolean isDropTableCalled() {
      return dropTableCalled;
    }

    public boolean isPurgeTableCalled() {
      return purgeTableCalled;
    }

    public Identifier droppedIdentifier() {
      return identifier;
    }

    @Override
    public boolean dropTable(Identifier ident) {
      dropTableCalled = true;
      identifier = ident;
      return super.dropTable(ident);
    }

    @Override
    public boolean purgeTable(Identifier ident) {
      purgeTableCalled = true;
      identifier = ident;
      return super.purgeTable(ident);
    }
  }

  public static class CustomSparkCatalog extends SparkCatalog {
    private boolean dropTableCalled = false;
    private boolean purgeTableCalled = false;
    private Identifier identifier = null;

    public boolean isDropTableCalled() {
      return dropTableCalled;
    }

    public boolean isPurgeTableCalled() {
      return purgeTableCalled;
    }

    public Identifier droppedIdentifier() {
      return identifier;
    }

    @Override
    public boolean dropTable(Identifier ident) {
      dropTableCalled = true;
      identifier = ident;
      return super.dropTable(ident);
    }

    @Override
    public boolean purgeTable(Identifier ident) {
      purgeTableCalled = true;
      identifier = ident;
      return super.purgeTable(ident);
    }
  }
}
