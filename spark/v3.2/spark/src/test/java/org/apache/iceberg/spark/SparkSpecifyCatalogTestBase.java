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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public abstract class SparkSpecifyCatalogTestBase extends SparkTestBase {
  private static File warehouse = null;

  @BeforeClass
  public static void createWarehouse() throws IOException {
    SparkSpecifyCatalogTestBase.warehouse = File.createTempFile("warehouse", null);
    Assert.assertTrue(warehouse.delete());
  }

  @AfterClass
  public static void dropWarehouse() {
    if (warehouse != null && warehouse.exists()) {
      warehouse.delete();
    }
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  protected final String catalogName;
  protected final Catalog validationCatalog;
  protected final SupportsNamespaces validationNamespaceCatalog;
  protected final TableIdentifier tableIdent = TableIdentifier.of(Namespace.of("default"), "table");
  protected final String tableName;
  protected final Map<String, String> catalogConfig;
  protected final String implementation;

  public SparkSpecifyCatalogTestBase() {
    this(SparkCatalogType.TEST_HADOOP, null);
  }

  public SparkSpecifyCatalogTestBase(SparkCatalogType sparkCatalogType) {
    this(sparkCatalogType, null);
  }

  public SparkSpecifyCatalogTestBase(SparkCatalogType sparkCatalogType, Map<String, String> config) {
    this.implementation = sparkCatalogType.getImplementation();

    this.catalogConfig = new HashMap<>(sparkCatalogType.getConfig());
    if (config != null && !config.isEmpty()) {
      config.forEach((key, value) -> catalogConfig.merge(key, value, (oldValue, newValue) -> newValue));
    }

    this.catalogName = sparkCatalogType.getCatalogName();
    this.validationCatalog = catalogName.equals(SparkCatalogType.TEST_HADOOP.getCatalogName()) ?
        new HadoopCatalog(spark.sessionState().newHadoopConf(), "file:" + warehouse) :
        catalog;
    this.validationNamespaceCatalog = (SupportsNamespaces) validationCatalog;

    spark.conf().set("spark.sql.catalog." + catalogName, implementation);
    catalogConfig.forEach((key, value) -> spark.conf().set("spark.sql.catalog." + catalogName + "." + key, value));

    if (catalogConfig.get("type").equalsIgnoreCase("hadoop")) {
      spark.conf().set("spark.sql.catalog." + catalogName + ".warehouse", "file:" + warehouse);
    }

    this.tableName = (catalogName.equals(SparkCatalogType.SPARK_CATALOG.getCatalogName()) ? "" :
        catalogName + ".") + "default" + ".table";

    sql("CREATE NAMESPACE IF NOT EXISTS default");
  }

  protected String tableName(String name) {
    return (catalogName.equals("spark_catalog") ? "" : catalogName + ".") + "default." + name;
  }
}
