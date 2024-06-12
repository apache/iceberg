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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSparkWarehouseLocation extends SparkCatalogTestBase {

  private String warehouseLocation;
  private final String testNameSpace;
  private final String testTableName;
  private final TableIdentifier testTableIdentifier;

  public TestSparkWarehouseLocation(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    testNameSpace = (catalogName.equals("spark_catalog") ? "" : catalogName + ".") + "default1";
    testTableName = testNameSpace + ".table";
    testTableIdentifier = TableIdentifier.of("default1", "table");
  }

  @Before
  public void createTestWarehouseLocation() throws Exception {
    this.warehouseLocation = "file:" + temp.newFolder(catalogName).getPath();
  }

  @After
  public void dropTestTable() {
    sql("DROP TABLE IF EXISTS %s", testTableName);
    sql("DROP NAMESPACE IF EXISTS %s", testNameSpace);
    spark.sessionState().catalogManager().reset();
    SparkSession.clearDefaultSession();
  }

  @Test
  public void testCatalogSpecificWarehouseLocation() {
    spark.conf().set("spark.sql.catalog." + catalogName + ".warehouse", warehouseLocation);
    spark.sessionState().catalogManager().reset();
    sql("CREATE NAMESPACE %s", testNameSpace);
    sql("CREATE TABLE %s (id BIGINT NOT NULL, data STRING) USING iceberg", testTableName);

    Table table = Spark3Util.loadIcebergCatalog(spark, catalogName).loadTable(testTableIdentifier);
    String expectedPath =
        SparkCatalogConfig.SPARK.catalogName().equals(catalogName)
            ? spark.sqlContext().conf().warehousePath()
            : warehouseLocation;
    expectedPath +=
        SparkCatalogConfig.HADOOP.catalogName().equals(catalogName)
            ? "/default1/table"
            : "/default1.db/table";
    assertThat(table.location()).isEqualTo(expectedPath);
  }

  @Test
  public void testCatalogWithSparkSqlWarehouseDir() {
    spark.conf().unset("spark.sql.catalog." + catalogName + ".warehouse");
    spark.sessionState().catalogManager().reset();
    sql("CREATE NAMESPACE %s", testNameSpace);
    sql("CREATE TABLE %s (id BIGINT NOT NULL, data STRING) USING iceberg", testTableName);
    Table table = Spark3Util.loadIcebergCatalog(spark, catalogName).loadTable(testTableIdentifier);
    String expectedPath =
        SparkCatalogConfig.HADOOP.catalogName().equals(catalogName)
            ? spark.sqlContext().conf().warehousePath() + "/default1/table"
            : spark.sqlContext().conf().warehousePath() + "/default1.db/table";
    assertThat(table.location()).isEqualTo(expectedPath);
  }
}
