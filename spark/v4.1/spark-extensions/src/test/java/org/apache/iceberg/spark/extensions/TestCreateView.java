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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestCreateView extends ExtensionsTestBase {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.SPARK_SESSION.catalogName(),
        SparkCatalogConfig.SPARK_SESSION.implementation(),
        SparkCatalogConfig.SPARK_SESSION.properties()
      }
    };
  }

  @BeforeEach
  public void useIcebergCatalog() {
    spark
        .conf()
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");
    spark.sessionState().catalogManager().reset();
  }

  @AfterEach
  public void useHiveCatalog() {
    spark.conf().unset("spark.sql.catalog.spark_catalog");
    spark.conf().unset("spark.sql.catalog.spark_catalog.type");
    spark.sessionState().catalogManager().reset();
  }

  @TestTemplate
  public void testCreateViewIfNotExistsWithExistingHiveView() {
    String viewName = "default.existing_hive_view";

    useHiveCatalog();
    try {
      // create Hive view
      spark.sql(String.format("CREATE VIEW %s AS SELECT 1 AS id", viewName));
    } finally {
      useIcebergCatalog();
    }

    try {
      spark.sql(String.format("CREATE VIEW IF NOT EXISTS %s AS SELECT 2 AS id", viewName));
    } finally {
      spark.sql(String.format("DROP VIEW IF EXISTS %s", viewName));
    }
  }

  @TestTemplate
  public void testCreateViewWithExistingHiveView() {
    String viewName = "default.collision_view";

    useHiveCatalog();
    try {
      // create Hive view
      spark.sql(String.format("CREATE VIEW %s AS SELECT 1 AS id", viewName));
    } finally {
      useIcebergCatalog();
    }

    assertThatThrownBy(() -> spark.sql(String.format("CREATE VIEW %s AS SELECT 2 AS id", viewName)))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("VIEW_ALREADY_EXISTS");

    spark.sql(String.format("DROP VIEW IF EXISTS %s", viewName));
  }
}
