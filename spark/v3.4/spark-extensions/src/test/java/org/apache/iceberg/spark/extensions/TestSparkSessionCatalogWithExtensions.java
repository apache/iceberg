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

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetAddress;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestSparkSessionCatalogWithExtensions {

  protected static TestHiveMetastore metastore = null;
  protected static HiveConf hiveConf = null;
  protected static SparkSession spark = null;
  protected static JavaSparkContext sparkContext = null;
  protected static HiveCatalog catalog = null;

  @BeforeAll
  public static void startMetastoreAndSpark() {
    metastore = new TestHiveMetastore();
    metastore.start();

    hiveConf = metastore.hiveConf();

    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
            .enableHiveSupport()
            .getOrCreate();

    sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);

    try {
      catalog.createNamespace(Namespace.of("default"));
    } catch (AlreadyExistsException ignored) {
      // the default namespace already exists. ignore the create error
    }
  }

  @AfterAll
  public static void stopMetastoreAndSpark() throws Exception {
    catalog = null;
    if (metastore != null) {
      metastore.stop();
      metastore = null;
    }
    if (spark != null) {
      spark.stop();
      spark = null;
      sparkContext = null;
    }
  }

  public static void setUpCatalog() {
    spark
        .conf()
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");
  }

  public static void resetSparkCatalog() {
    spark.conf().unset("spark.sql.catalog.spark_catalog");
    spark.conf().unset("spark.sql.catalog.spark_catalog.type");
  }

  @Test
  public void testCreateViewIfNotExistsWithExistingHiveView() {
    String viewName = "default.existing_hive_view";

    resetSparkCatalog();
    try {
      // create Hive view
      spark.sql(String.format("CREATE VIEW %s AS SELECT 1 AS id", viewName));
    } finally {
      setUpCatalog();
    }

    try {
      spark.sql(String.format("CREATE VIEW IF NOT EXISTS %s AS SELECT 2 AS id", viewName));
    } finally {
      spark.sql(String.format("DROP VIEW IF EXISTS %s", viewName));
    }
  }

  @Test
  public void testCreateViewWithExistingHiveView() {
    String viewName = "default.collision_view";

    resetSparkCatalog();
    try {
      // create Hive view
      spark.sql(String.format("CREATE VIEW %s AS SELECT 1 AS id", viewName));
    } finally {
      setUpCatalog();
    }

    assertThatThrownBy(() -> spark.sql(String.format("CREATE VIEW %s AS SELECT 2 AS id", viewName)))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("VIEW_ALREADY_EXISTS");

    spark.sql(String.format("DROP VIEW IF EXISTS %s", viewName));
  }
}
