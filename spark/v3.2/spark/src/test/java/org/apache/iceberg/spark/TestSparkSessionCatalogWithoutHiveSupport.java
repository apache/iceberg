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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.Assert;
import org.junit.Test;

public class TestSparkSessionCatalogWithoutHiveSupport {
  @Test
  public void testCheckHiveSupport() {
    try (SparkSession sparkWithoutHiveSupport =
        SparkSession.builder()
            .master("local[2]")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .getOrCreate()) {
      IllegalArgumentException exception =
          Assert.assertThrows(
              IllegalArgumentException.class,
              () -> sparkWithoutHiveSupport.sessionState().catalogManager().v2SessionCatalog());
      String errorMessage =
          String.format(
              "Please enable hive support for Spark"
                  + "by calling enableHiveSupport() or setting '%s=hive'. "
                  + "Using Spark's built-in %s catalog with Iceberg's hive catalog "
                  + "might result in inconsistent behavior of SparkSessionCatalog. ",
              StaticSQLConf.CATALOG_IMPLEMENTATION().key(),
              StaticSQLConf.CATALOG_IMPLEMENTATION().defaultValue().get());
      Assert.assertEquals(errorMessage, exception.getMessage());
    }
  }
}
