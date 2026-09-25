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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.CustomSparkTestBase;
import org.junit.jupiter.api.Test;

public class TestStaticCatalogConfigs extends CustomSparkTestBase {

  @Test
  public void sessionCatalogPicksUpDefaultDatabaseConfig() throws Exception {
    Map<String, String> overrides =
        ImmutableMap.of(
            "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog",
            "spark.sql.catalog.spark_catalog.defaultDatabase", "testDefaultDB");
    withCustomSpark(
        overrides,
        sparkSession -> {
          String[] foundDefaultNamespace =
              sparkSession.sessionState().catalogManager().v2SessionCatalog().defaultNamespace();

          assertThat(foundDefaultNamespace).containsExactly("testDefaultDB");
        });
  }

  @Test
  public void sparkCatalogPicksUpDefaultDatabaseConfig() throws Exception {
    Map<String, String> overrides =
        ImmutableMap.of(
            "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.spark_catalog.defaultDatabase", "testDefaultDB");
    withCustomSpark(
        overrides,
        sparkSession -> {
          String[] foundDefaultNamespace =
              sparkSession.sessionState().catalogManager().v2SessionCatalog().defaultNamespace();

          assertThat(foundDefaultNamespace).containsExactly("testDefaultDB");
        });
  }

  @Test
  void sparkCatalogStillPrefersDefaultNamespaceConfig() throws Exception {
    Map<String, String> overrides =
        ImmutableMap.of(
            "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.spark_catalog.defaultDatabase", "dbToBeOverridden",
            "spark.sql.catalog.spark_catalog.default-namespace", "testDefaultDB");
    withCustomSpark(
        overrides,
        sparkSession -> {
          String[] foundDefaultNamespace =
              sparkSession.sessionState().catalogManager().v2SessionCatalog().defaultNamespace();

          assertThat(foundDefaultNamespace).containsExactly("testDefaultDB");
        });
  }
}
