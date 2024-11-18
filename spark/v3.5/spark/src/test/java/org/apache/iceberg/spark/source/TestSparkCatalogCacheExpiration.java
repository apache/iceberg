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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;

public class TestSparkCatalogCacheExpiration extends TestBaseWithCatalog {

  private static final Map<String, String> SESSION_CATALOG_CONFIG =
      ImmutableMap.of(
          "type",
          "hadoop",
          "default-namespace",
          "default",
          CatalogProperties.CACHE_ENABLED,
          "true",
          CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS,
          "3000");

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {"spark_catalog", SparkSessionCatalog.class.getName(), SESSION_CATALOG_CONFIG},
    };
  }

  private static String asSqlConfCatalogKeyFor(String catalog, String configKey) {
    // configKey is empty when the catalog's class is being defined
    if (configKey.isEmpty()) {
      return String.format("spark.sql.catalog.%s", catalog);
    } else {
      return String.format("spark.sql.catalog.%s.%s", catalog, configKey);
    }
  }

  // Add more catalogs to the spark session, so we only need to start spark one time for multiple
  // different catalog configuration tests.
  @BeforeAll
  public static void beforeClass() {
    // Catalog - expiration_disabled: Catalog with caching on and expiration disabled.
    ImmutableMap.of(
            "",
            "org.apache.iceberg.spark.SparkCatalog",
            "type",
            "hive",
            CatalogProperties.CACHE_ENABLED,
            "true",
            CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS,
            "-1")
        .forEach((k, v) -> spark.conf().set(asSqlConfCatalogKeyFor("expiration_disabled", k), v));

    // Catalog - cache_disabled_implicitly: Catalog that does not cache, as the cache expiration
    // interval is 0.
    ImmutableMap.of(
            "",
            "org.apache.iceberg.spark.SparkCatalog",
            "type",
            "hive",
            CatalogProperties.CACHE_ENABLED,
            "true",
            CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS,
            "0")
        .forEach(
            (k, v) -> spark.conf().set(asSqlConfCatalogKeyFor("cache_disabled_implicitly", k), v));
  }

  @TestTemplate
  public void testSparkSessionCatalogWithExpirationEnabled() {
    SparkSessionCatalog<?> sparkCatalog = sparkSessionCatalog();
    assertThat(sparkCatalog)
        .extracting("icebergCatalog")
        .extracting("cacheEnabled")
        .isEqualTo(true);

    assertThat(sparkCatalog)
        .extracting("icebergCatalog")
        .extracting("icebergCatalog")
        .isInstanceOfSatisfying(
            Catalog.class,
            icebergCatalog -> {
              assertThat(icebergCatalog)
                  .isExactlyInstanceOf(CachingCatalog.class)
                  .extracting("expirationIntervalMillis")
                  .isEqualTo(3000L);
            });
  }

  @TestTemplate
  public void testCacheEnabledAndExpirationDisabled() {
    SparkCatalog sparkCatalog = getSparkCatalog("expiration_disabled");
    assertThat(sparkCatalog).extracting("cacheEnabled").isEqualTo(true);

    assertThat(sparkCatalog)
        .extracting("icebergCatalog")
        .isInstanceOfSatisfying(
            CachingCatalog.class,
            icebergCatalog -> {
              assertThat(icebergCatalog).extracting("expirationIntervalMillis").isEqualTo(-1L);
            });
  }

  @TestTemplate
  public void testCacheDisabledImplicitly() {
    SparkCatalog sparkCatalog = getSparkCatalog("cache_disabled_implicitly");
    assertThat(sparkCatalog).extracting("cacheEnabled").isEqualTo(false);

    assertThat(sparkCatalog)
        .extracting("icebergCatalog")
        .isInstanceOfSatisfying(
            Catalog.class,
            icebergCatalog -> assertThat(icebergCatalog).isNotInstanceOf(CachingCatalog.class));
  }

  private SparkSessionCatalog<?> sparkSessionCatalog() {
    TableCatalog catalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog("spark_catalog");
    return (SparkSessionCatalog<?>) catalog;
  }

  private SparkCatalog getSparkCatalog(String catalog) {
    return (SparkCatalog) spark.sessionState().catalogManager().catalog(catalog);
  }
}
