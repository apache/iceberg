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

import java.util.Map;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSparkCatalogCacheExpiration extends SparkTestBaseWithCatalog {

  private static final String sessionCatalogName = "spark_catalog";
  private static final String sessionCatalogImpl = SparkSessionCatalog.class.getName();
  private static final Map<String, String> sessionCatalogConfig =
      ImmutableMap.of(
          "type",
          "hadoop",
          "default-namespace",
          "default",
          CatalogProperties.CACHE_ENABLED,
          "true",
          CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS,
          "3000");

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
  @BeforeClass
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

  public TestSparkCatalogCacheExpiration() {
    super(sessionCatalogName, sessionCatalogImpl, sessionCatalogConfig);
  }

  @Test
  public void testSparkSessionCatalogWithExpirationEnabled() {
    SparkSessionCatalog<?> sparkCatalog = sparkSessionCatalog();
    Assertions.assertThat(sparkCatalog)
        .extracting("icebergCatalog")
        .extracting("cacheEnabled")
        .isEqualTo(true);

    Assertions.assertThat(sparkCatalog)
        .extracting("icebergCatalog")
        .extracting("icebergCatalog")
        .isInstanceOfSatisfying(
            Catalog.class,
            icebergCatalog -> {
              Assertions.assertThat(icebergCatalog)
                  .isExactlyInstanceOf(CachingCatalog.class)
                  .extracting("expirationIntervalMillis")
                  .isEqualTo(3000L);
            });
  }

  @Test
  public void testCacheEnabledAndExpirationDisabled() {
    SparkCatalog sparkCatalog = getSparkCatalog("expiration_disabled");
    Assertions.assertThat(sparkCatalog).extracting("cacheEnabled").isEqualTo(true);

    Assertions.assertThat(sparkCatalog)
        .extracting("icebergCatalog")
        .isInstanceOfSatisfying(
            CachingCatalog.class,
            icebergCatalog -> {
              Assertions.assertThat(icebergCatalog)
                  .extracting("expirationIntervalMillis")
                  .isEqualTo(-1L);
            });
  }

  @Test
  public void testCacheDisabledImplicitly() {
    SparkCatalog sparkCatalog = getSparkCatalog("cache_disabled_implicitly");
    Assertions.assertThat(sparkCatalog).extracting("cacheEnabled").isEqualTo(false);

    Assertions.assertThat(sparkCatalog)
        .extracting("icebergCatalog")
        .isInstanceOfSatisfying(
            Catalog.class,
            icebergCatalog ->
                Assertions.assertThat(icebergCatalog).isNotInstanceOf(CachingCatalog.class));
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
