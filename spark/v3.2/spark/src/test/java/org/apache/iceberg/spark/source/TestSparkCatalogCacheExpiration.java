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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.CatalogProperties.TABLE_CACHE_ENABLED;
import static org.apache.iceberg.CatalogProperties.TABLE_CACHE_EXPIRATION_ENABLED;

public class TestSparkCatalogCacheExpiration extends SparkCatalogTestBase {

  private String catalogConfigPrefix;
  private boolean isCacheEnabled;
  private boolean isCacheExpirationEnabled;

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
        { "testcacheexpirationnotenabled", SparkCatalog.class.getName(),
          ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default",
            TABLE_CACHE_ENABLED, "false",
            TABLE_CACHE_EXPIRATION_ENABLED, "false"
        ) },
        { "testhadoop", SparkCatalog.class.getName(),
           ImmutableMap.of(
            "type", "hadoop",
            TABLE_CACHE_ENABLED, "true",
            TABLE_CACHE_EXPIRATION_ENABLED, "false"
         ) }
    };
  }

  public TestSparkCatalogCacheExpiration(String catalogName,
                                         String implementation,
                                         Map<String, String> config) {
    super(catalogName, implementation, config);
    this.catalogConfigPrefix = "spark.sql.catalog." + catalogName + ".";
    this.isCacheEnabled = Boolean.parseBoolean(
        config.getOrDefault(TABLE_CACHE_ENABLED, "true"));
    this.isCacheExpirationEnabled = Boolean.parseBoolean(
        config.getOrDefault(TABLE_CACHE_EXPIRATION_ENABLED, "false"));
  }

  @Test
  public void testCachingSparkCatalogRespectsCacheExpirationEnabled() {
    boolean sparkConfHasCacheEnabled =
        Boolean.parseBoolean(spark.conf().get(catalogConfigPrefix + TABLE_CACHE_ENABLED));
    boolean sparkConfHasCacheExpirationEnabled =
        Boolean.parseBoolean(spark.conf().get(catalogConfigPrefix + TABLE_CACHE_EXPIRATION_ENABLED));

    Assert.assertEquals("The Spark conf should retain the value of cache-enabled",
        isCacheEnabled, sparkConfHasCacheEnabled);
    Assert.assertEquals("The Spark conf should retain the value of cache.expiration-enabled",
        isCacheExpirationEnabled, sparkConfHasCacheExpirationEnabled);

    SparkCatalog catalog = getSparkCatalog();

    Assert.assertEquals("The SparkCatalog should have caching enabled when configured with cache-enabled",
        isCacheEnabled, catalog.isCacheEnabled());

    Assert.assertEquals("The SparkCatalog should respect cache.expiration-enabled",
        isCacheExpirationEnabled, catalog.isCacheExpirationEnabled());
  }

  private SparkCatalog getSparkCatalog()  {
    TableCatalog catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    return (SparkCatalog) catalog;
  }
}
