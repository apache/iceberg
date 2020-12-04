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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public final class CustomCatalogs {
  private static final Cache<String, Catalog> CATALOG_CACHE = Caffeine.newBuilder().build();

  public static final String ICEBERG_CATALOG_PREFIX = "spark.sql.catalog.iceberg.";
  public static final String ICEBERG_CATALOG_TYPE = "type";
  public static final String ICEBERG_CATALOG_TYPE_HADOOP = "hadoop";
  public static final String ICEBERG_CATALOG_TYPE_HIVE = "hive";

  private CustomCatalogs() {
  }

  /**
   * Build an Iceberg {@link Catalog} to be used by this Spark source adapter.
   *
   * The cache is required to ensure the catalog isn't garbage collected and related resources closed before the
   * catalog's resources are finished being used. This tends to occur when HiveCatalog is finalized but the hive client
   * pool is still being used by child HiveTableOperations. The cache layer should be removed once Hive catalogs
   * keep track of their resources correctly.
   *
   * @param options options from Spark
   * @return an Iceberg catalog
   */
  public static Catalog buildIcebergCatalog(Map<String, String> options) {
    String cacheKey = options.entrySet()
        .stream().map(x -> String.format("%s:%s", x.getKey(), x.getValue())).collect(Collectors.joining(";"));
    Catalog catalog = CATALOG_CACHE.get(cacheKey, x -> buildIcebergCatalogImpl(options));
    return catalog;
  }

  static void clearCache() {
    CATALOG_CACHE.invalidateAll();
  }

  private static Catalog buildIcebergCatalogImpl(Map<String, String> options) {
    String name = "spark_source";
    SparkConf sparkConf = SparkSession.active().sparkContext().getConf();
    Map<String, String> sparkMap = Arrays.stream(sparkConf.getAllWithPrefix(ICEBERG_CATALOG_PREFIX))
        .collect(Collectors.toMap(x -> x._1, x -> x._2));
    sparkMap.putAll(options);
    Configuration conf = SparkSession.active().sessionState().newHadoopConf();

    String catalogImpl = sparkMap.get(CatalogProperties.CATALOG_IMPL);
    if (catalogImpl != null) {
      return CatalogUtil.loadCatalog(catalogImpl, name, sparkMap, conf);
    }

    String catalogType = sparkMap.getOrDefault(ICEBERG_CATALOG_TYPE, ICEBERG_CATALOG_TYPE_HIVE);
    switch (catalogType.toLowerCase(Locale.ENGLISH)) {
      case ICEBERG_CATALOG_TYPE_HIVE:
        int clientPoolSize = Integer.parseInt(sparkMap.getOrDefault(CatalogProperties.HIVE_CLIENT_POOL_SIZE,
            Integer.toString(CatalogProperties.HIVE_CLIENT_POOL_SIZE_DEFAULT)));
        String uri = options.get(CatalogProperties.HIVE_URI);
        return new HiveCatalog(name, uri, clientPoolSize, conf);

      case ICEBERG_CATALOG_TYPE_HADOOP:
        String warehouseLocation = sparkMap.get(CatalogProperties.WAREHOUSE_LOCATION);
        return new HadoopCatalog(name, conf, warehouseLocation, options);

      default:
        throw new UnsupportedOperationException("Unknown catalog type: " + catalogType);
    }
  }
}
