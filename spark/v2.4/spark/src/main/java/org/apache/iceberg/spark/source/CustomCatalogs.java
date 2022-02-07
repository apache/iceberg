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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public final class CustomCatalogs {
  private static final Cache<Pair<SparkSession, String>, Catalog> CATALOG_CACHE = Caffeine.newBuilder().build();

  public static final String ICEBERG_DEFAULT_CATALOG = "default_catalog";
  public static final String ICEBERG_CATALOG_PREFIX = "spark.sql.catalog";

  private CustomCatalogs() {
  }

  /**
   * Build an Iceberg {@link Catalog} to be used by this Spark source adapter.
   *
   * <p>
   * The cache is to facilitate reuse of catalogs, especially if wrapped in CachingCatalog. For non-Hive catalogs all
   * custom parameters passed to the catalog are considered in the cache key. Hive catalogs only cache based on
   * the Metastore URIs as per previous behaviour.
   *
   *
   * @param spark Spark Session
   * @param name Catalog Name
   * @return an Iceberg catalog
   */
  public static Catalog loadCatalog(SparkSession spark, String name) {
    return CATALOG_CACHE.get(Pair.of(spark, name), CustomCatalogs::buildCatalog);
  }

  private static Catalog buildCatalog(Pair<SparkSession, String> sparkAndName) {
    SparkSession spark = sparkAndName.first();
    String name = sparkAndName.second();
    SparkConf sparkConf = spark.sparkContext().getConf();
    Configuration conf = SparkUtil.hadoopConfCatalogOverrides(spark, name);

    String catalogPrefix = String.format("%s.%s", ICEBERG_CATALOG_PREFIX, name);
    if (!name.equals(ICEBERG_DEFAULT_CATALOG) &&
        !sparkConf.contains(catalogPrefix)) {
      // we return null if spark.sql.catalog.<name> is not the Spark Catalog
      // and we aren't looking for the default catalog
      return null;
    }

    Map<String, String> options = Arrays.stream(sparkConf.getAllWithPrefix(catalogPrefix + "."))
        .collect(Collectors.toMap(x -> x._1, x -> x._2));

    return CatalogUtil.buildIcebergCatalog(name, options, conf);
  }

  public static Table table(SparkSession spark, String path) {
    Pair<Catalog, TableIdentifier> catalogAndTableIdentifier = catalogAndIdentifier(spark, path);
    return catalogAndTableIdentifier.first().loadTable(catalogAndTableIdentifier.second());
  }

  private static Pair<Catalog, TableIdentifier> catalogAndIdentifier(SparkSession spark, String path) {
    String[] currentNamespace = new String[]{spark.catalog().currentDatabase()};
    List<String> nameParts = Splitter.on(".").splitToList(path);
    return SparkUtil.catalogAndIdentifier(nameParts,
        s -> loadCatalog(spark, s),
        (n, t) -> TableIdentifier.of(Namespace.of(n), t),
        loadCatalog(spark, ICEBERG_DEFAULT_CATALOG),
        currentNamespace);
  }

  @VisibleForTesting
  static void clearCache() {
    CATALOG_CACHE.invalidateAll();
  }
}
