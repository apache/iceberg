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

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.PathIdentifier;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.Spark3Util.CatalogAndIdentifier;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkRewriteTableCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.SparkTableCache;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SessionConfigSupport;
import org.apache.spark.sql.connector.catalog.SupportsCatalogOptions;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Data source for reading and writing Iceberg tables using the "iceberg" format.
 *
 * <p>The `path` parameter provided by Spark is resolved in the following priority order:
 *
 * <ol>
 *   <li>Rewrite key - If `path` is a rewrite key, load a table from the rewrite catalog
 *   <li>Table location - If `path` contains "/", load a table at the specified location
 *   <li>Catalog identifier - Otherwise resolve `path` as an identifier per Spark rules
 * </ol>
 */
public class IcebergSource
    implements DataSourceRegister, SupportsCatalogOptions, SessionConfigSupport {
  private static final String DEFAULT_CATALOG_NAME = "default_iceberg";
  private static final String REWRITE_CATALOG_NAME = "default_rewrite_catalog";
  private static final String CATALOG_PREFIX = "spark.sql.catalog.";
  private static final String DEFAULT_CATALOG = CATALOG_PREFIX + DEFAULT_CATALOG_NAME;
  private static final String REWRITE_CATALOG = CATALOG_PREFIX + REWRITE_CATALOG_NAME;
  private static final String BRANCH_PREFIX = "branch_";
  private static final String[] EMPTY_NAMESPACE = new String[0];

  private static final SparkTableCache TABLE_CACHE = SparkTableCache.get();

  @Override
  public String shortName() {
    return "iceberg";
  }

  @Override
  public String keyPrefix() {
    return shortName();
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return null;
  }

  @Override
  public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
    return getTable(null, null, options).partitioning();
  }

  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }

  @Override
  public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> options) {
    return loadTable(new CaseInsensitiveStringMap(options));
  }

  private Table loadTable(CaseInsensitiveStringMap options) {
    CatalogAndIdentifier catalogAndIdent = catalogAndIdentifier(options);
    TableCatalog catalog = catalogAndIdent.tableCatalog();
    Identifier ident = catalogAndIdent.identifier();
    try {
      return catalog.loadTable(ident);
    } catch (NoSuchTableException e) {
      // TableProvider doesn't permit typed exception while loading tables,
      // so throw Iceberg NoSuchTableException because the Spark one is typed
      throw new org.apache.iceberg.exceptions.NoSuchTableException(
          e,
          "Cannot find table %s in catalog %s (%s)",
          ident,
          catalog.name(),
          catalog.getClass().getName());
    }
  }

  private CatalogAndIdentifier catalogAndIdentifier(CaseInsensitiveStringMap options) {
    Preconditions.checkArgument(
        options.containsKey(SparkReadOptions.PATH), "Cannot open table: path is not set");
    Spark3Util.validateNoLegacyTimeTravel(options);

    SparkSession spark = SparkSession.active();
    CatalogManager catalogManager = spark.sessionState().catalogManager();

    setupDefaultSparkCatalogs(spark);

    String path = options.get(SparkReadOptions.PATH);
    String branch = options.get(SparkReadOptions.BRANCH);
    String branchSelector = branch != null ? BRANCH_PREFIX + branch : null;

    // return rewrite catalog with path as group ID if table is staged for rewrite
    if (TABLE_CACHE.contains(path)) {
      return new CatalogAndIdentifier(
          catalogManager.catalog(REWRITE_CATALOG_NAME), Identifier.of(EMPTY_NAMESPACE, path));
    }

    // return default catalog and PathIdentifier with branch selector for a path
    if (path.contains("/")) {
      return new CatalogAndIdentifier(
          catalogManager.catalog(DEFAULT_CATALOG_NAME),
          new PathIdentifier(pathWithSelector(path, branchSelector)));
    }

    // treat path as an identifier and resolve it against the session config
    // if the catalog resolves to an unknown session catalog, use the default Iceberg catalog
    CatalogAndIdentifier catalogAndIdent = resolveIdentifier(spark, path);
    CatalogPlugin catalog = catalogAndIdent.catalog();
    Identifier ident = catalogAndIdent.identifier();
    return new CatalogAndIdentifier(
        isUnknownSessionCatalog(catalog) ? catalogManager.catalog(DEFAULT_CATALOG_NAME) : catalog,
        identifierWithSelector(ident, branchSelector));
  }

  private static CatalogAndIdentifier resolveIdentifier(SparkSession spark, String ident) {
    return Spark3Util.catalogAndIdentifier("identifier", spark, ident);
  }

  private static boolean isUnknownSessionCatalog(CatalogPlugin catalog) {
    return catalog.name().equals("spark_catalog") && !(catalog instanceof SparkSessionCatalog);
  }

  private String pathWithSelector(String path, String selector) {
    return selector == null ? path : path + "#" + selector;
  }

  private Identifier identifierWithSelector(Identifier ident, String selector) {
    if (selector == null) {
      return ident;
    } else {
      String[] namespace = ident.namespace();
      String[] ns = Arrays.copyOf(namespace, namespace.length + 1);
      ns[namespace.length] = ident.name();
      return Identifier.of(ns, selector);
    }
  }

  @Override
  public Identifier extractIdentifier(CaseInsensitiveStringMap options) {
    return catalogAndIdentifier(options).identifier();
  }

  @Override
  public String extractCatalog(CaseInsensitiveStringMap options) {
    return catalogAndIdentifier(options).catalog().name();
  }

  @Override
  public Optional<String> extractTimeTravelVersion(CaseInsensitiveStringMap options) {
    return Optional.ofNullable(options.get(SparkReadOptions.VERSION_AS_OF));
  }

  @Override
  public Optional<String> extractTimeTravelTimestamp(CaseInsensitiveStringMap options) {
    return Optional.ofNullable(options.get(SparkReadOptions.TIMESTAMP_AS_OF));
  }

  private static void setupDefaultSparkCatalogs(SparkSession spark) {
    if (spark.conf().getOption(DEFAULT_CATALOG).isEmpty()) {
      ImmutableMap<String, String> config =
          ImmutableMap.of(
              "type", "hive",
              "default-namespace", "default",
              "cache-enabled", "false" // the source should not use a cache
              );
      spark.conf().set(DEFAULT_CATALOG, SparkCatalog.class.getName());
      config.forEach((key, value) -> spark.conf().set(DEFAULT_CATALOG + "." + key, value));
    }

    if (spark.conf().getOption(REWRITE_CATALOG).isEmpty()) {
      spark.conf().set(REWRITE_CATALOG, SparkRewriteTableCatalog.class.getName());
    }
  }
}
