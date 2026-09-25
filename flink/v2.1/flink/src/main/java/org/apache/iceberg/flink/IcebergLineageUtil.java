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
package org.apache.iceberg.flink;

import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.lineage.DatasetConfigFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader.CatalogTableLoader;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the FLIP-314 {@link LineageDataset} that the Iceberg source and sink publish, so a job's
 * source→sink table lineage reaches a Flink {@code JobStatusChangedListener}.
 *
 * <p>The dataset uses the namespace {@code iceberg} and carries the catalog alias, effective REST
 * prefix when available, and native table namespace and name in its config facet. Catalog URIs and
 * warehouses are not published because they may contain credentials. Composing a vendor-specific
 * fully-qualified name is the listener's responsibility; native namespaces are reported verbatim.
 *
 * <p>Coordinates live in a {@link DatasetConfigFacet} rather than in {@link LineageDataset#name()}
 * because on the SQL path the Table planner wraps the dataset in {@code TableLineageDatasetImpl},
 * which overwrites {@code name()} with the Flink object identifier. {@code namespace()} and {@code
 * facets()} survive.
 *
 * <p>Lineage is best-effort observability: every path here yields no dataset rather than throwing,
 * so a table whose coordinates cannot be resolved never fails the job.
 */
public class IcebergLineageUtil {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergLineageUtil.class);

  /** Facet key under which the table's coordinates are published. */
  static final String FACET_NAME = "iceberg";

  /** Connector namespace; native table coordinates are carried in the config facet. */
  static final String DEFAULT_NAMESPACE = "iceberg";

  // The local catalog alias is distinct from the initialized REST catalog's routing prefix.
  static final String CONFIG_CATALOG = "catalog";
  static final String CONFIG_CATALOG_PREFIX = "catalog.prefix";
  static final String CONFIG_NAMESPACE = "namespace";
  static final String CONFIG_TABLE = "table";

  // Mirrors ResourcePaths.PREFIX, after merging server defaults, client properties and overrides.
  private static final String REST_PREFIX = "prefix";

  /** A confirmed absent prefix; unlike an unresolved null, this suppresses catalog lookup. */
  static final String NO_REST_PREFIX = "";

  private IcebergLineageUtil() {}

  static List<LineageDataset> datasetsOf(TableLoader tableLoader, String fullTableName) {
    return datasetsOf(tableLoader, fullTableName, null);
  }

  /**
   * Returns one dataset for a catalog-backed table, or none when it cannot be described.
   *
   * @param tableLoader the table's loader
   * @param fullTableName the table's display name
   * @param restPrefix a captured prefix, empty to suppress lookup, or null to resolve it using a
   *     separate catalog instance
   */
  @SuppressWarnings("CatchBlockLogException") // Catalog exceptions may contain credentials.
  public static List<LineageDataset> datasetsOf(
      TableLoader tableLoader, String fullTableName, String restPrefix) {
    try {
      LineageDataset dataset = describe(tableLoader, fullTableName, restPrefix);
      return dataset == null ? ImmutableList.of() : ImmutableList.of(dataset);
    } catch (Exception e) {
      LOG.warn(
          "Could not resolve Iceberg lineage for {}; continuing without it ({})",
          fullTableName,
          e.getClass().getSimpleName());
      return ImmutableList.of();
    }
  }

  /**
   * Reads the effective REST prefix without opening or closing the loader.
   *
   * @return the prefix, empty if the open catalog has none, or null if unavailable
   */
  @SuppressWarnings("CatchBlockLogException") // Catalog exceptions may contain credentials.
  public static String restPrefixOf(TableLoader tableLoader) {
    try {
      if (!(tableLoader instanceof CatalogTableLoader) || !tableLoader.isOpen()) {
        return null;
      }

      Catalog catalog = ((CatalogTableLoader) tableLoader).catalog();
      if (!(catalog instanceof RESTCatalog)) {
        // A live catalog that is not REST has no prefix, and no second look will produce one.
        return NO_REST_PREFIX;
      }

      String prefix = ((RESTCatalog) catalog).properties().get(REST_PREFIX);
      return Strings.isNullOrEmpty(prefix) ? NO_REST_PREFIX : prefix;
    } catch (Exception e) {
      LOG.debug(
          "Could not read the REST catalog prefix from the open catalog ({})",
          e.getClass().getSimpleName());
      return null;
    }
  }

  /** The dataset describing {@code tableLoader}'s table, or null if it cannot be described. */
  private static LineageDataset describe(
      TableLoader tableLoader, String fullTableName, String restPrefix) {
    if (!(tableLoader instanceof CatalogTableLoader)) {
      LOG.debug("Skipping lineage for {}: not a catalog-backed table", fullTableName);
      return null;
    }

    if (Strings.isNullOrEmpty(fullTableName)) {
      LOG.debug("Skipping lineage: no table name available");
      return null;
    }

    CatalogTableLoader loader = (CatalogTableLoader) tableLoader;
    TableIdentifier identifier = loader.tableIdentifier();

    // Connection settings can contain credentials even in URI or warehouse values.
    ImmutableMap.Builder<String, String> config = ImmutableMap.builder();
    putIfPresent(config, CONFIG_CATALOG, catalogAlias(fullTableName, identifier));
    putIfPresent(
        config,
        CONFIG_CATALOG_PREFIX,
        restPrefix != null ? restPrefix : loadRestPrefix(loader.catalogLoader()));
    config.put(CONFIG_NAMESPACE, identifier.namespace().toString());
    config.put(CONFIG_TABLE, identifier.name());

    return new IcebergLineageDataset(fullTableName, config.build());
  }

  /** Resolves an uncaptured REST prefix; failures omit only the prefix, not the dataset. */
  @SuppressWarnings("CatchBlockLogException") // Catalog exceptions may contain credentials.
  private static String loadRestPrefix(CatalogLoader catalogLoader) {
    Catalog catalog = null;
    try {
      if (!isRestCatalog(catalogLoader)) {
        return null;
      }

      catalog = catalogLoader.clone().loadCatalog();
      return catalog instanceof RESTCatalog
          ? ((RESTCatalog) catalog).properties().get(REST_PREFIX)
          : null;
    } catch (Exception e) {
      LOG.warn(
          "Could not resolve the REST catalog prefix; reporting lineage without it ({})",
          e.getClass().getSimpleName());
      return null;
    } finally {
      closeQuietly(catalog);
    }
  }

  private static boolean isRestCatalog(CatalogLoader catalogLoader) {
    if (catalogLoader instanceof CatalogLoader.RESTCatalogLoader) {
      return true;
    }

    Map<String, String> catalogProperties = catalogLoader.properties();
    return CatalogUtil.ICEBERG_CATALOG_TYPE_REST.equalsIgnoreCase(
            catalogProperties.get(CatalogUtil.ICEBERG_CATALOG_TYPE))
        || RESTCatalog.class
            .getName()
            .equals(catalogProperties.get(CatalogProperties.CATALOG_IMPL));
  }

  @SuppressWarnings("CatchBlockLogException") // Catalog exceptions may contain credentials.
  private static void closeQuietly(Catalog catalog) {
    if (catalog instanceof AutoCloseable) {
      try {
        ((AutoCloseable) catalog).close();
      } catch (Exception e) {
        LOG.debug(
            "Failed to close the catalog opened to resolve lineage ({})",
            e.getClass().getSimpleName());
      }
    }
  }

  /**
   * {@code fullTableName} with the trailing {@code .$namespace.$table} removed, or null when it
   * does not end with the identifier.
   */
  private static String catalogAlias(String fullTableName, TableIdentifier identifier) {
    String suffix = "." + identifier;
    if (fullTableName.endsWith(suffix) && fullTableName.length() > suffix.length()) {
      return fullTableName.substring(0, fullTableName.length() - suffix.length());
    }

    return null;
  }

  private static void putIfPresent(
      ImmutableMap.Builder<String, String> config, String key, String value) {
    if (!Strings.isNullOrEmpty(value)) {
      config.put(key, value);
    }
  }

  private static class IcebergLineageDataset implements LineageDataset {
    private final String name;
    private final Map<String, LineageDatasetFacet> facets;

    IcebergLineageDataset(String name, Map<String, String> config) {
      this.name = name;
      this.facets = ImmutableMap.of(FACET_NAME, new IcebergConfigFacet(config));
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String namespace() {
      return DEFAULT_NAMESPACE;
    }

    @Override
    public Map<String, LineageDatasetFacet> facets() {
      return facets;
    }
  }

  /** Carries the table's coordinates as individual keys, so listeners need not parse a name. */
  private static class IcebergConfigFacet implements DatasetConfigFacet {
    private final Map<String, String> config;

    IcebergConfigFacet(Map<String, String> config) {
      this.config = config;
    }

    @Override
    public Map<String, String> config() {
      return config;
    }

    @Override
    public String name() {
      return FACET_NAME;
    }
  }
}
