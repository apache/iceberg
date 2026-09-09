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
 * <p>The dataset carries Iceberg's own vocabulary only: catalog, namespace, table, and the
 * catalog's {@code uri}/{@code warehouse}. Composing a vendor fully-qualified name — BigLake's
 * four-part {@code $project.$catalog.$database.$table}, say — is the listener's job, since only it
 * has the deployment context. Namespaces are reported verbatim for the same reason: Iceberg allows
 * any depth, and flattening one to fit a vendor scheme is not this class's decision.
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

  /** Dataset namespace when the catalog declares neither a {@code uri} nor a {@code warehouse}. */
  static final String DEFAULT_NAMESPACE = "iceberg";

  // Facet keys. CONFIG_CATALOG is the Flink CREATE CATALOG alias, which is arbitrary and local to
  // the job; CONFIG_CATALOG_PREFIX is the identity the REST server itself assigned, which is not.
  static final String CONFIG_CATALOG = "catalog";
  static final String CONFIG_CATALOG_PREFIX = "catalog.prefix";
  static final String CONFIG_CATALOG_URI = "catalog.uri";
  static final String CONFIG_CATALOG_WAREHOUSE = "catalog.warehouse";
  static final String CONFIG_NAMESPACE = "namespace";
  static final String CONFIG_TABLE = "table";

  /**
   * REST config key holding the catalog handle the server resolved for this client. Mirrors the
   * private {@code org.apache.iceberg.rest.ResourcePaths#PREFIX}; a client never sends it, so its
   * presence means the value came from the server's {@code GET /v1/config} response.
   */
  private static final String REST_PREFIX = "prefix";

  /**
   * What {@link #restPrefixOf} returns when a live catalog was consulted and has no prefix to give,
   * as distinct from null, which means no catalog could be consulted at all. Only the second is
   * worth a retry: an answer of "there is no prefix" is still an answer, and asking again would
   * cost a catalog initialization on every submission for the life of the deployment.
   */
  public static final String NO_REST_PREFIX = "";

  private IcebergLineageUtil() {}

  /**
   * The lineage datasets for the Iceberg table addressed by {@code tableLoader}, shaped for {@code
   * LineageVertex#datasets()}: one dataset, or none when the table cannot be described — for
   * example a path-based {@code HadoopTableLoader}, which has no catalog.
   *
   * @param tableLoader the loader the source or sink was built with
   * @param fullTableName {@code Table.name()}, i.e. {@code catalog.namespace.table}
   */
  public static List<LineageDataset> datasetsOf(TableLoader tableLoader, String fullTableName) {
    return datasetsOf(tableLoader, fullTableName, null);
  }

  /**
   * As {@link #datasetsOf(TableLoader, String)}, but using a {@code restPrefix} the caller already
   * captured with {@link #restPrefixOf} from a catalog it had open for its own reasons.
   *
   * <p>This is the overload sources and sinks should use. The prefix is the one coordinate that
   * only a live catalog knows, so resolving it here would cost a catalog initialization — on the
   * job-submission path, per table, and more than once per table, since Flink asks a connector for
   * its lineage vertex both when it extracts the dataset and when it constructs the transformation.
   * Passing a captured prefix makes reporting lineage free.
   *
   * @param restPrefix a prefix captured from a live catalog, {@link #NO_REST_PREFIX} if that
   *     catalog had none, or null to resolve it by opening a catalog
   */
  public static List<LineageDataset> datasetsOf(
      TableLoader tableLoader, String fullTableName, String restPrefix) {
    try {
      LineageDataset dataset = describe(tableLoader, fullTableName, restPrefix);
      return dataset == null ? ImmutableList.of() : ImmutableList.of(dataset);
    } catch (Exception e) {
      LOG.warn("Could not resolve Iceberg lineage for {}; continuing without it", fullTableName, e);
      return ImmutableList.of();
    }
  }

  /**
   * The REST {@code prefix} carried by {@code tableLoader}'s open catalog; {@link #NO_REST_PREFIX}
   * if that catalog answered but has no prefix to give; null if no catalog could be consulted,
   * because the loader is not catalog-backed or is not open.
   *
   * <p>Costs nothing — it reads a property off a live catalog rather than opening one. Call it
   * while a loader opened for some other purpose is still open, and hand the result to {@link
   * #datasetsOf(TableLoader, String, String)}.
   */
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
      LOG.debug("Could not read the REST catalog prefix from the open catalog", e);
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
    CatalogLoader catalogLoader = loader.catalogLoader();
    TableIdentifier identifier = loader.tableIdentifier();
    Map<String, String> catalogProperties = catalogLoader.properties();

    // Only these keys are copied: catalog properties routinely carry credentials, and the facet is
    // forwarded off-cluster.
    ImmutableMap.Builder<String, String> config = ImmutableMap.builder();
    putIfPresent(config, CONFIG_CATALOG, catalogAlias(fullTableName, identifier));
    putIfPresent(
        config,
        CONFIG_CATALOG_PREFIX,
        restPrefix != null ? restPrefix : loadRestPrefix(catalogLoader, catalogProperties));
    putIfPresent(config, CONFIG_CATALOG_URI, catalogProperties.get(CatalogProperties.URI));
    putIfPresent(
        config,
        CONFIG_CATALOG_WAREHOUSE,
        catalogProperties.get(CatalogProperties.WAREHOUSE_LOCATION));
    config.put(CONFIG_NAMESPACE, identifier.namespace().toString());
    config.put(CONFIG_TABLE, identifier.name());

    return new IcebergLineageDataset(
        datasetNamespace(catalogProperties), fullTableName, config.build());
  }

  /**
   * The {@code prefix} a REST catalog's server assigned to this client, resolved by opening a
   * catalog, or null for any other catalog type. This is the catalog's authoritative server-side
   * identity — the handle used for every {@code /v1/<prefix>/...} call — and is read rather than
   * derived from the {@code warehouse}, which is wrong for any catalog not backed by exactly one
   * bucket.
   *
   * <p>The fallback for callers that captured no prefix, which on the SQL path is nobody: the
   * prefix arrives in {@code GET /v1/config} and is merged into a catalog's properties at
   * initialization, so a caller that opened a catalog already has it and should pass it in via
   * {@link #restPrefixOf}. This path exists for a source or sink handed a pre-loaded {@link
   * org.apache.iceberg.Table}, which never opens a catalog of its own; it costs one initialization,
   * so other catalog types are skipped rather than opened speculatively. A clone is loaded so the
   * source's or sink's own loader keeps its lifecycle.
   *
   * <p>Failures are contained here rather than propagating: the rest of the coordinates are known
   * locally, so an unreachable catalog should cost the prefix, not the whole dataset.
   */
  private static String loadRestPrefix(
      CatalogLoader catalogLoader, Map<String, String> catalogProperties) {
    if (!isRestCatalog(catalogLoader, catalogProperties)) {
      return null;
    }

    Catalog catalog = null;
    try {
      catalog = catalogLoader.clone().loadCatalog();
      return catalog instanceof RESTCatalog
          ? Strings.emptyToNull(((RESTCatalog) catalog).properties().get(REST_PREFIX))
          : null;
    } catch (Exception e) {
      LOG.warn("Could not resolve the REST catalog prefix; reporting lineage without it", e);
      return null;
    } finally {
      closeQuietly(catalog);
    }
  }

  private static boolean isRestCatalog(
      CatalogLoader catalogLoader, Map<String, String> catalogProperties) {
    return catalogLoader instanceof CatalogLoader.RESTCatalogLoader
        || CatalogUtil.ICEBERG_CATALOG_TYPE_REST.equalsIgnoreCase(
            catalogProperties.get(CatalogUtil.ICEBERG_CATALOG_TYPE))
        || RESTCatalog.class
            .getName()
            .equals(catalogProperties.get(CatalogProperties.CATALOG_IMPL));
  }

  private static void closeQuietly(Catalog catalog) {
    if (catalog instanceof AutoCloseable) {
      try {
        ((AutoCloseable) catalog).close();
      } catch (Exception e) {
        LOG.debug("Failed to close the catalog opened to resolve lineage", e);
      }
    }
  }

  /**
   * The dataset namespace: the catalog's {@code uri}, else its {@code warehouse}, else {@link
   * #DEFAULT_NAMESPACE}. Unlike {@code name()} this survives the Table planner, so it is kept
   * coarse — it identifies the catalog, not the table.
   */
  private static String datasetNamespace(Map<String, String> catalogProperties) {
    String uri = Strings.emptyToNull(catalogProperties.get(CatalogProperties.URI));
    if (uri != null) {
      return uri;
    }

    String warehouse =
        Strings.emptyToNull(catalogProperties.get(CatalogProperties.WAREHOUSE_LOCATION));
    return warehouse != null ? warehouse : DEFAULT_NAMESPACE;
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
    private final String namespace;
    private final String name;
    private final Map<String, LineageDatasetFacet> facets;

    IcebergLineageDataset(String namespace, String name, Map<String, String> config) {
      this.namespace = namespace;
      this.name = name;
      this.facets = ImmutableMap.of(FACET_NAME, new IcebergConfigFacet(config));
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String namespace() {
      return namespace;
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
