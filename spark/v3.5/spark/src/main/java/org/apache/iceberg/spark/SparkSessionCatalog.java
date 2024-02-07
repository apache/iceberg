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

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogExtension;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.connector.catalog.ViewChange;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A Spark catalog that can also load non-Iceberg tables.
 *
 * @param <T> CatalogPlugin class to avoid casting to TableCatalog, FunctionCatalog and
 *     SupportsNamespaces.
 */
public class SparkSessionCatalog<
        T extends TableCatalog & FunctionCatalog & SupportsNamespaces & ViewCatalog>
    extends BaseCatalog implements CatalogExtension {
  private static final String[] DEFAULT_NAMESPACE = new String[] {"default"};

  private String catalogName = null;
  private TableCatalog icebergCatalog = null;
  private StagingTableCatalog asStagingCatalog = null;
  private ViewCatalog asViewCatalog = null;
  private SupportsNamespaces asNamespaceCatalog = null;
  private T sessionCatalog = null;
  private boolean createParquetAsIceberg = false;
  private boolean createAvroAsIceberg = false;
  private boolean createOrcAsIceberg = false;

  /**
   * Build a {@link SparkCatalog} to be used for Iceberg operations.
   *
   * <p>The default implementation creates a new SparkCatalog with the session catalog's name and
   * options.
   *
   * @param name catalog name
   * @param options catalog options
   * @return a SparkCatalog to be used for Iceberg tables
   */
  protected TableCatalog buildSparkCatalog(String name, CaseInsensitiveStringMap options) {
    SparkCatalog newCatalog = new SparkCatalog();
    newCatalog.initialize(name, options);
    return newCatalog;
  }

  @Override
  public String[] defaultNamespace() {
    return DEFAULT_NAMESPACE;
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    return getSessionCatalog().listNamespaces();
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return getSessionCatalog().listNamespaces(namespace);
  }

  @Override
  public boolean namespaceExists(String[] namespace) {
    boolean exists = getSessionCatalog().namespaceExists(namespace);
    //    if (null != asNamespaceCatalog && asNamespaceCatalog.namespaceExists(namespace)) {
    if (null != asNamespaceCatalog) {
      return asNamespaceCatalog.namespaceExists(namespace) && exists;
    }

    return exists;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    if (null != asNamespaceCatalog && asNamespaceCatalog.namespaceExists(namespace)) {
      return asNamespaceCatalog.loadNamespaceMetadata(namespace);
    }

    return getSessionCatalog().loadNamespaceMetadata(namespace);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    try {
      getSessionCatalog().createNamespace(namespace, metadata);
    } finally {
      if (null != asNamespaceCatalog && !asNamespaceCatalog.namespaceExists(namespace)) {
        asNamespaceCatalog.createNamespace(namespace, metadata);
      }
    }

    getSessionCatalog().createNamespace(namespace, metadata);
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    if (null != asNamespaceCatalog && asNamespaceCatalog.namespaceExists(namespace)) {
      asNamespaceCatalog.alterNamespace(namespace, changes);
    }

    getSessionCatalog().alterNamespace(namespace, changes);
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException, NonEmptyNamespaceException {
    if (null != asNamespaceCatalog && asNamespaceCatalog.namespaceExists(namespace)) {
      asNamespaceCatalog.dropNamespace(namespace, cascade);
    }

    return getSessionCatalog().dropNamespace(namespace, cascade);
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    // delegate to the session catalog because all tables share the same namespace
    return getSessionCatalog().listTables(namespace);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    try {
      return icebergCatalog.loadTable(ident);
    } catch (NoSuchTableException e) {
      return getSessionCatalog().loadTable(ident);
    }
  }

  @Override
  public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
    try {
      return icebergCatalog.loadTable(ident, version);
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      return getSessionCatalog().loadTable(ident, version);
    }
  }

  @Override
  public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
    try {
      return icebergCatalog.loadTable(ident, timestamp);
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      return getSessionCatalog().loadTable(ident, timestamp);
    }
  }

  @Override
  public void invalidateTable(Identifier ident) {
    // We do not need to check whether the table exists and whether
    // it is an Iceberg table to reduce remote service requests.
    icebergCatalog.invalidateTable(ident);
    getSessionCatalog().invalidateTable(ident);
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    String provider = properties.get("provider");
    if (useIceberg(provider)) {
      return icebergCatalog.createTable(ident, schema, partitions, properties);
    } else {
      // delegate to the session catalog
      return getSessionCatalog().createTable(ident, schema, partitions, properties);
    }
  }

  @Override
  public StagedTable stageCreate(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    String provider = properties.get("provider");
    TableCatalog catalog;
    if (useIceberg(provider)) {
      if (asStagingCatalog != null) {
        return asStagingCatalog.stageCreate(ident, schema, partitions, properties);
      }
      catalog = icebergCatalog;
    } else {
      catalog = getSessionCatalog();
    }

    // create the table with the session catalog, then wrap it in a staged table that will delete to
    // roll back
    Table table = catalog.createTable(ident, schema, partitions, properties);
    return new RollbackStagedTable(catalog, ident, table);
  }

  @Override
  public StagedTable stageReplace(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException, NoSuchTableException {
    String provider = properties.get("provider");
    TableCatalog catalog;
    if (useIceberg(provider)) {
      if (asStagingCatalog != null) {
        return asStagingCatalog.stageReplace(ident, schema, partitions, properties);
      }
      catalog = icebergCatalog;
    } else {
      catalog = getSessionCatalog();
    }

    // attempt to drop the table and fail if it doesn't exist
    if (!catalog.dropTable(ident)) {
      throw new NoSuchTableException(ident);
    }

    try {
      // create the table with the session catalog, then wrap it in a staged table that will delete
      // to roll back
      Table table = catalog.createTable(ident, schema, partitions, properties);
      return new RollbackStagedTable(catalog, ident, table);

    } catch (TableAlreadyExistsException e) {
      // the table was deleted, but now already exists again. retry the replace.
      return stageReplace(ident, schema, partitions, properties);
    }
  }

  @Override
  public StagedTable stageCreateOrReplace(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException {
    String provider = properties.get("provider");
    TableCatalog catalog;
    if (useIceberg(provider)) {
      if (asStagingCatalog != null) {
        return asStagingCatalog.stageCreateOrReplace(ident, schema, partitions, properties);
      }
      catalog = icebergCatalog;
    } else {
      catalog = getSessionCatalog();
    }

    // drop the table if it exists
    catalog.dropTable(ident);

    try {
      // create the table with the session catalog, then wrap it in a staged table that will delete
      // to roll back
      Table sessionCatalogTable = catalog.createTable(ident, schema, partitions, properties);
      return new RollbackStagedTable(catalog, ident, sessionCatalogTable);

    } catch (TableAlreadyExistsException e) {
      // the table was deleted, but now already exists again. retry the replace.
      return stageCreateOrReplace(ident, schema, partitions, properties);
    }
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    if (icebergCatalog.tableExists(ident)) {
      return icebergCatalog.alterTable(ident, changes);
    } else {
      return getSessionCatalog().alterTable(ident, changes);
    }
  }

  @Override
  public boolean dropTable(Identifier ident) {
    // no need to check table existence to determine which catalog to use. if a table doesn't exist
    // then both are
    // required to return false.
    return icebergCatalog.dropTable(ident) || getSessionCatalog().dropTable(ident);
  }

  @Override
  public boolean purgeTable(Identifier ident) {
    // no need to check table existence to determine which catalog to use. if a table doesn't exist
    // then both are
    // required to return false.
    return icebergCatalog.purgeTable(ident) || getSessionCatalog().purgeTable(ident);
  }

  @Override
  public void renameTable(Identifier from, Identifier to)
      throws NoSuchTableException, TableAlreadyExistsException {
    // rename is not supported by HadoopCatalog. to avoid UnsupportedOperationException for session
    // catalog tables,
    // check table existence first to ensure that the table belongs to the Iceberg catalog.
    if (icebergCatalog.tableExists(from)) {
      icebergCatalog.renameTable(from, to);
    } else {
      getSessionCatalog().renameTable(from, to);
    }
  }

  @Override
  public final void initialize(String name, CaseInsensitiveStringMap options) {
    super.initialize(name, options);

    if (options.containsKey(CatalogUtil.ICEBERG_CATALOG_TYPE)
        && options
            .get(CatalogUtil.ICEBERG_CATALOG_TYPE)
            .equalsIgnoreCase(CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE)) {
      validateHmsUri(options.get(CatalogProperties.URI));
    }

    this.catalogName = name;
    this.icebergCatalog = buildSparkCatalog(name, options);
    if (icebergCatalog instanceof StagingTableCatalog) {
      this.asStagingCatalog = (StagingTableCatalog) icebergCatalog;
    }

    if (icebergCatalog instanceof ViewCatalog) {
      this.asViewCatalog = (ViewCatalog) icebergCatalog;
    }

    if (icebergCatalog instanceof SupportsNamespaces) {
      this.asNamespaceCatalog = (SupportsNamespaces) icebergCatalog;
    }

    this.createParquetAsIceberg = options.getBoolean("parquet-enabled", createParquetAsIceberg);
    this.createAvroAsIceberg = options.getBoolean("avro-enabled", createAvroAsIceberg);
    this.createOrcAsIceberg = options.getBoolean("orc-enabled", createOrcAsIceberg);
  }

  private void validateHmsUri(String catalogHmsUri) {
    if (catalogHmsUri == null) {
      return;
    }

    Configuration conf = SparkSession.active().sessionState().newHadoopConf();
    String envHmsUri = conf.get(HiveConf.ConfVars.METASTOREURIS.varname, null);
    if (envHmsUri == null) {
      return;
    }

    Preconditions.checkArgument(
        catalogHmsUri.equals(envHmsUri),
        "Inconsistent Hive metastore URIs: %s (Spark session) != %s (spark_catalog)",
        envHmsUri,
        catalogHmsUri);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setDelegateCatalog(CatalogPlugin sparkSessionCatalog) {
    if (sparkSessionCatalog instanceof TableCatalog
        && sparkSessionCatalog instanceof FunctionCatalog
        && sparkSessionCatalog instanceof SupportsNamespaces) {
      this.sessionCatalog = (T) sparkSessionCatalog;
    } else {
      throw new IllegalArgumentException("Invalid session catalog: " + sparkSessionCatalog);
    }
  }

  @Override
  public String name() {
    return catalogName;
  }

  private boolean useIceberg(String provider) {
    if (provider == null || "iceberg".equalsIgnoreCase(provider)) {
      return true;
    } else if (createParquetAsIceberg && "parquet".equalsIgnoreCase(provider)) {
      return true;
    } else if (createAvroAsIceberg && "avro".equalsIgnoreCase(provider)) {
      return true;
    } else if (createOrcAsIceberg && "orc".equalsIgnoreCase(provider)) {
      return true;
    }

    return false;
  }

  private T getSessionCatalog() {
    Preconditions.checkNotNull(
        sessionCatalog,
        "Delegated SessionCatalog is missing. "
            + "Please make sure your are replacing Spark's default catalog, named 'spark_catalog'.");
    return sessionCatalog;
  }

  @Override
  public Catalog icebergCatalog() {
    Preconditions.checkArgument(
        icebergCatalog instanceof HasIcebergCatalog,
        "Cannot return underlying Iceberg Catalog, wrapped catalog does not contain an Iceberg Catalog");
    return ((HasIcebergCatalog) icebergCatalog).icebergCatalog();
  }

  private boolean isViewCatalog() {
    return getSessionCatalog() instanceof ViewCatalog;
  }

  @Override
  public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
    try {
      return super.loadFunction(ident);
    } catch (NoSuchFunctionException e) {
      return getSessionCatalog().loadFunction(ident);
    }
  }

  @Override
  public Identifier[] listViews(String... namespace) {
    try {
      if (null != asViewCatalog) {
        return asViewCatalog.listViews(namespace);
      } else if (isViewCatalog()) {
        getSessionCatalog().listViews(namespace);
      }
    } catch (NoSuchNamespaceException e) {
      throw new RuntimeException(e);
    }

    return new Identifier[0];
  }

  @Override
  public View loadView(Identifier ident) throws NoSuchViewException {
    if (null != asViewCatalog && asViewCatalog.viewExists(ident)) {
      return asViewCatalog.loadView(ident);
    } else if (isViewCatalog() && getSessionCatalog().viewExists(ident)) {
      return getSessionCatalog().loadView(ident);
    }

    throw new NoSuchViewException(ident);
  }

  @Override
  public View createView(
      Identifier ident,
      String sql,
      String currentCatalog,
      String[] currentNamespace,
      StructType schema,
      String[] queryColumnNames,
      String[] columnAliases,
      String[] columnComments,
      Map<String, String> properties)
      throws ViewAlreadyExistsException, NoSuchNamespaceException {
    if (null != asViewCatalog) {
      return asViewCatalog.createView(
          ident,
          sql,
          currentCatalog,
          currentNamespace,
          schema,
          queryColumnNames,
          columnAliases,
          columnComments,
          properties);
    } else if (isViewCatalog()) {
      return getSessionCatalog()
          .createView(
              ident,
              sql,
              currentCatalog,
              currentNamespace,
              schema,
              queryColumnNames,
              columnAliases,
              columnComments,
              properties);
    }

    throw new UnsupportedOperationException(
        "Creating a view is not supported by catalog: " + catalogName);
  }

  @Override
  public View replaceView(
      Identifier ident,
      String sql,
      String currentCatalog,
      String[] currentNamespace,
      StructType schema,
      String[] queryColumnNames,
      String[] columnAliases,
      String[] columnComments,
      Map<String, String> properties)
      throws NoSuchNamespaceException, NoSuchViewException {
    if (asViewCatalog instanceof SupportsReplaceView) {
      return ((SupportsReplaceView) asViewCatalog)
          .replaceView(
              ident,
              sql,
              currentCatalog,
              currentNamespace,
              schema,
              queryColumnNames,
              columnAliases,
              columnComments,
              properties);
    } else if (isViewCatalog()) {
      try {
        getSessionCatalog().dropView(ident);
        return getSessionCatalog()
            .createView(
                ident,
                sql,
                currentCatalog,
                currentNamespace,
                schema,
                queryColumnNames,
                columnAliases,
                columnComments,
                properties);
      } catch (ViewAlreadyExistsException e) {
        throw new RuntimeException(e);
      }
    }

    throw new UnsupportedOperationException(
        "Replacing a view is not supported by catalog: " + catalogName);
  }

  @Override
  public View alterView(Identifier ident, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException {
    if (null != asViewCatalog && asViewCatalog.viewExists(ident)) {
      return asViewCatalog.alterView(ident, changes);
    } else if (isViewCatalog()) {
      return getSessionCatalog().alterView(ident, changes);
    }

    throw new UnsupportedOperationException(
        "Altering a view is not supported by catalog: " + catalogName);
  }

  @Override
  public boolean dropView(Identifier ident) {
    if (null != asViewCatalog && asViewCatalog.viewExists(ident)) {
      return asViewCatalog.dropView(ident);
    } else if (isViewCatalog()) {
      return getSessionCatalog().dropView(ident);
    }

    return false;
  }

  @Override
  public void renameView(Identifier fromIdentifier, Identifier toIdentifier)
      throws NoSuchViewException, ViewAlreadyExistsException {
    if (null != asViewCatalog && asViewCatalog.viewExists(fromIdentifier)) {
      asViewCatalog.renameView(fromIdentifier, toIdentifier);
    } else if (isViewCatalog()) {
      getSessionCatalog().renameView(fromIdentifier, toIdentifier);
    } else {
      throw new UnsupportedOperationException(
          "Renaming a view is not supported by catalog: " + catalogName);
    }
  }
}
