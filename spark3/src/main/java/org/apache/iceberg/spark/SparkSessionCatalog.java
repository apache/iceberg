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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogExtension;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A Spark catalog that can also load non-Iceberg tables.
 *
 * @param <T> CatalogPlugin class to avoid casting to TableCatalog and SupportsNamespaces.
 */
public class SparkSessionCatalog<T extends TableCatalog & SupportsNamespaces>
    implements StagingTableCatalog, SupportsNamespaces, CatalogExtension {
  private static final String[] DEFAULT_NAMESPACE = new String[] {"default"};

  private String catalogName = null;
  private TableCatalog icebergCatalog = null;
  private StagingTableCatalog asStagingCatalog = null;
  private T sessionCatalog = null;
  private boolean createParquetAsIceberg = false;
  private boolean createAvroAsIceberg = false;
  private boolean createOrcAsIceberg = false;

  /**
   * Build a {@link SparkCatalog} to be used for Iceberg operations.
   * <p>
   * The default implementation creates a new SparkCatalog with the session catalog's name and options.
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
  public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
    return getSessionCatalog().loadNamespaceMetadata(namespace);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) throws NamespaceAlreadyExistsException {
    getSessionCatalog().createNamespace(namespace, metadata);
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) throws NoSuchNamespaceException {
    getSessionCatalog().alterNamespace(namespace, changes);
  }

  @Override
  public boolean dropNamespace(String[] namespace) throws NoSuchNamespaceException {
    return getSessionCatalog().dropNamespace(namespace);
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
  public Table createTable(Identifier ident, StructType schema, Transform[] partitions,
                           Map<String, String> properties)
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
  public StagedTable stageCreate(Identifier ident, StructType schema, Transform[] partitions,
                                 Map<String, String> properties)
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

    // create the table with the session catalog, then wrap it in a staged table that will delete to roll back
    Table table = catalog.createTable(ident, schema, partitions, properties);
    return new RollbackStagedTable(catalog, ident, table);
  }

  @Override
  public StagedTable stageReplace(Identifier ident, StructType schema, Transform[] partitions,
                                  Map<String, String> properties)
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
      // create the table with the session catalog, then wrap it in a staged table that will delete to roll back
      Table table = catalog.createTable(ident, schema, partitions, properties);
      return new RollbackStagedTable(catalog, ident, table);

    } catch (TableAlreadyExistsException e) {
      // the table was deleted, but now already exists again. retry the replace.
      return stageReplace(ident, schema, partitions, properties);
    }
  }

  @Override
  public StagedTable stageCreateOrReplace(Identifier ident, StructType schema, Transform[] partitions,
                                          Map<String, String> properties) throws NoSuchNamespaceException {
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
      // create the table with the session catalog, then wrap it in a staged table that will delete to roll back
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
    // no need to check table existence to determine which catalog to use. if a table doesn't exist then both are
    // required to return false.
    return icebergCatalog.dropTable(ident) || getSessionCatalog().dropTable(ident);
  }

  @Override
  public void renameTable(Identifier from, Identifier to) throws NoSuchTableException, TableAlreadyExistsException {
    // rename is not supported by HadoopCatalog. to avoid UnsupportedOperationException for session catalog tables,
    // check table existence first to ensure that the table belongs to the Iceberg catalog.
    if (icebergCatalog.tableExists(from)) {
      icebergCatalog.renameTable(from, to);
    } else {
      getSessionCatalog().renameTable(from, to);
    }
  }

  @Override
  public final void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
    this.icebergCatalog = buildSparkCatalog(name, options);
    if (icebergCatalog instanceof StagingTableCatalog) {
      this.asStagingCatalog = (StagingTableCatalog) icebergCatalog;
    }

    this.createParquetAsIceberg = options.getBoolean("parquet-enabled", createParquetAsIceberg);
    this.createAvroAsIceberg = options.getBoolean("avro-enabled", createAvroAsIceberg);
    this.createOrcAsIceberg = options.getBoolean("orc-enabled", createOrcAsIceberg);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setDelegateCatalog(CatalogPlugin sparkSessionCatalog) {
    if (sparkSessionCatalog instanceof TableCatalog && sparkSessionCatalog instanceof SupportsNamespaces) {
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
    Preconditions.checkNotNull(sessionCatalog, "Delegated SessionCatalog is missing. " +
        "Please make sure your are replacing Spark's default catalog, named 'spark_catalog'.");
    return sessionCatalog;
  }
}
