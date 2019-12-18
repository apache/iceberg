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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.iceberg.TableProperties;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogExtension;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
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
    implements TableCatalog, SupportsNamespaces, CatalogExtension {

  private String name = null;
  private SparkCatalog icebergCatalog = null;
  private T sessionCatalog = null;
  private boolean createParquetAsIceberg = false;
  private boolean createAvroAsIceberg = false;

  /**
   * Build a {@link SparkCatalog} to be used for Iceberg operations.
   * <p>
   * The default implementation creates a new SparkCatalog with the session catalog's name and options.
   *
   * @param name catalog name
   * @param options catalog options
   * @return a SparkCatalog to be used for Iceberg tables
   */
  protected SparkCatalog buildSparkCatalog(String name, CaseInsensitiveStringMap options) {
    SparkCatalog newCatalog = new SparkCatalog();
    newCatalog.initialize(name, options);
    return newCatalog;
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    return sessionCatalog.listNamespaces();
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return sessionCatalog.listNamespaces(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
    return sessionCatalog.loadNamespaceMetadata(namespace);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) throws NamespaceAlreadyExistsException {
    sessionCatalog.createNamespace(namespace, metadata);
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) throws NoSuchNamespaceException {
    sessionCatalog.alterNamespace(namespace, changes);
  }

  @Override
  public boolean dropNamespace(String[] namespace) throws NoSuchNamespaceException {
    return sessionCatalog.dropNamespace(namespace);
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    // delegate to the session catalog because all tables share the same namespace
    return sessionCatalog.listTables(namespace);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    try {
      return icebergCatalog.loadTable(ident);
    } catch (NoSuchTableException e) {
      return sessionCatalog.loadTable(ident);
    }
  }

  @Override
  public Table createTable(Identifier ident, StructType schema, Transform[] partitions,
                           Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    String provider = properties.get("provider");
    if (provider == null || "iceberg".equalsIgnoreCase(provider)) {
      return icebergCatalog.createTable(ident, schema, partitions, properties);

    } else if (createParquetAsIceberg && "parquet".equalsIgnoreCase(provider)) {
      ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
      propertiesBuilder.putAll(properties);
      propertiesBuilder.put(TableProperties.DEFAULT_FILE_FORMAT, "parquet");
      return icebergCatalog.createTable(ident, schema, partitions, propertiesBuilder.build());

    } else if (createAvroAsIceberg && "avro".equalsIgnoreCase(provider)) {
      ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
      propertiesBuilder.putAll(properties);
      propertiesBuilder.put(TableProperties.DEFAULT_FILE_FORMAT, "avro");
      return icebergCatalog.createTable(ident, schema, partitions, properties);

    } else {
      // delegate to the session catalog
      return sessionCatalog.createTable(ident, schema, partitions, properties);
    }
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    try {
      return icebergCatalog.alterTable(ident, changes);
    } catch (NoSuchTableException e) {
      return sessionCatalog.alterTable(ident, changes);
    }
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return icebergCatalog.dropTable(ident) || sessionCatalog.dropTable(ident);
  }

  @Override
  public void renameTable(Identifier from, Identifier to) throws NoSuchTableException, TableAlreadyExistsException {
    try {
      icebergCatalog.renameTable(from, to);
    } catch (NoSuchTableException e) {
      sessionCatalog.renameTable(from, to);
    }
  }

  @Override
  public final void initialize(String name, CaseInsensitiveStringMap options) {
    this.name = name;
    this.icebergCatalog = buildSparkCatalog(name, options);
    this.createParquetAsIceberg = options.getBoolean("parquet-enabled", createParquetAsIceberg);
    this.createAvroAsIceberg = options.getBoolean("avro-enabled", createAvroAsIceberg);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setDelegateCatalog(CatalogPlugin sessionCatalog) {
    if (sessionCatalog instanceof TableCatalog && sessionCatalog instanceof SupportsNamespaces) {
      this.sessionCatalog = (T) sessionCatalog;
    } else {
      throw new IllegalArgumentException("Invalid session catalog: " + sessionCatalog);
    }
  }

  @Override
  public String name() {
    return name;
  }
}
