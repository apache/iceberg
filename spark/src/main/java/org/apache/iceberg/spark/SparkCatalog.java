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

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.spark.source.StagedSparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.TableChange.RemoveProperty;
import org.apache.spark.sql.connector.catalog.TableChange.SetProperty;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A Spark TableCatalog implementation that wraps Iceberg's {@link Catalog} interface.
 */
public class SparkCatalog implements StagingTableCatalog, SupportsNamespaces {
  private String catalogName = null;
  private Catalog icebergCatalog = null;

  /**
   * Build an Iceberg {@link Catalog} to be used by this Spark catalog adapter.
   *
   * @param name Spark's catalog name
   * @param options Spark's catalog options
   * @return an Iceberg catalog
   */
  protected Catalog buildIcebergCatalog(String name, CaseInsensitiveStringMap options) {
    Configuration conf = SparkSession.active().sparkContext().hadoopConfiguration();
    String catalogType = options.getOrDefault("type", "hive");
    switch (catalogType) {
      case "hive":
        int clientPoolSize = options.getInt("clients", 2);
        String uri = options.get("uri");
        return new HiveCatalog(name, uri, clientPoolSize, conf);

      case "hadoop":
        String warehouseLocation = options.get("warehouse");
        return new HadoopCatalog(name, conf, warehouseLocation);

      default:
        throw new UnsupportedOperationException("Unknown catalog type: " + catalogType);
    }
  }

  /**
   * Build an Iceberg {@link TableIdentifier} for the given Spark identifier.
   *
   * @param identifier Spark's identifier
   * @return an Iceberg identifier
   */
  protected TableIdentifier buildIdentifier(Identifier identifier) {
    return TableIdentifier.of(Namespace.of(identifier.namespace()), identifier.name());
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    // TODO: handle namespaces
    return new Identifier[0];
  }

  @Override
  public SparkTable loadTable(Identifier ident) throws NoSuchTableException {
    try {
      return new SparkTable(icebergCatalog.loadTable(buildIdentifier(ident)));
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public SparkTable createTable(Identifier ident, StructType schema, Transform[] transforms,
                                Map<String, String> properties) throws TableAlreadyExistsException {
    Schema icebergSchema = SparkSchemaUtil.convert(schema);
    try {
      return new SparkTable(icebergCatalog.createTable(buildIdentifier(ident),
          icebergSchema, SparkUtil.toPartitionSpec(icebergSchema, transforms), properties.get("location"), properties));
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(ident);
    }
  }

  @Override
  public StagedTable stageCreate(Identifier ident, StructType schema, Transform[] transforms,
                                 Map<String, String> properties) throws TableAlreadyExistsException {
    Schema icebergSchema = SparkSchemaUtil.convert(schema);
    try {
      return new StagedSparkTable(icebergCatalog.newCreateTableTransaction(buildIdentifier(ident), icebergSchema,
          SparkUtil.toPartitionSpec(icebergSchema, transforms), properties.get("location"), properties));
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(ident);
    }
  }

  @Override
  public StagedTable stageReplace(Identifier ident, StructType schema, Transform[] transforms,
                                  Map<String, String> properties) throws NoSuchTableException {
    Schema icebergSchema = SparkSchemaUtil.convert(schema);
    try {
      return new StagedSparkTable(icebergCatalog.newReplaceTableTransaction(buildIdentifier(ident), icebergSchema,
          SparkUtil.toPartitionSpec(icebergSchema, transforms), properties.get("location"), properties,
          false /* do not create */));
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public StagedTable stageCreateOrReplace(Identifier ident, StructType schema, Transform[] transforms,
                                          Map<String, String> properties) {
    Schema icebergSchema = SparkSchemaUtil.convert(schema);
    return new StagedSparkTable(icebergCatalog.newReplaceTableTransaction(buildIdentifier(ident), icebergSchema,
        SparkUtil.toPartitionSpec(icebergSchema, transforms), properties.get("location"), properties,
        true /* create or replace */));
  }

  @Override
  public SparkTable alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    SetProperty setLocation = null;
    SetProperty setSnapshotId = null;
    List<TableChange> propertyChanges = Lists.newArrayList();
    List<TableChange> schemaChanges = Lists.newArrayList();

    for (TableChange change : changes) {
      if (change instanceof SetProperty) {
        SetProperty set = (SetProperty) change;
        if ("location".equalsIgnoreCase(set.property())) {
          setLocation = set;
        } else if ("current-snapshot-id".equalsIgnoreCase(set.property())) {
          setSnapshotId = set;
        } else {
          propertyChanges.add(set);
        }
      } else if (change instanceof RemoveProperty) {
        propertyChanges.add(change);
      } else {
        schemaChanges.add(change);
      }
    }

    try {
      Table table = icebergCatalog.loadTable(buildIdentifier(ident));

      if (setSnapshotId != null) {
        long newSnapshotId = Long.parseLong(setSnapshotId.value());
        table.rollback().toSnapshotId(newSnapshotId).commit();
      }

      Transaction transaction = table.newTransaction();

      if (setLocation != null) {
        transaction.updateLocation()
            .setLocation(setLocation.value())
            .commit();
      }

      if (!propertyChanges.isEmpty()) {
        SparkUtil.applyPropertyChanges(transaction.updateProperties(), propertyChanges).commit();
      }

      if (!schemaChanges.isEmpty()) {
        SparkUtil.applySchemaChanges(transaction.updateSchema(), schemaChanges).commit();
      }

      transaction.commitTransaction();

    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
    return null;
  }

  @Override
  public boolean dropTable(Identifier ident) {
    try {
      return icebergCatalog.dropTable(buildIdentifier(ident), true);
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      return false;
    }
  }

  @Override
  public void renameTable(Identifier from, Identifier to) throws NoSuchTableException, TableAlreadyExistsException {
    try {
      icebergCatalog.renameTable(buildIdentifier(from), buildIdentifier(to));
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(from);
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(to);
    }
  }

  @Override
  public void invalidateTable(Identifier ident) {
    try {
      icebergCatalog.loadTable(buildIdentifier(ident)).refresh();
    } catch (org.apache.iceberg.exceptions.NoSuchTableException ignored) {
      // ignore if the table doesn't exist, it is not cached
    }
  }

  @Override
  public final void initialize(String name, CaseInsensitiveStringMap options) {
    boolean cacheEnabled = Boolean.parseBoolean(options.getOrDefault("cache-enabled", "true"));
    Catalog catalog = buildIcebergCatalog(name, options);

    this.catalogName = name;
    this.icebergCatalog = cacheEnabled ? CachingCatalog.wrap(catalog) : catalog;
  }

  @Override
  public String name() {
    return catalogName;
  }

  /**
   * List top-level namespaces from the catalog.
   * <p>
   * If an object such as a table, view, or function exists, its parent namespaces must also exist
   * and must be returned by this discovery method. For example, if table a.b.t exists, this method
   * must return ["a"] in the result array.
   *
   * @return an array of multi-part namespace names
   */
  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    return listNamespaces(Namespace.empty().levels());
  }

  /**
   * List namespaces in a namespace.
   * <p>
   * If an object such as a table, view, or function exists, its parent namespaces must also exist
   * and must be returned by this discovery method. For example, if table a.b.t exists, this method
   * invoked as listNamespaces(["a"]) must return ["a", "b"] in the result array.
   *
   * @param namespace a multi-part namespace
   * @return an array of multi-part namespace names
   * @throws NoSuchNamespaceException If the namespace does not exist (optional)
   */
  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    List<Namespace> namespaces;
    try {
      if (namespace.length == 0) {
        namespaces = icebergCatalog.listNamespaces();
      } else {
        namespaces = icebergCatalog.listNamespaces(Namespace.of(namespace));
      }
      String[][] spaces = new String[namespaces.size()][];
      for (int    i = 0; i < namespaces.size(); i++) {
        spaces[i] = namespaces.get(i).levels();
      }
      return spaces;
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchNamespaceException(e.getMessage());
    }
  }

  /**
   * Load metadata properties for a namespace.
   *
   * @param namespace a multi-part namespace
   * @return a string map of properties for the given namespace
   * @throws NoSuchNamespaceException      If the namespace does not exist (optional)
   * @throws UnsupportedOperationException If namespace properties are not supported
   */
  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
     //TODO: spark error "Describing columns is not supported for v2 tables.;??"
    try {
      return icebergCatalog.loadNamespaceMetadata(Namespace.of(namespace));
    } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
      throw new NoSuchNamespaceException(e.getMessage());
    }
  }

  /**
   * Create a namespace in the catalog.
   *
   * @param namespace a multi-part namespace
   * @param metadata  a string map of properties for the given namespace
   * @throws NamespaceAlreadyExistsException If the namespace already exists
   * @throws UnsupportedOperationException   If create is not a supported operation
   */
  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) throws NamespaceAlreadyExistsException {
    Namespace space = Namespace.of(namespace);
    metadata.forEach(space::setParameters);
    try {
      icebergCatalog.createNamespace(space);
    } catch (org.apache.iceberg.exceptions.AlreadyExistsException e) {
      throw new NamespaceAlreadyExistsException(e.getMessage());
    }
  }

  /**
   * Apply a set of metadata changes to a namespace in the catalog.
   *
   * @param namespace a multi-part namespace
   * @param changes   a collection of changes to apply to the namespace
   * @throws NoSuchNamespaceException      If the namespace does not exist (optional)
   * @throws UnsupportedOperationException If namespace properties are not supported
   */
  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("un supported load namespace metadata");
  }

  /**
   * Drop a namespace from the catalog.
   * <p>
   * This operation may be rejected by the catalog implementation if the namespace is not empty by
   * throwing {@link IllegalStateException}. If the catalog implementation does not support this
   * operation, it may throw {@link UnsupportedOperationException}.
   *
   * @param namespace a multi-part namespace
   * @return true if the namespace was dropped
   * @throws NoSuchNamespaceException      If the namespace does not exist (optional)
   * @throws IllegalStateException         If the namespace is not empty
   * @throws UnsupportedOperationException If drop is not a supported operation
   */
  @Override
  public boolean dropNamespace(String[] namespace) throws NoSuchNamespaceException {
    try {
      return icebergCatalog.dropNamespace(Namespace.of(namespace));
    } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException  e) {
      throw new NoSuchNamespaceException(e.getMessage());
    }
  }
}
