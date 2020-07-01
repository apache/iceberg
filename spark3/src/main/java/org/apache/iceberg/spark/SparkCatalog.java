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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.spark.source.StagedSparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.TableChange.ColumnChange;
import org.apache.spark.sql.connector.catalog.TableChange.RemoveProperty;
import org.apache.spark.sql.connector.catalog.TableChange.SetProperty;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A Spark TableCatalog implementation that wraps Iceberg's {@link Catalog} interface.
 */
public class SparkCatalog implements StagingTableCatalog {
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
  public SparkTable createTable(Identifier ident, StructType schema,
                                Transform[] transforms,
                                Map<String, String> properties) throws TableAlreadyExistsException {
    Schema icebergSchema = SparkSchemaUtil.convert(schema);
    try {
      return new SparkTable(icebergCatalog.createTable(
          buildIdentifier(ident),
          icebergSchema,
          Spark3Util.toPartitionSpec(icebergSchema, transforms),
          properties.get("location"),
          Spark3Util.rebuildCreateProperties(properties)));
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
          Spark3Util.toPartitionSpec(icebergSchema, transforms), properties.get("location"), properties));
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
          Spark3Util.toPartitionSpec(icebergSchema, transforms), properties.get("location"), properties,
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
        Spark3Util.toPartitionSpec(icebergSchema, transforms), properties.get("location"), properties,
        true /* create or replace */));
  }

  @Override
  public SparkTable alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    SetProperty setLocation = null;
    SetProperty setSnapshotId = null;
    SetProperty pickSnapshotId = null;
    List<TableChange> propertyChanges = Lists.newArrayList();
    List<TableChange> schemaChanges = Lists.newArrayList();

    for (TableChange change : changes) {
      if (change instanceof SetProperty) {
        SetProperty set = (SetProperty) change;
        if (TableCatalog.PROP_LOCATION.equalsIgnoreCase(set.property())) {
          setLocation = set;
        } else if ("current-snapshot-id".equalsIgnoreCase(set.property())) {
          setSnapshotId = set;
        } else if ("cherry-pick-snapshot-id".equalsIgnoreCase(set.property())) {
          pickSnapshotId = set;
        } else {
          propertyChanges.add(set);
        }
      } else if (change instanceof RemoveProperty) {
        propertyChanges.add(change);
      } else if (change instanceof ColumnChange) {
        schemaChanges.add(change);
      } else {
        throw new UnsupportedOperationException("Cannot apply unknown table change: " + change);
      }
    }

    try {
      Table table = icebergCatalog.loadTable(buildIdentifier(ident));
      commitChanges(table, setLocation, setSnapshotId, pickSnapshotId, propertyChanges, schemaChanges);
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

  private static void commitChanges(Table table, SetProperty setLocation, SetProperty setSnapshotId,
                                    SetProperty pickSnapshotId, List<TableChange> propertyChanges,
                                    List<TableChange> schemaChanges) {
    // don't allow setting the snapshot and picking a commit at the same time because order is ambiguous and choosing
    // one order leads to different results
    Preconditions.checkArgument(setSnapshotId == null || pickSnapshotId == null,
        "Cannot set the current the current snapshot ID and cherry-pick snapshot changes");

    if (setSnapshotId != null) {
      long newSnapshotId = Long.parseLong(setSnapshotId.value());
      table.manageSnapshots().setCurrentSnapshot(newSnapshotId).commit();
    }

    // if updating the table snapshot, perform that update first in case it fails
    if (pickSnapshotId != null) {
      long newSnapshotId = Long.parseLong(pickSnapshotId.value());
      table.manageSnapshots().cherrypick(newSnapshotId).commit();
    }

    Transaction transaction = table.newTransaction();

    if (setLocation != null) {
      transaction.updateLocation()
          .setLocation(setLocation.value())
          .commit();
    }

    if (!propertyChanges.isEmpty()) {
      Spark3Util.applyPropertyChanges(transaction.updateProperties(), propertyChanges).commit();
    }

    if (!schemaChanges.isEmpty()) {
      Spark3Util.applySchemaChanges(transaction.updateSchema(), schemaChanges).commit();
    }

    transaction.commitTransaction();
  }
}
