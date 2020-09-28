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

package org.apache.iceberg.actions;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import jline.internal.Log;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogUtils;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

/**
 * This action will migrate a known table in a Spark Catalog that is not an Iceberg table into an Iceberg table.
 * The created new table will be able to interact with and modify files in the original table.
 *
 * There are two main code paths
 *   - Creating a brand new iceberg table or replacing an existing Iceberg table
 *   This pathway will use a staged table to stage the creation or replacement, only committing after
 *   import has succeeded.
 *
 *   - Replacing a table in the Session Catalog with an Iceberg Table of the same name.
 *   This pathway will first create a temporary table with a different name. This replacement table will
 *   be committed upon a successful import. Then the original session catalog entry will be dropped
 *   and the new replacement table renamed to take its place.
 */
class Spark3CreateAction implements CreateAction {
  private static final Logger LOG = LoggerFactory.getLogger(Spark3CreateAction.class);
  private static final Set<String> ALLOWED_SOURCES = ImmutableSet.of("parquet", "avro", "orc", "hive");
  private static final String ICEBERG_METADATA_FOLDER = "metadata";
  private static final String REPLACEMENT_NAME = "_REPLACEMENT_";

  private final SparkSession spark;

  // Source Fields
  private final CatalogTable sourceTable;
  private final String sourceTableLocation;
  private final CatalogPlugin sourceCatalog;
  private final Identifier sourceTableName;
  private final PartitionSpec sourcePartitionSpec;

  // Destination Fields
  private final Boolean sessionCatalogReplacement;
  private final CatalogPlugin destCatalog;
  private final Identifier destTableName;

  // Optional Parameters for destination
  private String destDataLocation;
  private String destMetadataLocation;
  private Map<String, String> additionalProperties = Maps.newHashMap();

  Spark3CreateAction(SparkSession spark, CatalogPlugin sourceCatalog, Identifier sourceTableName,
                       CatalogPlugin destCatalog,  Identifier destTableName) {

    this.spark = spark;
    this.sourceCatalog = checkSourceCatalog(sourceCatalog);
    this.sourceTableName = sourceTableName;
    this.destCatalog = destCatalog;
    this.destTableName = destTableName;

    try {
      String sourceString = String.join(".", sourceTableName.namespace()) + "." + sourceTableName.name();
      sourcePartitionSpec = SparkSchemaUtil.specForTable(spark, sourceString);
    } catch (AnalysisException e) {
      throw new IllegalArgumentException("Cannot determining partitioning of " + sourceTableName.toString(), e);
    }

    try {
      this.sourceTable = spark.sessionState().catalog().getTableMetadata(Spark3Util.toTableIdentifier(sourceTableName));
    } catch (NoSuchTableException | NoSuchDatabaseException e) {
      throw new IllegalArgumentException(String.format("Could not find source table %s", sourceTableName), e);
    }
    validateSourceTable(sourceTable);

    this.sessionCatalogReplacement = isSessionCatalogReplacement();

    this.sourceTableLocation = CatalogUtils.URIToString(sourceTable.storage().locationUri().get());
    this.destDataLocation = sourceTableLocation;
    this.destMetadataLocation = sourceTableLocation + "/" + ICEBERG_METADATA_FOLDER;
  }

  private boolean isSessionCatalogReplacement() {
    boolean sourceIceberg = sourceTable.provider().get().toLowerCase(Locale.ROOT).equals("iceberg");
    boolean sameCatalog = sourceCatalog == destCatalog;
    boolean sameIdentifier = sourceTableName.name().equals(destTableName.name()) &&
        Arrays.equals(sourceTableName.namespace(), destTableName.namespace());
    return !sourceIceberg && sameCatalog && sameIdentifier;
  }


  /**
   * Creates the Iceberg data and metadata at a given location instead of the source table
   * location. New metadata and data files will be added to this
   * new location and further operations will not effect the source table.
   *
   * @param newLocation the base directory for the new Iceberg Table
   * @return this for chaining
   */
  CreateAction asSnapshotAtLocation(String newLocation) {
    Preconditions.checkArgument(!newLocation.equals(sourceTableLocation), "Cannot create a snapshot with the" +
        "same data location as the source table. To place new files in the source table directory use the migrate " +
        "command.");
    this.destDataLocation = newLocation;
    this.destMetadataLocation = newLocation + "/" + ICEBERG_METADATA_FOLDER;
    return this;
  }

  /**
   * Creates the Iceberg data and metadata at the catalog default location for the
   * new table.
   *
   * @return this for chaining
   */
  CreateAction asSnapshotAtDefaultLocation() {
    this.destDataLocation = null;
    this.destMetadataLocation = null;
    return this;
  }

  @Override
  public Long execute() {
    StagingTableCatalog stagingCatalog = checkDestinationCatalog(destCatalog);
    Map<String, String> newTableProperties = new ImmutableMap.Builder<String, String>()
        .put(TableCatalog.PROP_PROVIDER, "iceberg")
        .putAll(JavaConverters.mapAsJavaMapConverter(sourceTable.properties()).asJava())
        .putAll(extraIcebergTableProps(destDataLocation, destMetadataLocation))
        .putAll(additionalProperties)
        .build();

    StagedTable stagedTable;
    try {
      if (sessionCatalogReplacement) {
        /*
         * Spark Session Catalog cannot stage a replacement of a Session table with an Iceberg Table.
         * To workaround this we create a replacement table which is renamed after it
         * is successfully constructed.
         */
        stagedTable = stagingCatalog.stageCreate(Identifier.of(
            destTableName.namespace(),
            destTableName.name() + REPLACEMENT_NAME),
            sourceTable.schema(),
            Spark3Util.toTransforms(sourcePartitionSpec), newTableProperties);
      } else {
        stagedTable = stagingCatalog.stageCreate(destTableName, sourceTable.schema(),
            Spark3Util.toTransforms(sourcePartitionSpec), newTableProperties);
      }
    } catch (NoSuchNamespaceException e) {
      throw new IllegalArgumentException("Cannot create a new table in a namespace which does not exist", e);
    } catch (TableAlreadyExistsException e) {
      throw new IllegalArgumentException("Destination table already exists", e);
    }

    Table icebergTable = ((SparkTable) stagedTable).table();

    String stagingLocation;
    if (destMetadataLocation != null) {
      stagingLocation = destMetadataLocation;
    } else {
      stagingLocation = ((SparkTable) stagedTable).table().location() + "/" + ICEBERG_METADATA_FOLDER;
    }

    LOG.info("Beginning migration of {} to {} using metadata location {}", sourceTableName, destTableName,
        stagingLocation);

    long numMigratedFiles;
    try {
      SparkTableUtil.importSparkTable(spark, Spark3Util.toTableIdentifier(sourceTableName), icebergTable,
          stagingLocation);

      Snapshot snapshot = icebergTable.currentSnapshot();
      numMigratedFiles = Long.valueOf(snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));

      stagedTable.commitStagedChanges();
      LOG.info("Successfully loaded Iceberg metadata for {} files", numMigratedFiles);
    } catch (Exception e) {
      LOG.error("Error when attempting to commit migration changes, rolling back", e);
      stagedTable.abortStagedChanges();
      throw e;
    }

    if (sessionCatalogReplacement) {
      Identifier replacementTable = Identifier.of(destTableName.namespace(), destTableName.name() + REPLACEMENT_NAME);
      try {
        stagingCatalog.dropTable(destTableName);
        stagingCatalog.renameTable(replacementTable, destTableName);
      } catch (NoSuchTableException e) {
        LOG.error("Cannot migrate, replacement table is missing. Attempting to recreate source table", e);
        try {
          stagingCatalog.createTable(sourceTableName, sourceTable.schema(),
              Spark3Util.toTransforms(sourcePartitionSpec), JavaConverters.mapAsJavaMap(sourceTable.properties()));
        } catch (TableAlreadyExistsException tableAlreadyExistsException) {
          Log.error("Cannot recreate source table. Source table has already been recreated", e);
          throw new RuntimeException(e);
        } catch (NoSuchNamespaceException noSuchNamespaceException) {
          Log.error("Cannot recreate source table. Source namespace has been removed, cannot recreate", e);
          throw new RuntimeException(e);
        }
      } catch (TableAlreadyExistsException e) {
        Log.error("Cannot migrate, Source table was recreated before replacement could be moved. " +
            "Attempting to remove replacement table.", e);
        stagingCatalog.dropTable(replacementTable);
        stagedTable.abortStagedChanges();
      }
    }

    return numMigratedFiles;
  }

  @Override
  public CreateAction set(Map<String, String> properties) {
    this.additionalProperties.putAll(properties);
    return this;
  }

  @Override
  public CreateAction set(String key, String value) {
    this.additionalProperties.put(key, value);
    return this;
  }

  private static void validateSourceTable(CatalogTable sourceTable) {
    String sourceTableProvider = sourceTable.provider().get().toLowerCase(Locale.ROOT);

    if (!ALLOWED_SOURCES.contains(sourceTableProvider)) {
      throw new IllegalArgumentException(
          String.format("Cannot create an Iceberg table from source provider: %s", sourceTableProvider));
    }
    if (sourceTable.storage().locationUri().isEmpty()) {
      throw new IllegalArgumentException("Cannot create an Iceberg table from a source without an explicit location");
    }
  }

  private static Map<String, String> extraIcebergTableProps(String tableLocation, String metadataLocation) {
    if (tableLocation != null && metadataLocation != null) {
      return ImmutableMap.of(
          TableProperties.WRITE_METADATA_LOCATION, metadataLocation,
          TableProperties.WRITE_NEW_DATA_LOCATION, tableLocation,
          "migrated", "true"
      );
    } else {
      return ImmutableMap.of("migrated", "true");
    }
  }

  private static StagingTableCatalog checkDestinationCatalog(CatalogPlugin catalog) {
    if (!(catalog instanceof SparkSessionCatalog) && !(catalog instanceof SparkCatalog)) {
      throw new IllegalArgumentException(String.format("Cannot create Iceberg table in non Iceberg Catalog. " +
              "Catalog %s was of class %s but %s or %s are required", catalog.name(), catalog.getClass(),
          SparkSessionCatalog.class.getName(), SparkCatalog.class.getName()));
    }
    return (StagingTableCatalog) catalog;
  }

  private CatalogPlugin checkSourceCatalog(CatalogPlugin catalog) {
    // Currently the Import code relies on being able to look up the table in the session code
    if (!catalog.name().equals("spark_catalog")) {
      throw new IllegalArgumentException(String.format(
          "Cannot create an Iceberg table from a non-Session Catalog table. " +
              "Found %s of class %s as the source catalog o", catalog.name(), catalog.getClass().getName()));
    }
    return catalog;
  }

}
