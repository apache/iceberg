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
class Spark3SnapshotAction implements CreateAction {
  private static final Logger LOG = LoggerFactory.getLogger(Spark3SnapshotAction.class);
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

  Spark3SnapshotAction(SparkSession spark, CatalogPlugin sourceCatalog, Identifier sourceTableName,
                       CatalogPlugin destCatalog, Identifier destTableName) {
  @Override
  public Long execute() {
    StagingTableCatalog stagingCatalog = checkDestinationCatalog(destCatalog);
    Map<String, String> newTableProperties = new ImmutableMap.Builder<String, String>()
        .put(TableCatalog.PROP_PROVIDER, "iceberg")
        .putAll(JavaConverters.mapAsJavaMapConverter(sourceTable.properties()).asJava())
        .putAll(extraIcebergTableProps(destDataLocation, destMetadataLocation))
        //put SnapshotProperty
        .putAll(additionalProperties)
        .build();

    StagedTable stagedTable;
    try {

      stagedTable = stagingCatalog.stageCreate(destTableName, sourceTable.schema(),
          Spark3Util.toTransforms(sourcePartitionSpec), newTableProperties);
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
}
