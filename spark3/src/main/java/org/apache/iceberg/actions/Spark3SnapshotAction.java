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

import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
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
class Spark3SnapshotAction extends Spark3CreateAction {
  private static final Logger LOG = LoggerFactory.getLogger(Spark3SnapshotAction.class);

  private final String destTableLocation;

  Spark3SnapshotAction(SparkSession spark, CatalogPlugin sourceCatalog,
                       Identifier sourceTableName, CatalogPlugin destCatalog,
                       Identifier destTableName) {
    super(spark, sourceCatalog, sourceTableName, destCatalog, destTableName);
    this.destTableLocation = null;
  }

  Spark3SnapshotAction(SparkSession spark, CatalogPlugin sourceCatalog,
                       Identifier sourceTableName, CatalogPlugin destCatalog,
                       Identifier destTableName, String destTableLocation) {
    super(spark, sourceCatalog, sourceTableName, destCatalog, destTableName);
    this.destTableLocation = destTableLocation;
  }

  @Override
  public Long execute() {
    Preconditions.checkArgument(!sourceTableLocation().equals(destTableLocation),
        "Cannot create snapshot where destination location is the same as the source location. This" +
            "would cause a mixing of original table created and snapshot created files.");
    StagingTableCatalog stagingCatalog = checkDestinationCatalog();

    StagedTable stagedTable = null;
    Table icebergTable = null;

    try {
      stagedTable = stagingCatalog.stageCreate(destTableName(), v1SourceTable().schema(),
          sourcePartitionSpec(), buildPropertyMap());
      icebergTable = ((SparkTable) stagedTable).table();

      if (!icebergTable.properties().containsKey(TableProperties.DEFAULT_NAME_MAPPING)) {
        applyDefaultTableNameMapping(icebergTable);
      }
    } catch (TableAlreadyExistsException taeException) {
      throw new IllegalArgumentException("Cannot create snapshot because a table already exists with that name",
          taeException);
    } catch (NoSuchNamespaceException nsnException) {
      throw new IllegalArgumentException("Cannot create snapshot because the namespace given does not exist",
          nsnException);
    }

    try {
      String stagingLocation = icebergTable.location() + "/" + ICEBERG_METADATA_FOLDER;
      LOG.info("Beginning snapshot of {} to {} using metadata location {}", sourceTableName(), destTableName(),
          stagingLocation);

      TableIdentifier v1TableIdentifier = v1SourceTable().identifier();
      SparkTableUtil.importSparkTable(spark(), v1TableIdentifier, icebergTable, stagingLocation);
      stagedTable.commitStagedChanges();

    } catch (Exception e) {
      LOG.error("Error when attempting to commit snapshot changes, rolling back", e);
      if (stagedTable != null) {
        stagedTable.abortStagedChanges();
      }
      throw e;
    }

    long numMigratedFiles;
    Snapshot snapshot = icebergTable.currentSnapshot();
    numMigratedFiles = Long.valueOf(snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    LOG.info("Successfully loaded Iceberg metadata for {} files", numMigratedFiles);
    return numMigratedFiles;
  }

  private Map<String, String> buildPropertyMap() {
    ImmutableMap.Builder<String, String> propBuilder = ImmutableMap.<String, String>builder()
        .put(TableCatalog.PROP_PROVIDER, "iceberg")
        .putAll(JavaConverters.mapAsJavaMapConverter(v1SourceTable().properties()).asJava())
        .putAll(additionalProperties());

    // Don't use the default location for the destination table if an alternate is specified in the constructor
    if (destTableLocation != null) {
      propBuilder.putAll(tableLocationProperties(destTableLocation));
    }

    return propBuilder.build();
  }
}
