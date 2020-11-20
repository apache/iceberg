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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
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
class Spark3MigrateAction extends Spark3CreateAction {
  private static final Logger LOG = LoggerFactory.getLogger(Spark3MigrateAction.class);
  private static final String REPLACEMENT_NAME = "_REPLACEMENT_";
  private static final String BACKUP_SUFFIX = "_BACKUP_";


  private Spark3MigrateAction(SparkSession spark, CatalogPlugin sourceCatalog,
                      Identifier sourceTableName, CatalogPlugin destCatalog,
                      Identifier destTableName) {
    super(spark, sourceCatalog, sourceTableName, destCatalog, destTableName);
  }

  Spark3MigrateAction(SparkSession spark, CatalogPlugin sourceCatalog, Identifier sourceTableName) {
    super(spark, sourceCatalog, sourceTableName, sourceCatalog, sourceTableName);
  }

  @Override
  public Long execute() {
    StagingTableCatalog stagingCatalog = checkDestinationCatalog(sourceCatalog);


    Table icebergTable = null;

    String backupName = sourceTableName.name() + BACKUP_SUFFIX;
    Identifier backupIdentifier = Identifier.of(sourceTableName.namespace(), backupName);
    try {
      stagingCatalog.renameTable(sourceTableName, backupIdentifier);
    } catch (NoSuchTableException e) {
      throw new IllegalArgumentException("Cannot find table to migrate", e);
    } catch (TableAlreadyExistsException e) {
      throw new IllegalArgumentException("Cannot rename migration source to backup name", e);
    }

    StagedTable stagedTable = null;
    try {
      Map<String, String> newTableProperties = new ImmutableMap.Builder<String, String>()
          .put(TableCatalog.PROP_PROVIDER, "iceberg")
          .putAll(JavaConverters.mapAsJavaMapConverter(sourceTable.properties()).asJava())
          .putAll(tableLocationProperties(sourceTableLocation))
          .putAll(additionalProperties)
          .build();

      stagedTable = stagingCatalog.stageCreate(Identifier.of(
          destTableName.namespace(),
          destTableName.name() + REPLACEMENT_NAME),
          sourceTable.schema(),
          Spark3Util.toTransforms(sourcePartitionSpec), newTableProperties);

      icebergTable = ((SparkTable) stagedTable).table();

      String stagingLocation;
      stagingLocation = ((SparkTable) stagedTable).table().location() + "/" + ICEBERG_METADATA_FOLDER;

      LOG.info("Beginning migration of {} using metadata location {}", sourceTableName, stagingLocation);

      TableIdentifier v1TableIdentifier = Spark3Util.toTableIdentifier(sourceTableName);
      SparkTableUtil.importSparkTable(spark, v1TableIdentifier, icebergTable, stagingLocation);

      stagedTable.commitStagedChanges();
    } catch (Exception e) {
      LOG.error("Error when attempting perform migration changes, aborting table creation and restoring backup", e);

      try {
        if (stagedTable != null) {
          stagedTable.abortStagedChanges();
        }
      } catch (Exception abortException) {
        LOG.error("Unable to abort staged changes", abortException);
      }

      try {
        stagingCatalog.renameTable(backupIdentifier, sourceTableName);
      } catch (NoSuchTableException nstException) {
        throw new IllegalArgumentException("Cannot restore backup, the backup cannot be found", nstException);
      } catch (TableAlreadyExistsException taeException) {
        throw new IllegalArgumentException(String.format("Cannot restore backup, a table with the original name " +
            "exists. The backup can be found with the name %s", backupIdentifier.toString()), taeException);
      }

      throw new RuntimeException(e);
    }

    Snapshot snapshot = icebergTable.currentSnapshot();
    long numMigratedFiles = Long.valueOf(snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    LOG.info("Successfully loaded Iceberg metadata for {} files", numMigratedFiles);
    return numMigratedFiles;
  }
}
