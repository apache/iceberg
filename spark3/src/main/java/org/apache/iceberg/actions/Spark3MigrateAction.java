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

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Some;
import scala.collection.JavaConverters;

/**
 * Takes a Spark table in the sourceCatalog and attempts to transform it into an Iceberg
 * Table in the same location with the same identifier. Once complete the identifier which
 * previously referred to a non-iceberg table will refer to the newly migrated iceberg
 * table.
 */
class Spark3MigrateAction extends Spark3CreateAction {
  private static final Logger LOG = LoggerFactory.getLogger(Spark3MigrateAction.class);
  private static final String BACKUP_SUFFIX = "_BACKUP_";

  Spark3MigrateAction(SparkSession spark, CatalogPlugin sourceCatalog, Identifier sourceTableName) {
    super(spark, sourceCatalog, sourceTableName, sourceCatalog, sourceTableName);
  }

  @Override
  public Long execute() {
    Table icebergTable;

    // Move source table to a new name, halting all modifications and allowing us to stage
    // the creation of a new Iceberg table in its place
    String backupName = sourceTableName().name() + BACKUP_SUFFIX;
    Identifier backupIdentifier = Identifier.of(sourceTableName().namespace(), backupName);
    try {
      destCatalog().renameTable(sourceTableName(), backupIdentifier);
    } catch (NoSuchTableException e) {
      throw new IllegalArgumentException("Cannot find table to migrate", e);
    } catch (TableAlreadyExistsException e) {
      throw new IllegalArgumentException("Cannot rename migration source to backup name", e);
    }

    StagedTable stagedTable = null;
    boolean threw = true;
    try {
      Map<String, String> newTableProperties = new HashMap<>();
      newTableProperties.put(TableCatalog.PROP_PROVIDER, "iceberg");
      newTableProperties.putAll(JavaConverters.mapAsJavaMapConverter(v1SourceTable().properties()).asJava());
      newTableProperties.putAll(tableLocationProperties(sourceTableLocation()));
      newTableProperties.putAll(additionalProperties());

      stagedTable = destCatalog().stageCreate(Identifier.of(
          destTableName().namespace(),
          destTableName().name()),
          v1SourceTable().schema(),
          sourcePartitionSpec(),
          newTableProperties);

      icebergTable = ((SparkTable) stagedTable).table();

      if (!stagedTable.properties().containsKey(TableProperties.DEFAULT_NAME_MAPPING)) {
        assignDefaultTableNameMapping(icebergTable);
      }

      String stagingLocation = stagedTable.properties().get(TableProperties.WRITE_METADATA_LOCATION);

      LOG.info("Beginning migration of {} using metadata location {}", sourceTableName(), stagingLocation);

      Some<String> backupNamespace = Some.apply(backupIdentifier.namespace()[0]);
      TableIdentifier v1BackupIdentifier = new TableIdentifier(backupIdentifier.name(), backupNamespace);
      SparkTableUtil.importSparkTable(spark(), v1BackupIdentifier, icebergTable, stagingLocation);

      stagedTable.commitStagedChanges();
      threw = false;

    } catch (TableAlreadyExistsException tableAlreadyExistsException) {
      throw new IllegalArgumentException("Cannot migrate because a table was created with the same name as the " +
          "original was created after we renamed the original table to a backup identifier.",
          tableAlreadyExistsException);
    } catch (NoSuchNamespaceException noSuchNamespaceException) {
      throw new IllegalArgumentException("Cannot migrate because the namespace for the table no longer exists after" +
          "we renamed the original table to a backup identifier.",
          noSuchNamespaceException);
    } finally {

      if (threw) {
        LOG.error("Error when attempting perform migration changes, aborting table creation and restoring backup.");
        try {
          destCatalog().renameTable(backupIdentifier, sourceTableName());
        } catch (NoSuchTableException nstException) {
          throw new IllegalArgumentException("Cannot restore backup, the backup cannot be found", nstException);
        } catch (TableAlreadyExistsException taeException) {
          throw new IllegalArgumentException(String.format("Cannot restore backup, a table with the original name " +
              "exists. The backup can be found with the name %s", backupIdentifier.toString()), taeException);
        }

        try {
          if (stagedTable != null) {
            stagedTable.abortStagedChanges();
          }
        } catch (Exception abortException) {
          LOG.error("Unable to abort staged changes", abortException);
        }
      }
    }

    Snapshot snapshot = icebergTable.currentSnapshot();
    long numMigratedFiles = Long.valueOf(snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    LOG.info("Successfully loaded Iceberg metadata for {} files", numMigratedFiles);
    return numMigratedFiles;
  }

  @Override
  protected TableCatalog checkSourceCatalog(CatalogPlugin catalog) {
    // Currently the Import code relies on being able to look up the table in the session code
    if (!(catalog instanceof SparkSessionCatalog)) {
      throw new IllegalArgumentException(String.format(
          "Cannot migrate a table from a non-Iceberg Spark Session Catalog. " +
              "Found %s of class %s as the source catalog.", catalog.name(), catalog.getClass().getName()));
    }
    return (TableCatalog) catalog;
  }
}
