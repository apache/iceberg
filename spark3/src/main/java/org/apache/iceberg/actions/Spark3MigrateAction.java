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
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.source.StagedSparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
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
public class Spark3MigrateAction extends Spark3CreateAction {
  private static final Logger LOG = LoggerFactory.getLogger(Spark3MigrateAction.class);
  private static final String BACKUP_SUFFIX = "_BACKUP_";

  public Spark3MigrateAction(SparkSession spark, CatalogPlugin sourceCatalog, Identifier sourceTableName) {
    super(spark, sourceCatalog, sourceTableName, sourceCatalog, sourceTableName);
  }

  private Long doExecute() {
    // Move source table to a new name, halting all modifications and allowing us to stage
    // the creation of a new Iceberg table in its place
    String backupName = sourceTableIdent().name() + BACKUP_SUFFIX;
    Identifier backupIdentifier = Identifier.of(sourceTableIdent().namespace(), backupName);
    try {
      destCatalog().renameTable(sourceTableIdent(), backupIdentifier);
    } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
      throw new NoSuchTableException("Cannot find table '%s' to migrate", sourceTableIdent());
    } catch (org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException e) {
      throw new AlreadyExistsException("Cannot rename migration source '%s' to backup name '%s'." +
          " Backup table already exists.", sourceTableIdent(), backupIdentifier);
    }

    StagedSparkTable stagedTable = null;
    Table icebergTable;
    boolean threw = true;
    try {
      stagedTable = stageDestTable();
      icebergTable = stagedTable.table();

      ensureNameMappingPresent(icebergTable);

      String stagingLocation = getMetadataLocation(icebergTable);

      LOG.info("Beginning migration of {} using metadata location {}", sourceTableIdent(), stagingLocation);
      Some<String> backupNamespace = Some.apply(backupIdentifier.namespace()[0]);
      TableIdentifier v1BackupIdentifier = new TableIdentifier(backupIdentifier.name(), backupNamespace);
      SparkTableUtil.importSparkTable(spark(), v1BackupIdentifier, icebergTable, stagingLocation);

      stagedTable.commitStagedChanges();
      threw = false;
    } finally {
      if (threw) {
        LOG.error("Error when attempting perform migration changes, aborting table creation and restoring backup.");

        try {
          destCatalog().renameTable(backupIdentifier, sourceTableIdent());
        } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException nstException) {
          LOG.error("Cannot restore backup '{}', the backup cannot be found", backupIdentifier, nstException);
        } catch (org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException taeException) {
          LOG.error("Cannot restore backup, a table with the original name " +
              "exists. The backup can be found with the name '{}'", backupIdentifier, taeException);
        }

        try {
          stagedTable.abortStagedChanges();
        } catch (Exception abortException) {
          LOG.error("Cannot abort staged changes", abortException);
        }
      }
    }

    Snapshot snapshot = icebergTable.currentSnapshot();
    long numMigratedFiles = Long.parseLong(snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    LOG.info("Successfully loaded Iceberg metadata for {} files", numMigratedFiles);
    return numMigratedFiles;
  }

  @Override
  public Long execute() {
    JobGroupInfo info = new JobGroupInfo("MIGRATE", "MIGRATE", false);
    return withJobGroupInfo(info, this::doExecute);
  }

  @Override
  protected Map<String, String> targetTableProps() {
    Map<String, String> properties = Maps.newHashMap();

    properties.putAll(JavaConverters.mapAsJavaMapConverter(v1SourceTable().properties()).asJava());
    EXCLUDED_PROPERTIES.forEach(properties::remove);

    properties.put(TableCatalog.PROP_PROVIDER, "iceberg");
    properties.put("migrated", "true");
    properties.putAll(additionalProperties());
    properties.putIfAbsent(LOCATION, sourceTableLocation());

    return properties;
  }

  @Override
  protected TableCatalog checkSourceCatalog(CatalogPlugin catalog) {
    // Currently the Import code relies on being able to look up the table in the session code
    Preconditions.checkArgument(catalog instanceof SparkSessionCatalog,
        "Cannot migrate a table from a non-Iceberg Spark Session Catalog. Found %s of class %s as the source catalog.",
        catalog.name(), catalog.getClass().getName());

    return (TableCatalog) catalog;
  }
}
