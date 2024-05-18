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
package org.apache.iceberg.spark.actions;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.ImmutableSnapshotTable;
import org.apache.iceberg.actions.SnapshotTable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.Spark3Util.CatalogAndIdentifier;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.source.StagedSparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

/**
 * Creates a new Iceberg table based on a source Spark table. The new Iceberg table will have a
 * different data and metadata directory allowing it to exist independently of the source table.
 */
public class SnapshotTableSparkAction extends BaseTableCreationSparkAction<SnapshotTableSparkAction>
    implements SnapshotTable {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotTableSparkAction.class);

  private StagingTableCatalog destCatalog;
  private Identifier destTableIdent;
  private String destTableLocation = null;
  private ExecutorService executorService;

  SnapshotTableSparkAction(
      SparkSession spark, CatalogPlugin sourceCatalog, Identifier sourceTableIdent) {
    super(spark, sourceCatalog, sourceTableIdent);
  }

  @Override
  protected SnapshotTableSparkAction self() {
    return this;
  }

  @Override
  protected StagingTableCatalog destCatalog() {
    return destCatalog;
  }

  @Override
  protected Identifier destTableIdent() {
    return destTableIdent;
  }

  @Override
  public SnapshotTableSparkAction as(String ident) {
    String ctx = "snapshot destination";
    CatalogPlugin defaultCatalog = spark().sessionState().catalogManager().currentCatalog();
    CatalogAndIdentifier catalogAndIdent =
        Spark3Util.catalogAndIdentifier(ctx, spark(), ident, defaultCatalog);
    this.destCatalog = checkDestinationCatalog(catalogAndIdent.catalog());
    this.destTableIdent = catalogAndIdent.identifier();
    return this;
  }

  @Override
  public SnapshotTableSparkAction tableProperties(Map<String, String> properties) {
    setProperties(properties);
    return this;
  }

  @Override
  public SnapshotTableSparkAction tableProperty(String property, String value) {
    setProperty(property, value);
    return this;
  }

  @Override
  public SnapshotTableSparkAction executeWith(ExecutorService service) {
    this.executorService = service;
    return this;
  }

  @Override
  public SnapshotTable.Result execute() {
    String desc = String.format("Snapshotting table %s as %s", sourceTableIdent(), destTableIdent);
    JobGroupInfo info = newJobGroupInfo("SNAPSHOT-TABLE", desc);
    return withJobGroupInfo(info, this::doExecute);
  }

  private SnapshotTable.Result doExecute() {
    Preconditions.checkArgument(
        destCatalog() != null && destTableIdent() != null,
        "The destination catalog and identifier cannot be null. "
            + "Make sure to configure the action with a valid destination table identifier via the `as` method.");

    LOG.info(
        "Staging a new Iceberg table {} as a snapshot of {}", destTableIdent(), sourceTableIdent());
    StagedSparkTable stagedTable = stageDestTable();
    Table icebergTable = stagedTable.table();

    // TODO: Check the dest table location does not overlap with the source table location

    boolean threw = true;
    try {
      LOG.info("Ensuring {} has a valid name mapping", destTableIdent());
      ensureNameMappingPresent(icebergTable);

      TableIdentifier v1TableIdent = v1SourceTable().identifier();
      String stagingLocation = getMetadataLocation(icebergTable);
      LOG.info("Generating Iceberg metadata for {} in {}", destTableIdent(), stagingLocation);
      SparkTableUtil.importSparkTable(
          spark(), v1TableIdent, icebergTable, stagingLocation, executorService);

      LOG.info("Committing staged changes to {}", destTableIdent());
      stagedTable.commitStagedChanges();
      threw = false;
    } finally {
      if (threw) {
        LOG.error("Error when populating the staged table with metadata, aborting changes");

        try {
          stagedTable.abortStagedChanges();
        } catch (Exception abortException) {
          LOG.error("Cannot abort staged changes", abortException);
        }
      }
    }

    Snapshot snapshot = icebergTable.currentSnapshot();
    long importedDataFilesCount =
        Long.parseLong(snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    LOG.info(
        "Successfully loaded Iceberg metadata for {} files to {}",
        importedDataFilesCount,
        destTableIdent());
    return ImmutableSnapshotTable.Result.builder()
        .importedDataFilesCount(importedDataFilesCount)
        .build();
  }

  @Override
  protected Map<String, String> destTableProps() {
    Map<String, String> properties = Maps.newHashMap();

    // copy over relevant source table props
    properties.putAll(JavaConverters.mapAsJavaMapConverter(v1SourceTable().properties()).asJava());
    EXCLUDED_PROPERTIES.forEach(properties::remove);

    // remove any possible location properties from origin properties
    properties.remove(LOCATION);
    properties.remove(TableProperties.WRITE_METADATA_LOCATION);
    properties.remove(TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
    properties.remove(TableProperties.OBJECT_STORE_PATH);
    properties.remove(TableProperties.WRITE_DATA_LOCATION);

    // set default and user-provided props
    properties.put(TableCatalog.PROP_PROVIDER, "iceberg");
    properties.putAll(additionalProperties());

    // make sure we mark this table as a snapshot table
    properties.put(TableProperties.GC_ENABLED, "false");
    properties.put("snapshot", "true");

    // set the destination table location if provided
    if (destTableLocation != null) {
      properties.put(LOCATION, destTableLocation);
    }

    return properties;
  }

  @Override
  protected TableCatalog checkSourceCatalog(CatalogPlugin catalog) {
    // currently the import code relies on being able to look up the table in the session catalog
    Preconditions.checkArgument(
        catalog.name().equalsIgnoreCase("spark_catalog"),
        "Cannot snapshot a table that isn't in the session catalog (i.e. spark_catalog). "
            + "Found source catalog: %s.",
        catalog.name());

    Preconditions.checkArgument(
        catalog instanceof TableCatalog,
        "Cannot snapshot as catalog %s of class %s in not a table catalog",
        catalog.name(),
        catalog.getClass().getName());

    return (TableCatalog) catalog;
  }

  @Override
  public SnapshotTableSparkAction tableLocation(String location) {
    Preconditions.checkArgument(
        !sourceTableLocation().equals(location),
        "The snapshot table location cannot be same as the source table location. "
            + "This would mix snapshot table files with original table files.");
    this.destTableLocation = location;
    return this;
  }
}
