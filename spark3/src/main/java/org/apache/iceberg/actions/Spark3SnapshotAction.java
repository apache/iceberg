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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

/**
 * Creates a new Iceberg table based on a source Spark table. The new Iceberg table will
 * have a different data and metadata directory allowing it to exist independently of the
 * source table.
 */
class Spark3SnapshotAction extends Spark3CreateAction implements SnapshotAction {
  private static final Logger LOG = LoggerFactory.getLogger(Spark3SnapshotAction.class);

  private String destTableLocation = null;

  Spark3SnapshotAction(SparkSession spark, CatalogPlugin sourceCatalog,
                       Identifier sourceTableName, CatalogPlugin destCatalog,
                       Identifier destTableName) {
    super(spark, sourceCatalog, sourceTableName, destCatalog, destTableName);
  }

  @Override
  public Long execute() {
    StagedTable stagedTable;
    Table icebergTable;

    try {
      stagedTable = destCatalog().stageCreate(destTableName(), v1SourceTable().schema(),
          sourcePartitionSpec(), buildPropertyMap());
      icebergTable = ((SparkTable) stagedTable).table();

      if (!icebergTable.properties().containsKey(TableProperties.DEFAULT_NAME_MAPPING)) {
        assignDefaultTableNameMapping(icebergTable);
      }
    } catch (TableAlreadyExistsException taeException) {
      throw new IllegalArgumentException("Cannot create snapshot because a table already exists with that name",
          taeException);
    } catch (NoSuchNamespaceException nsnException) {
      throw new IllegalArgumentException("Cannot create snapshot because the namespace given does not exist",
          nsnException);
    }

    boolean threw = true;
    try {
      String stagingLocation = icebergTable.location() + "/" + ICEBERG_METADATA_FOLDER;
      LOG.info("Beginning snapshot of {} to {} using metadata location {}", sourceTableName(), destTableName(),
          stagingLocation);

      TableIdentifier v1TableIdentifier = v1SourceTable().identifier();
      SparkTableUtil.importSparkTable(spark(), v1TableIdentifier, icebergTable, stagingLocation);
      stagedTable.commitStagedChanges();
      threw = false;
    } finally {
      if (threw) {
        LOG.error("Error when attempting to commit snapshot changes, rolling back");
        if (stagedTable != null) {
          stagedTable.abortStagedChanges();
        }
      }
    }

    long numMigratedFiles;
    Snapshot snapshot = icebergTable.currentSnapshot();
    numMigratedFiles = Long.valueOf(snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    LOG.info("Successfully loaded Iceberg metadata for {} files", numMigratedFiles);
    return numMigratedFiles;
  }

  private Map<String, String> buildPropertyMap() {
    Map<String, String> properties = new HashMap<>();
    properties.put(TableCatalog.PROP_PROVIDER, "iceberg");
    properties.put(TableProperties.GC_ENABLED, "false");
    properties.putAll(JavaConverters.mapAsJavaMapConverter(v1SourceTable().properties()).asJava());
    properties.putAll(additionalProperties());

    // Don't use the default location for the destination table if an alternate has be set
    if (destTableLocation != null) {
      properties.putAll(tableLocationProperties(destTableLocation));
    }

    return properties;
  }

  @Override
  protected TableCatalog checkSourceCatalog(CatalogPlugin catalog) {
    // Currently the Import code relies on being able to look up the table in the session code
    if (!(catalog.name().equals("spark_catalog"))) {
      throw new IllegalArgumentException(String.format(
          "Cannot snapshot a table that isn't in spark_catalog, the session catalog. " +
              "Found source catalog %s", catalog.name()));
    }

    if (!(catalog instanceof TableCatalog)) {
      throw new IllegalArgumentException(String.format(
          "Cannot snapshot a table from a non-table catalog %s. Catalog has class of %s.", catalog.name(),
          catalog.getClass().toString()
      ));
    }

    return (TableCatalog) catalog;
  }

  @Override
  public SnapshotAction withLocation(String location) {
    Preconditions.checkArgument(!sourceTableLocation().equals(location),
        "Cannot create snapshot where destination location is the same as the source location. This" +
            "would cause a mixing of original table created and snapshot created files.");
    this.destTableLocation = location;
    return this;
  }
}
