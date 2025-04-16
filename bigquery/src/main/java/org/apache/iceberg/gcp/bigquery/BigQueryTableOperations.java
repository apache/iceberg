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
package org.apache.iceberg.gcp.bigquery;

import com.google.api.services.bigquery.model.ExternalCatalogTableOptions;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.iceberg.BaseMetastoreOperations;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles BigQuery metastore table operations. */
public final class BigQueryTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableOperations.class);

  public static final String TABLE_PROPERTIES_BQ_CONNECTION = "bq_connection";

  private final BigQueryMetaStoreClient client;
  private final FileIO fileIO;
  private final TableReference tableReference;
  private final Configuration conf;

  BigQueryTableOperations(
      BigQueryMetaStoreClient client,
      FileIO fileIO,
      String project,
      String dataset,
      String table,
      Configuration conf) {
    this.client = client;
    this.fileIO = fileIO;
    this.tableReference =
        new TableReference().setProjectId(project).setDatasetId(dataset).setTableId(table);
    this.conf = conf;
  }

  // The doRefresh method should provide implementation on how to get the metadata location.
  @Override
  public void doRefresh() {
    // Must default to null.
    String metadataLocation = null;
    try {
      metadataLocation =
          getMetadataLocationOrThrow(
              client.getTable(this.tableReference).getExternalCatalogTableOptions());
    } catch (NoSuchTableException e) {
      if (currentMetadataLocation() != null) {
        // Re-throws the exception because the table must exist in this case.
        throw e;
      }
    }
    refreshFromMetadataLocation(metadataLocation);
  }

  // The doCommit method should provide implementation on how to update with metadata location
  // atomically
  @Override
  public void doCommit(TableMetadata base, TableMetadata metadata) {
    String newMetadataLocation =
        base == null && metadata.metadataFileLocation() != null
            ? metadata.metadataFileLocation()
            : writeNewMetadata(metadata, currentVersion() + 1);
    BaseMetastoreOperations.CommitStatus commitStatus =
        BaseMetastoreOperations.CommitStatus.FAILURE;
    try {
      if (base == null) {
        createTable(newMetadataLocation, metadata);
      } else {
        updateTable(base.metadataFileLocation(), newMetadataLocation, metadata);
      }
      commitStatus = BaseMetastoreOperations.CommitStatus.SUCCESS;
    } catch (CommitFailedException | CommitStateUnknownException e) {
      throw e;
    } catch (Throwable e) {
      LOG.error("Exception thrown on commit: ", e);

      if (commitStatus == BaseMetastoreOperations.CommitStatus.FAILURE
          && e instanceof AlreadyExistsException) {
        throw e;
      }

      commitStatus =
          BaseMetastoreOperations.CommitStatus.valueOf(
              checkCommitStatus(newMetadataLocation, metadata).name());
      if (commitStatus == BaseMetastoreOperations.CommitStatus.FAILURE) {
        throw new CommitFailedException(e, "Failed to commit");
      }
      if (commitStatus == BaseMetastoreOperations.CommitStatus.UNKNOWN) {
        throw new CommitStateUnknownException(e);
      }
    } finally {
      try {
        if (commitStatus == BaseMetastoreOperations.CommitStatus.FAILURE) {
          LOG.warn("Failed to commit updates to table {}", tableName());
          io().deleteFile(newMetadataLocation);
        }
      } catch (RuntimeException e) {
        LOG.error(
            "Failed to cleanup metadata file at {} for table {}",
            newMetadataLocation,
            tableName(),
            e);
      }
    }
  }

  @Override
  public String tableName() {
    return String.format("%s.%s", tableReference.getDatasetId(), tableReference.getTableId());
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  private void createTable(String newMetadataLocation, TableMetadata metadata) {
    LOG.debug("Creating a new Iceberg table: {}", tableName());
    Table tableBuilder = makeNewTable(metadata, newMetadataLocation);
    tableBuilder.setTableReference(this.tableReference);
    addConnectionIfProvided(tableBuilder, metadata.properties());

    client.createTable(tableBuilder);
  }

  private void addConnectionIfProvided(Table tableBuilder, Map<String, String> metadataProperties) {
    if (metadataProperties.containsKey(TABLE_PROPERTIES_BQ_CONNECTION)) {
      tableBuilder
          .getExternalCatalogTableOptions()
          .setConnectionId(metadataProperties.get(TABLE_PROPERTIES_BQ_CONNECTION));
    }
  }

  /** Update table properties with concurrent update detection using etag. */
  private void updateTable(
      String oldMetadataLocation, String newMetadataLocation, TableMetadata metadata) {
    Table table = client.getTable(this.tableReference);
    if (table.getEtag().isEmpty()) {
      throw new ValidationException(
          "Etag of legacy table %s is empty, manually update the table via the BigQuery API or"
              + " recreate and retry",
          tableName());
    }
    ExternalCatalogTableOptions options = table.getExternalCatalogTableOptions();
    addConnectionIfProvided(table, metadata.properties());

    // If `metadataLocationFromMetastore` is different from metadata location of base, it means
    // someone has updated metadata location in metastore, which is a conflict update.
    String metadataLocationFromMetastore =
        options.getParameters().getOrDefault(METADATA_LOCATION_PROP, "");
    if (!metadataLocationFromMetastore.isEmpty()
        && !metadataLocationFromMetastore.equals(oldMetadataLocation)) {
      throw new CommitFailedException(
          "Cannot commit base metadata location '%s' is not same as the current table metadata location '%s' for"
              + " %s.%s",
          oldMetadataLocation,
          metadataLocationFromMetastore,
          tableReference.getDatasetId(),
          tableReference.getTableId());
    }

    options.setParameters(buildTableParameters(newMetadataLocation, metadata));
    try {
      client.patchTable(tableReference, table);
    } catch (ValidationException e) {
      if (e.getMessage().toLowerCase().contains("etag mismatch")) {
        throw new CommitFailedException(
            "Updating table failed due to conflict updates (etag mismatch). Retry the update");
      }

      throw e;
    }
  }

  // To make the table queryable from Hive, the user would likely be setting the HIVE_ENGINE_ENABLED
  // parameter.
  //
  // TODO(b/318693532): We need to make a decision on how to make the table queryable from Hive.
  // (could be a server side change or a client side change - that's TBD).
  private Table makeNewTable(TableMetadata metadata, String metadataFileLocation) {
    boolean hiveEngineEnabled = getHiveEngineEnabled(metadata);
    return new Table()
        .setExternalCatalogTableOptions(
            BigQueryMetastoreUtils.createExternalCatalogTableOptions(
                metadata.location(),
                buildTableParameters(metadataFileLocation, metadata),
                hiveEngineEnabled));
  }

  // Follow Iceberg's HiveTableOperations to populate more table parameters for HMS compatibility.
  private Map<String, String> buildTableParameters(
      String metadataFileLocation, TableMetadata metadata) {
    Map<String, String> parameters = Maps.newHashMap(metadata.properties());
    if (metadata.uuid() != null) {
      parameters.put(TableProperties.UUID, metadata.uuid());
    }
    if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
      parameters.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
    }
    parameters.put(METADATA_LOCATION_PROP, metadataFileLocation);
    parameters.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE);
    // Follow HMS to use the EXTERNAL type.
    parameters.put("EXTERNAL", "TRUE");

    // Hive style basic statistics.
    updateParametersWithSnapshotMetadata(metadata, parameters);
    // More Iceberg metadata can be exposed, e.g., statistic, schema, partition spec, as HMS do. But
    // we should be careful that these metadata could be huge and make the metadata API response
    // less readable (e.g., list tables). Users can always inspect these metadata in Spark, so they
    // are not set for now.
    return parameters;
  }

  /** Adds Hive-style basic statistics from snapshot metadata if it exists. */
  private static void updateParametersWithSnapshotMetadata(
      TableMetadata metadata, Map<String, String> parameters) {
    if (metadata.currentSnapshot() == null) {
      return;
    }

    Map<String, String> summary = metadata.currentSnapshot().summary();
    if (summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP) != null) {
      parameters.put(StatsSetupConst.NUM_FILES, summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    }
    if (summary.get(SnapshotSummary.TOTAL_RECORDS_PROP) != null) {
      parameters.put(StatsSetupConst.ROW_COUNT, summary.get(SnapshotSummary.TOTAL_RECORDS_PROP));
    }
    if (summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP) != null) {
      parameters.put(StatsSetupConst.TOTAL_SIZE, summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP));
    }
  }

  private String getMetadataLocationOrThrow(ExternalCatalogTableOptions tableOptions) {
    if (tableOptions == null || !tableOptions.getParameters().containsKey(METADATA_LOCATION_PROP)) {
      throw new ValidationException(
          "Table %s is not a valid BigQuery Metastore Iceberg table, metadata location not found",
          tableName());
    }
    return tableOptions.getParameters().get(METADATA_LOCATION_PROP);
  }

  /**
   * Returns if the hive engine related values should be enabled on the table, or not.
   *
   * <p>The decision is made like this:
   *
   * <ol>
   *   <li>Table property value {@link TableProperties#ENGINE_HIVE_ENABLED}
   *   <li>If the table property is not set then check the hive-site.xml property value {@link
   *       ConfigProperties#ENGINE_HIVE_ENABLED}
   *   <li>If none of the above is enabled then use the default value {@link
   *       TableProperties#ENGINE_HIVE_ENABLED_DEFAULT}
   * </ol>
   *
   * @param metadata Table metadata to use
   * @return if the hive engine related values should be enabled or not
   */
  private boolean getHiveEngineEnabled(TableMetadata metadata) {
    if (metadata.properties().get(TableProperties.ENGINE_HIVE_ENABLED) != null) {
      // We know that the property is set, so default value will not be used,
      return metadata.propertyAsBoolean(TableProperties.ENGINE_HIVE_ENABLED, false);
    }

    return this.conf.getBoolean(
        ConfigProperties.ENGINE_HIVE_ENABLED, TableProperties.ENGINE_HIVE_ENABLED_DEFAULT);
  }
}
