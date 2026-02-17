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
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.gcp.bigquery.util.RetryDetector;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles BigQuery metastore table operations. */
final class BigQueryTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableOperations.class);

  private static final String TABLE_PROPERTIES_BQ_CONNECTION = "bq_connection";

  private final BigQueryMetastoreClient client;
  private final FileIO fileIO;
  private final TableReference tableReference;

  /** Table loaded in doRefresh() for reuse in updateTable() to avoid redundant API call. */
  private volatile Table metastoreTable;

  BigQueryTableOperations(
      BigQueryMetastoreClient client, FileIO fileIO, TableReference tableReference) {
    this.client = client;
    this.fileIO = fileIO;
    this.tableReference = tableReference;
  }

  // The doRefresh method should provide implementation on how to get the metadata location.
  @Override
  public void doRefresh() {
    // Must default to null.
    String metadataLocation = null;
    this.metastoreTable = null;
    try {
      this.metastoreTable = client.load(tableReference);
      metadataLocation =
          loadMetadataLocationOrThrow(metastoreTable.getExternalCatalogTableOptions());
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
    CommitStatus commitStatus = CommitStatus.FAILURE;
    RetryDetector retryDetector = new RetryDetector();

    String newMetadataLocation = null;
    try {
      boolean newTable = base == null;
      newMetadataLocation = writeNewMetadataIfRequired(newTable, metadata);

      if (newTable) {
        createTable(newMetadataLocation, metadata, retryDetector);
      } else {
        updateTable(newMetadataLocation, metadata, retryDetector);
      }
      commitStatus = CommitStatus.SUCCESS;
    } catch (CommitFailedException e) {
      throw e;
    } catch (Throwable e) {
      LOG.error("Exception thrown on commit: ", e);
      boolean isAlreadyExistsException = e instanceof AlreadyExistsException;
      boolean isRuntimeIOException = e instanceof RuntimeIOException;

      // If retries occurred, an earlier attempt may have succeeded. If we got a
      // RuntimeIOException, we have no way of knowing if the request reached the server.
      // In either case, check whether the commit actually succeeded.
      if (isRuntimeIOException || retryDetector.retried()) {
        LOG.warn(
            "Received unexpected failure when committing to {}, validating if commit ended up succeeding.",
            tableName(),
            e);
        commitStatus = checkCommitStatus(newMetadataLocation, metadata);
      }

      if (commitStatus == CommitStatus.FAILURE) {
        if (isAlreadyExistsException) {
          throw new AlreadyExistsException(e, "Table already exists: %s", tableName());
        }
        throw new CommitFailedException(e, "Failed to commit");
      }
      if (commitStatus == CommitStatus.UNKNOWN) {
        throw new CommitStateUnknownException(e);
      }
    } finally {
      cleanupMetadata(commitStatus, newMetadataLocation);
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

  private void createTable(
      String newMetadataLocation, TableMetadata metadata, RetryDetector retryDetector) {
    LOG.debug("Creating a new Iceberg table: {}", tableName());
    Table tableBuilder = makeNewTable(metadata, newMetadataLocation);
    tableBuilder.setTableReference(tableReference);
    addConnectionIfProvided(tableBuilder, metadata.properties());

    client.create(tableBuilder, retryDetector);
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
      String newMetadataLocation, TableMetadata metadata, RetryDetector retryDetector) {
    Preconditions.checkState(
        metastoreTable != null,
        "Table %s must be loaded during refresh before commit",
        tableName());

    if (metastoreTable.getEtag().isEmpty()) {
      throw new ValidationException(
          "Etag of legacy table %s is empty, manually update the table via the BigQuery API or"
              + " recreate and retry",
          tableName());
    }
    ExternalCatalogTableOptions options = metastoreTable.getExternalCatalogTableOptions();
    addConnectionIfProvided(metastoreTable, metadata.properties());

    options.setParameters(buildTableParameters(newMetadataLocation, metadata));
    client.update(tableReference, metastoreTable, retryDetector);
    this.metastoreTable = null;
  }

  // To make the table queryable from Hive, the user would likely be setting the HIVE_ENGINE_ENABLED
  // parameter.
  //
  // TODO: We need to make a decision on how to make the table queryable from Hive.
  // (could be a server side change or a client side change - that's TBD).
  private Table makeNewTable(TableMetadata metadata, String metadataFileLocation) {
    return new Table()
        .setExternalCatalogTableOptions(
            BigQueryMetastoreUtils.createExternalCatalogTableOptions(
                metadata.location(), buildTableParameters(metadataFileLocation, metadata)));
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
      parameters.put("numFiles", summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    }

    if (summary.get(SnapshotSummary.TOTAL_RECORDS_PROP) != null) {
      parameters.put("numRows", summary.get(SnapshotSummary.TOTAL_RECORDS_PROP));
    }

    if (summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP) != null) {
      parameters.put("totalSize", summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP));
    }
  }

  private String loadMetadataLocationOrThrow(ExternalCatalogTableOptions tableOptions) {
    if (tableOptions == null || !tableOptions.getParameters().containsKey(METADATA_LOCATION_PROP)) {
      throw new ValidationException(
          "Table %s is not a valid BigQuery Metastore Iceberg table, metadata location not found",
          tableName());
    }

    return tableOptions.getParameters().get(METADATA_LOCATION_PROP);
  }

  @VisibleForTesting
  void cleanupMetadata(CommitStatus commitStatus, String metadataLocation) {
    try {
      if (commitStatus == CommitStatus.FAILURE
          && metadataLocation != null
          && !metadataLocation.isEmpty()) {
        // if anything went wrong, clean up the uncommitted metadata file
        io().deleteFile(metadataLocation);
      }
    } catch (RuntimeException e) {
      LOG.error(
          "Failed to cleanup metadata file at {} for table {}", metadataLocation, tableName(), e);
    }
  }
}
