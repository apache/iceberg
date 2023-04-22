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
package org.apache.iceberg.gcp.biglake;

import com.google.api.gax.rpc.AbortedException;
import com.google.cloud.bigquery.biglake.v1.HiveTableOptions;
import com.google.cloud.bigquery.biglake.v1.HiveTableOptions.SerDeInfo;
import com.google.cloud.bigquery.biglake.v1.HiveTableOptions.StorageDescriptor;
import com.google.cloud.bigquery.biglake.v1.Table;
import com.google.cloud.bigquery.biglake.v1.TableName;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles BigLake table operations. */
public final class BigLakeTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(BigLakeTableOperations.class);

  private final Configuration conf;
  private final BigLakeClient client;
  private final FileIO fileIO;
  private final TableName tableName;

  BigLakeTableOperations(
      Configuration conf, BigLakeClient client, FileIO fileIO, TableName tableName) {
    this.conf = conf;
    this.client = client;
    this.fileIO = fileIO;
    this.tableName = tableName;
  }

  // The doRefresh method should provide implementation on how to get the metadata location
  @Override
  public void doRefresh() {
    // Must default to null.
    String metadataLocation = null;
    try {
      HiveTableOptions hiveOptions = client.getTable(tableName).getHiveOptions();
      if (!hiveOptions.containsParameters(METADATA_LOCATION_PROP)) {
        throw new ValidationException(
            "Table %s is not a valid Iceberg table, metadata location not found", tableName());
      }
      metadataLocation = hiveOptions.getParametersOrThrow(METADATA_LOCATION_PROP);
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
    boolean isNewTable = base == null;
    String newMetadataLocation = writeNewMetadataIfRequired(isNewTable, metadata);

    CommitStatus commitStatus = CommitStatus.FAILURE;
    try {
      if (isNewTable) {
        createTable(newMetadataLocation, metadata);
      } else {
        updateTable(base.metadataFileLocation(), newMetadataLocation, metadata);
      }
      commitStatus = CommitStatus.SUCCESS;
    } catch (CommitFailedException | CommitStateUnknownException e) {
      throw e;
    } catch (Throwable e) {
      commitStatus = checkCommitStatus(newMetadataLocation, metadata);
      if (commitStatus == CommitStatus.FAILURE) {
        throw new CommitFailedException(e, "Failed to commit");
      }
      if (commitStatus == CommitStatus.UNKNOWN) {
        throw new CommitStateUnknownException(e);
      }
    } finally {
      try {
        if (commitStatus == CommitStatus.FAILURE) {
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
    return String.format(
        "%s.%s.%s", tableName.getCatalog(), tableName.getDatabase(), tableName.getTable());
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  private void createTable(String newMetadataLocation, TableMetadata metadata) {
    LOG.debug("Creating a new Iceberg table: {}", tableName());
    client.createTable(tableName, makeNewTable(metadata, newMetadataLocation));
  }

  /** Update table properties with concurrent update detection using etag. */
  private void updateTable(
      String oldMetadataLocation, String newMetadataLocation, TableMetadata metadata) {
    Table table = client.getTable(tableName);
    String etag = table.getEtag();
    if (etag.isEmpty()) {
      throw new ValidationException(
          "Etag of legacy table %s is empty, manually update the table by BigLake API or"
              + " recreate and retry",
          tableName());
    }
    HiveTableOptions options = table.getHiveOptions();

    // If `metadataLocationFromMetastore` is different from metadata location of base, it means
    // someone has updated metadata location in metastore, which is a conflict update.
    String metadataLocationFromMetastore =
        options.getParametersOrDefault(METADATA_LOCATION_PROP, "");
    if (!metadataLocationFromMetastore.isEmpty()
        && !metadataLocationFromMetastore.equals(oldMetadataLocation)) {
      throw new CommitFailedException(
          "Base metadata location '%s' is not same as the current table metadata location '%s' for"
              + " %s.%s",
          oldMetadataLocation,
          metadataLocationFromMetastore,
          tableName.getDatabase(),
          tableName.getTable());
    }

    try {
      client.updateTableParameters(
          tableName, buildTableParameters(newMetadataLocation, metadata), etag);
    } catch (AbortedException e) {
      if (e.getMessage().toLowerCase().contains("etag mismatch")) {
        throw new CommitFailedException(
            "Updating table failed due to conflict updates (etag mismatch)");
      }
    }
  }

  private Table makeNewTable(TableMetadata metadata, String metadataFileLocation) {
    Table.Builder builder = Table.newBuilder().setType(Table.Type.HIVE);
    builder
        .getHiveOptionsBuilder()
        .setTableType("EXTERNAL_TABLE")
        .setStorageDescriptor(
            StorageDescriptor.newBuilder()
                .setLocationUri(metadata.location())
                .setInputFormat("org.apache.hadoop.mapred.FileInputFormat")
                .setOutputFormat("org.apache.hadoop.mapred.FileOutputFormat")
                .setSerdeInfo(
                    SerDeInfo.newBuilder()
                        .setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")))
        .putAllParameters(buildTableParameters(metadataFileLocation, metadata));
    return builder.build();
  }

  // Follow Iceberg's HiveTableOperations to populate more table parameters for HMS compatibility.
  private Map<String, String> buildTableParameters(
      String metadataFileLocation, TableMetadata metadata) {
    Map<String, String> parameters = Maps.newHashMap();
    parameters.putAll(metadata.properties());
    if (metadata.uuid() != null) {
      parameters.put(TableProperties.UUID, metadata.uuid());
    }
    if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
      parameters.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
    }
    parameters.put(METADATA_LOCATION_PROP, metadataFileLocation);
    // Follow HMS to use the EXTERNAL type.
    parameters.put("EXTERNAL", "TRUE");
    parameters.put("table_type", "ICEBERG");

    // Hive style basic statistics.
    if (metadata.currentSnapshot() != null) {
      Map<String, String> summary = metadata.currentSnapshot().summary();
      if (summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP) != null) {
        parameters.put(
            StatsSetupConst.NUM_FILES, summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
      }
      if (summary.get(SnapshotSummary.TOTAL_RECORDS_PROP) != null) {
        parameters.put(StatsSetupConst.ROW_COUNT, summary.get(SnapshotSummary.TOTAL_RECORDS_PROP));
      }
      if (summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP) != null) {
        parameters.put(
            StatsSetupConst.TOTAL_SIZE, summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP));
      }
    }
    // TODO: to expose more Iceberg metadata if needed, e.g., statistic, schema, partition spec.
    return parameters;
  }
}
