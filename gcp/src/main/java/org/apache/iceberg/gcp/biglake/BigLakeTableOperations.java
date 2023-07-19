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
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles BigLake table operations. */
final class BigLakeTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(BigLakeTableOperations.class);

  private final BigLakeClient client;
  private final FileIO io;
  // The name of this Iceberg catalog plugin.
  private final String name;
  private final TableName tableName;

  BigLakeTableOperations(BigLakeClient client, FileIO io, String name, TableName tableName) {
    this.client = client;
    this.io = io;
    this.name = name;
    this.tableName = tableName;
  }

  // The doRefresh method should provide implementation on how to get the metadata location
  @Override
  protected void doRefresh() {
    // Must default to null.
    String metadataLocation = null;
    try {
      HiveTableOptions hiveOptions = client.table(tableName).getHiveOptions();
      if (!hiveOptions.containsParameters(METADATA_LOCATION_PROP)) {
        throw new NoSuchIcebergTableException(
            "Invalid Iceberg table %s: missing metadata location", tableName());
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
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
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
    } catch (AlreadyExistsException | CommitFailedException e) {
      throw e;
    } catch (CommitStateUnknownException e) {
      commitStatus = CommitStatus.UNKNOWN;
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
  protected String tableName() {
    return String.format("%s.%s.%s", name, tableName.getDatabase(), tableName.getTable());
  }

  @Override
  public FileIO io() {
    return io;
  }

  private void createTable(String newMetadataLocation, TableMetadata metadata) {
    LOG.debug("Creating a new Iceberg table: {}", tableName());
    client.createTable(tableName, makeNewTable(metadata, newMetadataLocation));
  }

  /** Update table properties with concurrent update detection using etag. */
  private void updateTable(
      String oldMetadataLocation, String newMetadataLocation, TableMetadata metadata) {
    Table table = client.table(tableName);
    String etag = table.getEtag();
    Preconditions.checkArgument(
        !etag.isEmpty(),
        "Etag of legacy table %s is empty, manually update the table by BigLake API or recreate and retry",
        tableName());
    HiveTableOptions options = table.getHiveOptions();

    if (!options.containsParameters(METADATA_LOCATION_PROP)) {
      throw new NoSuchIcebergTableException(
          "Table %s is not a valid Iceberg table, metadata location is empty", tableName());
    }

    String metadataLocationFromMetastore = options.getParametersOrThrow(METADATA_LOCATION_PROP);

    // If `metadataLocationFromMetastore` is different from metadata location of base, it means
    // someone has updated metadata location in metastore, which is a conflict update.
    if (!metadataLocationFromMetastore.equals(oldMetadataLocation)) {
      throw new CommitFailedException(
          "Cannot commit %s. Base metadata location '%s' is not same as the current table metadata location '%s' for"
              + " %s.%s",
          tableName(),
          oldMetadataLocation,
          metadataLocationFromMetastore,
          tableName.getDatabase(),
          tableName.getTable());
    }

    try {
      // Updating a BLMS table with etag. The BLMS server transactionally (1) checks that the etag
      // of a table on server is the same as the etag provided by the client, and (2) updates the
      // table (and its etag). The server returns an error containing message "etag mismatch", if
      // the etag on server has changed.
      client.updateTableParameters(
          tableName, buildTableParameters(newMetadataLocation, metadata), etag);
    } catch (AbortedException e) {
      if (e.getMessage().toLowerCase().contains("etag mismatch")) {
        throw new CommitFailedException(
            "Updating table failed due to conflicting updates (etag mismatch)");
      }
      throw e;
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
    ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
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
    return parameters.build();
  }
}
