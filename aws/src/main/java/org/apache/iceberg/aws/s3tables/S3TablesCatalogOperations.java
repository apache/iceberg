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
package org.apache.iceberg.aws.s3tables;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.BaseMetastoreOperations.CommitStatus;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.util.RetryDetector;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3tables.S3TablesClient;
import software.amazon.awssdk.services.s3tables.model.ConflictException;
import software.amazon.awssdk.services.s3tables.model.DeleteTableRequest;
import software.amazon.awssdk.services.s3tables.model.GetTableMetadataLocationRequest;
import software.amazon.awssdk.services.s3tables.model.GetTableMetadataLocationResponse;
import software.amazon.awssdk.services.s3tables.model.NotFoundException;
import software.amazon.awssdk.services.s3tables.model.UpdateTableMetadataLocationRequest;
import software.amazon.awssdk.services.s3tables.model.UpdateTableMetadataLocationResponse;

// https://iceberg.apache.org/docs/nightly/custom-catalog/#custom-table-operations-implementation
public class S3TablesCatalogOperations extends BaseMetastoreTableOperations implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(S3TablesCatalogOperations.class);

  private FileIO fileIO;
  private final Map<String, String> tableCatalogProperties;
  private final S3TablesClient tablesClient;

  private final String namespaceName;
  private final String tableName;
  private final String tableWareHouseLocation;
  private final Object hadoopConf;
  private final CloseableGroup closeableGroup;

  protected S3TablesCatalogOperations(
      S3TablesClient s3IceClient,
      String namespaceName,
      String tableName,
      String tableWareHouseLocation,
      Map<String, String> tableCatalogProperties,
      Object hadoopConf) {
    this.tablesClient = s3IceClient;
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.tableWareHouseLocation = tableWareHouseLocation;
    this.tableCatalogProperties = tableCatalogProperties;
    this.hadoopConf = hadoopConf;
    this.closeableGroup = new CloseableGroup();
    closeableGroup.setSuppressCloseFailure(true);
    closeableGroup.addCloseable(tablesClient);
  }

  @Override
  protected String tableName() {
    return tableName;
  }

  @Override
  public FileIO io() {
    if (fileIO == null) {
      try {
        fileIO = initializeFileIO(this.tableCatalogProperties, this.hadoopConf);
        closeableGroup.addCloseable(fileIO);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return fileIO;
  }

  protected static FileIO initializeFileIO(Map<String, String> properties, Object hadoopConf) {
    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    if (fileIOImpl == null) {
      FileIO io = new S3FileIO();
      io.initialize(properties);
      return io;
    } else {
      return CatalogUtil.loadFileIO(fileIOImpl, properties, hadoopConf);
    }
  }

  /**
   * For S3 Tables, we effectively turn `write.object-storage.enabled` on by default. Customers can
   * still explicitly disable it as a table property, but if omitted, we default to using the
   * S3TablesLocationProvider, which is a clone of the recently upstreamed changes to Iceberg's
   * ObjectStoreLocationProvider.
   */
  @Override
  public LocationProvider locationProvider() {
    Map<String, String> properties = current().properties();
    boolean isObjectStoreEnabled =
        PropertyUtil.propertyAsBoolean(properties, TableProperties.OBJECT_STORE_ENABLED, true);
    if (properties.containsKey(TableProperties.WRITE_LOCATION_PROVIDER_IMPL)
        || !isObjectStoreEnabled) {
      return LocationProviders.locationsFor(current().location(), current().properties());
    } else {
      return new S3TablesLocationProvider(current().location(), current().properties());
    }
  }

  /**
   * The doRefresh method should provide implementation on how to get the metadata location If the
   * table doesn't exist, it will throw an error.
   */
  @Override
  public void doRefresh() {
    // Example custom service which returns the metadata location given a dbName and tableName
    String metadataLocation = null;
    try {
      GetTableMetadataLocationRequest getTableMetadataLocationRequest =
          GetTableMetadataLocationRequest.builder()
              .tableBucketARN(tableWareHouseLocation)
              .namespace(namespaceName)
              .name(tableName)
              .build();

      GetTableMetadataLocationResponse getTableMetadataLocationResponse =
          this.tablesClient.getTableMetadataLocation(getTableMetadataLocationRequest);

      if (StringUtils.isEmpty(getTableMetadataLocationResponse.metadataLocation())) {
        LOG.debug(
            "Empty metadata location for table {}.{}, skipping doRefresh()",
            namespaceName,
            tableName);
        disableRefresh();
        return;
      }

      metadataLocation = getTableMetadataLocationResponse.metadataLocation();
    } catch (NotFoundException ex) {
      LOG.debug("Received not found exception: ", ex);
      LOG.debug(
          "Empty metadata location for table {}.{}, skipping doRefresh()",
          namespaceName,
          tableName);
      if (currentMetadataLocation() != null) {
        LOG.error("Cannot find S3 Table {} after refresh", tableName);
        throw new NoSuchTableException(
            "Cannot find S3 table metadata location for table %s after refresh, "
                + "maybe another process deleted it or revoked your access permission",
            tableName());
      }
    }

    // When updating from a metadata file location, call the helper method
    refreshFromMetadataLocation(metadataLocation);
  }

  /**
   * The doCommit method should provide implementation on how to update with metadata location
   * atomically
   *
   * @param base the base metadata, before any changes were made
   * @param metadata the updated metadata, to be committed Will drop temporary table if it failed to
   *     commit the data
   */
  @Override
  public void doCommit(TableMetadata base, TableMetadata metadata) {
    boolean newTable = false;
    RetryDetector retryDetector = new RetryDetector();
    CommitStatus commitStatus = CommitStatus.FAILURE;
    String newMetadataLocation = null;
    String versionToken = null;
    try {
      LOG.debug("Commiting metadata to namespace: {} with tableName {}", namespaceName, tableName);

      newTable = base == null;

      newMetadataLocation = writeNewMetadataIfRequired(newTable, metadata);
      LOG.debug("Wrote new metadata to {}", newMetadataLocation);

      GetTableMetadataLocationResponse tableMetadataLocationResponse =
          this.tablesClient.getTableMetadataLocation(
              GetTableMetadataLocationRequest.builder()
                  .name(tableName)
                  .namespace(namespaceName)
                  .tableBucketARN(tableWareHouseLocation)
                  .build());

      versionToken = tableMetadataLocationResponse.versionToken();

      if (base != null) {
        // New tables will have a base empty metadata file written by the control plane
        checkMetadataLocation(tableMetadataLocationResponse, base);
        LOG.debug(
            "Successfully checked metadata location for {} got VersionToken {}",
            tableName,
            versionToken);
      } else {
        LOG.debug(
            "Skipped checking metadata location for {} because this is a new table", tableName);
      }

      UpdateTableMetadataLocationResponse updateTableMetadataLocationResponse =
          this.tablesClient.updateTableMetadataLocation(
              UpdateTableMetadataLocationRequest.builder()
                  .overrideConfiguration(c -> c.addMetricPublisher(retryDetector))
                  .tableBucketARN(tableWareHouseLocation)
                  .namespace(namespaceName)
                  .name(tableName)
                  .metadataLocation(newMetadataLocation)
                  .versionToken(versionToken)
                  .build());

      versionToken = updateTableMetadataLocationResponse.versionToken();

      LOG.debug("Successfully updated metadata new version token is: {}", versionToken);
      commitStatus = CommitStatus.SUCCESS;
    } catch (ConflictException e) {
      LOG.error("Failed to commit metadata due to conflict: ", e);
      throw new CommitFailedException(e);
    } catch (CommitFailedException e) {
      LOG.error("Failed commit metadata: ", e);
      throw e;
    } catch (RuntimeException persistFailure) {
      boolean isAwsServiceException = persistFailure instanceof AwsServiceException;

      if (!isAwsServiceException || retryDetector.retried()) {
        LOG.warn(
            "Received unexpected failure when committing to {}, validating if commit ended up succeeding.",
            tableName,
            persistFailure);

        commitStatus = checkCommitStatus(newMetadataLocation, metadata);
      }

      // If we got an AWS exception we would usually handle, but find we
      // succeeded on a retry that threw an exception, skip the exception.
      if (commitStatus != CommitStatus.SUCCESS && isAwsServiceException) {
        LOG.error("Received unexpected failure when committing to {}", tableName, persistFailure);
        throw new RuntimeException("Persisting failure", persistFailure);
      }

      handleCommitStatus(commitStatus, persistFailure);
    } finally {
      if (newTable && commitStatus != CommitStatus.SUCCESS) {
        handleFailedNewTable(versionToken);
      }
    }
  }

  private void handleFailedNewTable(String versionToken) {
    try {
      if (versionToken == null) {
        LOG.error(
            "[Critical] Couldn't find version token for {} will not try delete table with invalid metadata",
            tableName);
        // Not throwing an exception here to make sure we bubble up the correct exception for
        // the refresh to the stack trace.
      } else {
        LOG.info("Commit failed deleting table {} and versionToken {}", tableName, versionToken);
        this.tablesClient.deleteTable(
            DeleteTableRequest.builder()
                .name(tableName)
                .versionToken(versionToken)
                .tableBucketARN(tableWareHouseLocation)
                .namespace(namespaceName)
                .build());
        LOG.info("Successfully deleted table {}", tableName);
      }
    } catch (Throwable deleteFailure) {
      // suppress this exception so we can propagate the original exception
      LOG.warn(
          "Received unexpected failure when deleting table {}, suppressing.",
          tableName,
          deleteFailure);
    }
  }

  private void handleCommitStatus(CommitStatus status, RuntimeException ex) {
    switch (status) {
      case SUCCESS:
        return;
      case FAILURE:
        LOG.error("Commit failed ", ex);
        throw new CommitFailedException(
            ex, "Cannot commit %s due to unexpected exception", tableName());
      case UNKNOWN:
        throw new CommitStateUnknownException(ex);
    }
  }

  private void checkMetadataLocation(
      GetTableMetadataLocationResponse tableMetadataLocationResponse, TableMetadata base) {
    String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
    String tableMetadataLocationInDDB = tableMetadataLocationResponse.metadataLocation();
    if (!Objects.equals(baseMetadataLocation, tableMetadataLocationInDDB)) {
      LOG.error(
          "Base metadata location {} is not the same as current metadata location {} in DDB ",
          baseMetadataLocation,
          tableMetadataLocationInDDB);
      throw new CommitFailedException(
          "Base metadata location %s  is not the same as current metadata location %s in DDB ",
          baseMetadataLocation, tableMetadataLocationInDDB);
    }
  }

  @VisibleForTesting
  Map<String, String> tableCatalogProperties() {
    return tableCatalogProperties;
  }

  @Override
  public void close() throws IOException {
    closeableGroup.close();
  }
}
