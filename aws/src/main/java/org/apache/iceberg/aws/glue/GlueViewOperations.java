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
package org.apache.iceberg.aws.glue;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.util.RetryDetector;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.view.BaseViewOperations;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

/** Implementation of ViewOperations for AWS Glue Data Catalog. */
public class GlueViewOperations extends BaseViewOperations implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(GlueViewOperations.class);

  private final GlueClient glue;
  private final LockManager lockManager;
  private final AwsProperties awsProperties;
  private final String databaseName;
  private final String viewName;
  private final String fullViewName;
  private final String commitLockEntityId;
  private final FileIO fileIO;

  /**
   * Creates a new GlueViewOperations instance.
   *
   * @param glue Glue client
   * @param lockManager Lock manager
   * @param catalogName Catalog name
   * @param awsProperties AWS properties
   * @param catalogProperties Catalog properties
   * @param hadoopConf Hadoop configuration
   * @param viewIdentifier View identifier
   */
  GlueViewOperations(
      GlueClient glue,
      LockManager lockManager,
      String catalogName,
      AwsProperties awsProperties,
      Map<String, String> catalogProperties,
      Object hadoopConf,
      TableIdentifier viewIdentifier) {
    this.glue = glue;
    this.lockManager = lockManager;
    this.awsProperties = awsProperties;
    this.databaseName =
        IcebergToGlueConverter.getDatabaseName(
            viewIdentifier, awsProperties.glueCatalogSkipNameValidation());
    this.viewName =
        IcebergToGlueConverter.getTableName(
            viewIdentifier, awsProperties.glueCatalogSkipNameValidation());
    this.fullViewName = String.format("%s.%s.%s", catalogName, databaseName, viewName);
    this.commitLockEntityId = String.format("%s.%s", databaseName, viewName);
    this.fileIO = GlueTableOperations.initializeFileIO(catalogProperties, hadoopConf);
  }

  @Override
  public void close() {
    if (fileIO != null) {
      try {
        fileIO.close();
      } catch (Exception e) {
        LOG.error("Failed to close FileIO: {}", e.getMessage(), e);
      }
    }
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = null;
    Table table = getGlueTable();

    if (table != null) {
      String table_type = table.parameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP);

      if (table_type == null) {
        throw new NoSuchViewException("Iceberg View does not exist: %s.%s", databaseName, viewName);
      }

      if (table_type.equalsIgnoreCase(BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE)) {
        throw new AlreadyExistsException(
            "Table with same name already exists: %s.%s", databaseName, viewName);
      }

      if (table_type.equalsIgnoreCase(GlueCatalog.ICEBERG_VIEW_TYPE_VALUE)) {
        metadataLocation = table.parameters().get("metadata_location");
      } else {
        return;
      }
    } else {
      if (currentMetadataLocation() != null) {
        throw new NoSuchViewException("View does not exist: %s", databaseName + "." + viewName);
      } else {
        this.disableRefresh();
        return;
      }
    }
    refreshFromMetadataLocation(metadataLocation);
  }

  @Override
  protected void doCommit(ViewMetadata base, ViewMetadata metadata) {
    String newMetadataLocation = writeNewMetadataIfRequired(metadata);
    CommitStatus commitStatus = CommitStatus.FAILURE;
    RetryDetector retryDetector = new RetryDetector();

    try {
      if (lockManager != null) {
        // Acquire a lock if needed
        if (!lockManager.acquire(commitLockEntityId, newMetadataLocation)) {
          throw new IllegalStateException(
              String.format("Cannot acquire lock for commit: %s", commitLockEntityId));
        }
      }

      Table glueTable = getGlueTable();

      if (glueTable != null) {
        String tableType = glueTable.parameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP);
        String glueTableType = glueTable.tableType();

        if (!"VIRTUAL_VIEW".equalsIgnoreCase(glueTableType)) {
          if (BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(tableType)) {
            throw new AlreadyExistsException(
                "Table with same name already exists: %s.%s", databaseName, viewName);
          } else {
            throw new ValidationException(
                "Cannot create view %s.%s, a non-Iceberg table with the same name exists",
                databaseName, viewName);
          }
        } else if (!"iceberg-view".equalsIgnoreCase(tableType) && base == null) {
          throw new ValidationException(
              "Cannot create view %s.%s, a non-Iceberg view with the same name exists",
              databaseName, viewName);
        }
      }

      // For replace operations, we skip the metadata location check
      // base = null for create, non-null for replace
      boolean isReplace = base != null;
      if (!isReplace) {
        // Only check metadata location if this is not a replace operation
        if (!checkMetadataLocation(glueTable, base)) {
          throw new CommitFailedException(
              "Cannot commit %s because base metadata location '%s' is not same as the current Glue location '%s'",
              fullViewName,
              null,
              glueTable != null
                  ? glueTable.parameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)
                  : null);
        }
      }

      // Prepare properties for the update
      Map<String, String> parameters = prepareParameters(glueTable, metadata, newMetadataLocation);

      // Persist the changes to Glue
      if (glueTable != null) {
        LOG.debug("Committing existing Glue view: {}", fullViewName);
        updateGlueView(metadata, parameters, retryDetector);
      } else {
        LOG.debug("Committing new Glue view: {}", fullViewName);
        createGlueView(metadata, parameters, retryDetector);
      }

      commitStatus = CommitStatus.SUCCESS;

    } catch (CommitFailedException e) {
      throw e;
    } catch (RuntimeException persistFailure) {
      LOG.error("Error during commit for view {}", fullViewName, persistFailure);

      boolean isAwsServiceException = persistFailure instanceof AwsServiceException;

      // If this is an AWS service exception, and we didn't retry, or it's some other exception,
      // let's check if the commit actually succeeded
      if (!isAwsServiceException || retryDetector.retried()) {
        LOG.warn(
            "Received unexpected failure when committing to {}, validating if commit ended up succeeding.",
            fullViewName,
            persistFailure);
        commitStatus = checkCommitStatus(newMetadataLocation);
      }

      if (commitStatus != CommitStatus.SUCCESS && isAwsServiceException) {
        handleAwsExceptions((AwsServiceException) persistFailure);
      }

      switch (commitStatus) {
        case SUCCESS:
          break;
        case FAILURE:
          throw new CommitFailedException(
              persistFailure, "Cannot commit %s due to unexpected exception", fullViewName);
        case UNKNOWN:
          throw new CommitStateUnknownException(persistFailure);
      }

    } finally {
      try {
        if (commitStatus == CommitStatus.FAILURE && newMetadataLocation != null) {
          // Clean up the uncommitted metadata file
          io().deleteFile(newMetadataLocation);
        }
      } catch (RuntimeException e) {
        LOG.error("Failed to clean up metadata location: {}", newMetadataLocation, e);
      } finally {
        if (lockManager != null) {
          lockManager.release(commitLockEntityId, newMetadataLocation);
        }
      }
    }
  }

  /**
   * Get the Glue table for this view.
   *
   * @return The Glue table or null if it doesn't exist
   */
  private Table getGlueTable() {
    try {
      GetTableResponse response =
          glue.getTable(
              GetTableRequest.builder()
                  .catalogId(awsProperties.glueCatalogId())
                  .databaseName(databaseName)
                  .name(viewName)
                  .build());
      return response.table();
    } catch (EntityNotFoundException e) {
      return null;
    }
  }

  /**
   * Check if the base metadata location matches the current metadata location.
   *
   * @param glueTable The Glue table
   * @param base The base view metadata
   * @return True if the metadata locations match or if both are null
   */
  private boolean checkMetadataLocation(Table glueTable, ViewMetadata base) {
    String glueMetadataLocation =
        glueTable != null
            ? glueTable.parameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)
            : null;
    String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
    return Objects.equals(baseMetadataLocation, glueMetadataLocation);
  }

  /**
   * Prepare parameters for a Glue table update or creation.
   *
   * @param glueTable The existing Glue table or null if creating a new one
   * @param metadata The view metadata
   * @param newMetadataLocation The new metadata location
   * @return The parameters map for the Glue table
   */
  private Map<String, String> prepareParameters(
      Table glueTable, ViewMetadata metadata, String newMetadataLocation) {
    Map<String, String> parameters;

    if (glueTable != null && glueTable.parameters() != null) {
      parameters = Maps.newHashMap(glueTable.parameters());
    } else {
      parameters = Maps.newHashMap();
    }

    // Add standard Iceberg parameters
    parameters.put(BaseMetastoreTableOperations.TABLE_TYPE_PROP, "iceberg-view");
    parameters.put(BaseMetastoreTableOperations.METADATA_LOCATION_PROP, newMetadataLocation);

    if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
      parameters.put(
          BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
    }

    // Add view properties
    parameters.putAll(metadata.properties());

    // Remove any obsolete properties
    if (glueTable != null && glueTable.parameters() != null) {
      Set<String> obsoleteProps = Sets.newHashSet();
      glueTable
          .parameters()
          .forEach(
              (key, value) -> {
                if (!key.equals(BaseMetastoreTableOperations.TABLE_TYPE_PROP)
                    && !key.equals(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)
                    && !key.equals(BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP)
                    && !metadata.properties().containsKey(key)) {
                  obsoleteProps.add(key);
                }
              });
      obsoleteProps.forEach(parameters::remove);
    }

    return parameters;
  }

  /**
   * Create a new Glue view.
   *
   * @param metadata The view metadata
   * @param parameters The parameters for the Glue table
   * @param retryDetector The retry detector
   */
  private void createGlueView(
      ViewMetadata metadata, Map<String, String> parameters, RetryDetector retryDetector) {
    String sqlText = extractSqlText(metadata);

    // Create a new Glue table
    glue.createTable(
        CreateTableRequest.builder()
            .overrideConfiguration(c -> c.addMetricPublisher(retryDetector))
            .catalogId(awsProperties.glueCatalogId())
            .databaseName(databaseName)
            .tableInput(
                TableInput.builder()
                    .name(viewName)
                    .tableType(GlueCatalog.GLUE_VIRTUAL_VIEW_TYPE)
                    .parameters(parameters)
                    .storageDescriptor(
                        StorageDescriptor.builder().location(metadata.location()).build())
                    .viewOriginalText(sqlText)
                    .viewExpandedText(sqlText)
                    .build())
            .build());
  }

  /**
   * Update an existing Glue view.
   *
   * @param metadata The view metadata
   * @param parameters The parameters for the Glue table
   * @param retryDetector The retry detector
   */
  private void updateGlueView(
      ViewMetadata metadata, Map<String, String> parameters, RetryDetector retryDetector) {
    String sqlText = extractSqlText(metadata);

    StorageDescriptor storageDescriptor =
        StorageDescriptor.builder()
            .location(metadata.location())
            .build();

    // Update the existing Glue table
    UpdateTableRequest.Builder updateTableRequest =
        UpdateTableRequest.builder()
            .overrideConfiguration(c -> c.addMetricPublisher(retryDetector))
            .catalogId(awsProperties.glueCatalogId())
            .databaseName(databaseName)
            .skipArchive(awsProperties.glueCatalogSkipArchive())
            .tableInput(
                TableInput.builder()
                    .name(viewName)
                    .tableType(GlueCatalog.GLUE_VIRTUAL_VIEW_TYPE)
                    .parameters(parameters)
                    .storageDescriptor(storageDescriptor)
                    .viewOriginalText(sqlText)
                    .viewExpandedText(sqlText)
                    .build());

    glue.updateTable(updateTableRequest.build());
  }

  /**
   * Extract SQL text from view metadata.
   *
   * @param metadata The view metadata
   * @return The SQL text for the view
   */
  private String extractSqlText(ViewMetadata metadata) {
    SQLViewRepresentation closest = null;

    // Try to find a representation for the view
    if (metadata.currentVersion() != null && metadata.currentVersion().representations() != null) {
      for (ViewRepresentation representation : metadata.currentVersion().representations()) {
        if (representation instanceof SQLViewRepresentation) {
          SQLViewRepresentation sqlViewRepresentation = (SQLViewRepresentation) representation;

          // Prefer a Hive SQL representation if available
          if (sqlViewRepresentation.dialect().equalsIgnoreCase("hive")) {
            return sqlViewRepresentation.sql();
          } else if (closest == null) {
            closest = sqlViewRepresentation;
          }
        }
      }
    }

    return closest != null ? closest.sql() : "";
  }

  /**
   * Check the status of a commit.
   *
   * @param newMetadataLocation The new metadata location
   * @return The commit status
   */
  private CommitStatus checkCommitStatus(String newMetadataLocation) {
    try {
      Table table = getGlueTable();

      if (table == null) {
        return CommitStatus.FAILURE;
      }

      String metadataLocation =
          table.parameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);

      if (metadataLocation != null && metadataLocation.equals(newMetadataLocation)) {
        return CommitStatus.SUCCESS;
      } else {
        return CommitStatus.FAILURE;
      }
    } catch (Exception e) {
      LOG.error("Failed to check commit status for {}", fullViewName, e);
      return CommitStatus.UNKNOWN;
    }
  }

  /**
   * Handle AWS exceptions.
   *
   * @param awsException The AWS exception
   * @throws CommitFailedException if the exception indicates a commit failure
   * @throws AlreadyExistsException if the view already exists
   * @throws ValidationException if there is a validation error
   */
  private void handleAwsExceptions(AwsServiceException awsException) {
    if (awsException instanceof ConcurrentModificationException) {
      throw new CommitFailedException(
          awsException, "Cannot commit %s because Glue detected concurrent update", fullViewName);
    } else if (awsException
        instanceof software.amazon.awssdk.services.glue.model.AlreadyExistsException) {
      throw new AlreadyExistsException(
          awsException, "Cannot commit %s because its Glue table already exists", fullViewName);
    } else if (awsException instanceof EntityNotFoundException) {
      throw new NoSuchViewException(
          awsException,
          "Cannot commit %s because Glue cannot find the requested entity",
          fullViewName);
    } else if (awsException instanceof AccessDeniedException) {
      throw new ValidationException(
          awsException, "Cannot commit %s because of insufficient permissions", fullViewName);
    } else if (awsException
        instanceof software.amazon.awssdk.services.glue.model.ValidationException) {
      throw new ValidationException(
          awsException, "Cannot commit %s because of Glue validation error", fullViewName);
    } else {
      // For 5xx errors, we'll let the caller retry
      int statusCode = awsException.statusCode();
      if (statusCode < 500 || statusCode >= 600) {
        throw awsException;
      }
    }
  }

  @Override
  protected String viewName() {
    return fullViewName;
  }

  @Override
  protected FileIO io() {
    return fileIO;
  }
}
