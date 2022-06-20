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

import static org.apache.iceberg.TableProperties.COMMIT_NUM_STATUS_CHECKS;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_STATUS_CHECKS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_MAX_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_MAX_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_MIN_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_MIN_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS_DEFAULT;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.view.MetastoreViewOperations;
import org.apache.iceberg.view.ViewMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;

public class GlueViewOperations extends MetastoreViewOperations {

  private static final Logger LOG = LoggerFactory.getLogger(GlueViewOperations.class);

  private final GlueClient glue;
  private final AwsProperties awsProperties;
  private final String databaseName;
  private final String viewName;
  private final String fullViewName;
  private final String commitLockEntityId;
  private final FileIO fileIO;
  private final LockManager lockManager;
  private boolean shouldRefresh = true;

  private ViewMetadata currentViewMetadata = null;
  private String currentViewMetadataLocation = null;
  private static final String GLUE_TABLE_TYPE = "CROSS_ENGINE_VIEW";
  private String VIEW_METADATA_CONTAINER = "VIEW_METADATA_CONTAINER";

  public GlueViewOperations(
      GlueClient glue,
      LockManager lockManager,
      String catalogName,
      AwsProperties awsProperties,
      FileIO fileIO,
      TableIdentifier tableIdentifier) {

    this.glue = glue;
    this.awsProperties = awsProperties;
    this.databaseName = IcebergToGlueConverter.getDatabaseName(tableIdentifier, awsProperties.glueCatalogSkipNameValidation());
    this.viewName = IcebergToGlueConverter.getTableName(tableIdentifier, awsProperties.glueCatalogSkipNameValidation());
    this.fullViewName = String.format("%s.%s.%s", catalogName, databaseName, viewName);
    this.commitLockEntityId = String.format("%s.%s", databaseName, viewName);
    this.fileIO = fileIO;
    this.lockManager = lockManager;
  }

  // Attempt to set versionId if available on the path
  private static final DynMethods.UnboundMethod SET_VERSION_ID =
      DynMethods.builder("versionId")
          .hiddenImpl(
              "software.amazon.awssdk.services.glue.model.UpdateTableRequest$Builder", String.class)
          .orNoop()
          .build();

  @Override
  public ViewMetadata current() {
    if (shouldRefresh) {
      return refresh();
    }
    return currentViewMetadata;
  }

  @Override
  public ViewMetadata refresh() {
    boolean currentViewExists = currentViewMetadata != null;
    try {
      doRefresh();
    } catch (NoSuchTableException e) {
      if (currentViewExists) {
        LOG.warn("Could not find the table during refresh, setting current metadata to null", e);
        shouldRefresh = true;
      }

      currentViewMetadata = null;
      currentViewMetadataLocation = null;
      throw e;
    }
    return current();
  }

  protected void doRefresh() {
    String metadataLocation = null;
    Table view = getGlueTable();
    if (view != null) {
      metadataLocation = view.parameters().get(METADATA_LOCATION_PROP);
      refreshFromMetadataLocation(metadataLocation, null, 3);
    } else {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(
            "Cannot find Glue table %s after refresh, "
                + "maybe another process deleted it or revoked your access permission",
            viewName());
      }
    }
  }

  @Override
  public void commit(ViewMetadata base, ViewMetadata metadata, Map<String, String> properties) {
    boolean isViewMetadataContainer =
        properties.containsKey(VIEW_METADATA_CONTAINER) && Boolean.parseBoolean(VIEW_METADATA_CONTAINER);
    commitHelper(base, metadata, isViewMetadataContainer, properties);
  }

  private void commitHelper(ViewMetadata base, ViewMetadata metadata, boolean viewMetadataContainer, Map<String,
      String> properties) {
    MetastoreViewOperations.CommitStatus commitStatus =
        MetastoreViewOperations.CommitStatus.FAILURE;
    String newMetadataLocation = "";
    try {
      if (viewMetadataContainer) {
        newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);
        lock(newMetadataLocation);
        Table glueTable = getGlueTable();
        checkMetadataLocation(glueTable, base);
        persistGlueTable(glueTable, properties, metadata);
        commitStatus = MetastoreViewOperations.CommitStatus.SUCCESS;
      } else {
        Table glueTable = getGlueTable();
        checkMetadataLocation(glueTable, base);
        persistGlueTable(glueTable, properties, metadata);
        commitStatus = MetastoreViewOperations.CommitStatus.SUCCESS;
      }
    } catch (CommitFailedException e) {
      throw e;
    } catch (ConcurrentModificationException e) {
      throw new CommitFailedException(
          e, "Cannot commit %s because Glue detected concurrent update", viewName());
    } catch (software.amazon.awssdk.services.glue.model.AlreadyExistsException e) {
      throw new org.apache.iceberg.exceptions.AlreadyExistsException(
          e,
          "Cannot commit %s because its Glue table already exists when trying to create one",
          viewName());
    } catch (RuntimeException persistFailure) {
      LOG.error(
          "Confirming if commit to {} indeed failed to persist, attempting to reconnect and check.",
          viewName(),
          persistFailure);
      switch (commitStatus) {
        case SUCCESS:
          break;
        case FAILURE:
          throw new CommitFailedException(
              persistFailure, "Cannot commit %s due to unexpected exception", viewName());
        case UNKNOWN:
          throw new CommitStateUnknownException(persistFailure);
      }
    } finally {
      cleanupMetadataAndUnlock(commitStatus, newMetadataLocation);
    }
  }

  private Map<String, String> prepareProperties(Table glueTable, String newMetadataLocation) {
    Map<String, String> properties =
        glueTable != null ? Maps.newHashMap(glueTable.parameters()) : Maps.newHashMap();
    properties.put(TABLE_TYPE_PROP, "CROSS_ENGINE_VIEW");
    properties.put(METADATA_LOCATION_PROP, newMetadataLocation);
    if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
      properties.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
    }

    return properties;
  }

  private void checkMetadataLocation(Table glueTable, ViewMetadata base) {
    String glueMetadataLocation =
        glueTable != null ? glueTable.parameters().get(METADATA_LOCATION_PROP) : null;
    String baseMetadataLocation = base != null ? base.location() : null;
    if (!Objects.equals(baseMetadataLocation, glueMetadataLocation)) {
      throw new CommitFailedException(
          "Cannot commit %s because base metadata location '%s' is not same as the current Glue location '%s'",
          viewName(), baseMetadataLocation, glueMetadataLocation);
    }
  }

  @VisibleForTesting
  void cleanupMetadataAndUnlock(
      MetastoreViewOperations.CommitStatus commitStatus, String metadataLocation) {
    try {
      if (commitStatus == MetastoreViewOperations.CommitStatus.FAILURE) {
        // if anything went wrong, clean up the uncommitted metadata file
        io().deleteFile(metadataLocation);
      }
    } catch (RuntimeException e) {
      LOG.error("Fail to cleanup metadata file at {}", metadataLocation, e);
      throw e;
    } finally {
      if (lockManager != null) {
        lockManager.release(commitLockEntityId, metadataLocation);
      }
    }
  }

  private void lock(String newMetadataLocation) {
    if (lockManager != null && !lockManager.acquire(commitLockEntityId, newMetadataLocation)) {
      throw new IllegalStateException(
          String.format(
              "Fail to acquire lock %s to commit new metadata at %s",
              commitLockEntityId, newMetadataLocation));
    }
  }

  @VisibleForTesting
  void persistGlueTable(Table glueTable, Map<String, String> parameters, ViewMetadata metadata) {
    if (glueTable != null) {
      LOG.debug("Committing existing Glue table: {}", viewName());
      UpdateTableRequest.Builder updateTableRequest =
          UpdateTableRequest.builder()
              .catalogId(awsProperties.glueCatalogId())
              .databaseName(databaseName)
              .skipArchive(awsProperties.glueCatalogSkipArchive())
              .tableInput(
                  TableInput.builder()
                      .applyMutation(
                          builder ->
                              IcebergToGlueConverter.setViewInputInformation(builder, metadata))
                      .name(viewName())
                      .tableType(GLUE_TABLE_TYPE)
                      .parameters(parameters)
                      .build());
      // Use Optimistic locking with table version id while updating table
      if (!SET_VERSION_ID.isNoop() && lockManager == null) {
        SET_VERSION_ID.invoke(updateTableRequest, glueTable.versionId());
      }

      glue.updateTable(updateTableRequest.build());
    } else {
      LOG.debug("Committing new Glue table: {}", viewName());
      glue.createTable(
          CreateTableRequest.builder()
              .catalogId(awsProperties.glueCatalogId())
              .databaseName(databaseName)
              .tableInput(
                  TableInput.builder()
                      .applyMutation(
                          builder ->
                              IcebergToGlueConverter.setViewInputInformation(builder, metadata))
                      .name(viewName())
                      .tableType(GLUE_TABLE_TYPE)
                      .parameters(parameters)
                      .build())
              .build());
    }
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected String viewName() {
    return fullViewName;
  }

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
}
