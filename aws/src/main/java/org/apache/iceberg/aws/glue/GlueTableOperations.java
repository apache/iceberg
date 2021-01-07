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

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

class GlueTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(GlueTableOperations.class);

  // same as org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE
  // more details: https://docs.aws.amazon.com/glue/latest/webapi/API_TableInput.html
  private static final String GLUE_EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE";

  private final GlueClient glue;
  private final AwsProperties awsProperties;
  private final String databaseName;
  private final String tableName;
  private final String fullTableName;
  private final String commitLockEntityId;
  private final FileIO fileIO;
  private final LockManager lockManager;

  GlueTableOperations(GlueClient glue, LockManager lockManager, String catalogName, AwsProperties awsProperties,
                      FileIO fileIO, TableIdentifier tableIdentifier) {
    this.glue = glue;
    this.awsProperties = awsProperties;
    this.databaseName = IcebergToGlueConverter.getDatabaseName(tableIdentifier);
    this.tableName = IcebergToGlueConverter.getTableName(tableIdentifier);
    this.fullTableName = String.format("%s.%s.%s", catalogName, databaseName, tableName);
    this.commitLockEntityId = String.format("%s.%s", databaseName, tableName);
    this.fileIO = fileIO;
    this.lockManager = lockManager;
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected String tableName() {
    return fullTableName;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = null;
    Table table = getGlueTable();
    if (table != null) {
      GlueToIcebergConverter.validateTable(table, tableName());
      metadataLocation = table.parameters().get(METADATA_LOCATION_PROP);
    } else {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException("Cannot find Glue table %s after refresh, " +
            "maybe another process deleted it or revoked your access permission", tableName());
      }
    }

    refreshFromMetadataLocation(metadataLocation);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);
    boolean exceptionThrown = true;
    try {
      lock(newMetadataLocation);
      Table glueTable = getGlueTable();
      checkMetadataLocation(glueTable, base);
      Map<String, String> properties = prepareProperties(glueTable, newMetadataLocation);
      persistGlueTable(glueTable, properties);
      exceptionThrown = false;
    } catch (ConcurrentModificationException e) {
      throw new CommitFailedException(e, "Cannot commit %s because Glue detected concurrent update", tableName());
    } catch (software.amazon.awssdk.services.glue.model.AlreadyExistsException e) {
      throw new AlreadyExistsException(e,
          "Cannot commit %s because its Glue table already exists when trying to create one", tableName());
    } catch (SdkException e) {
      throw new CommitFailedException(e, "Cannot commit %s because unexpected exception contacting AWS", tableName());
    } finally {
      cleanupMetadataAndUnlock(exceptionThrown, newMetadataLocation);
    }
  }

  private void lock(String newMetadataLocation) {
    if (!lockManager.acquire(commitLockEntityId, newMetadataLocation)) {
      throw new IllegalStateException(String.format("Fail to acquire lock %s to commit new metadata at %s",
          commitLockEntityId, newMetadataLocation));
    }
  }

  private void checkMetadataLocation(Table glueTable, TableMetadata base) {
    String glueMetadataLocation = glueTable != null ? glueTable.parameters().get(METADATA_LOCATION_PROP) : null;
    String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
    if (!Objects.equals(baseMetadataLocation, glueMetadataLocation)) {
      throw new CommitFailedException(
          "Cannot commit %s because base metadata location '%s' is not same as the current Glue location '%s'",
          tableName(), baseMetadataLocation, glueMetadataLocation);
    }
  }

  private Table getGlueTable() {
    try {
      GetTableResponse response = glue.getTable(GetTableRequest.builder()
          .catalogId(awsProperties.glueCatalogId())
          .databaseName(databaseName)
          .name(tableName)
          .build());
      return response.table();
    } catch (EntityNotFoundException e) {
      return null;
    }
  }

  private Map<String, String> prepareProperties(Table glueTable, String newMetadataLocation) {
    Map<String, String> properties = glueTable != null ? Maps.newHashMap(glueTable.parameters()) : Maps.newHashMap();
    properties.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH));
    properties.put(METADATA_LOCATION_PROP, newMetadataLocation);
    if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
      properties.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
    }

    return properties;
  }

  private void persistGlueTable(Table glueTable, Map<String, String> parameters) {
    if (glueTable != null) {
      LOG.debug("Committing existing Glue table: {}", tableName());
      glue.updateTable(UpdateTableRequest.builder()
          .catalogId(awsProperties.glueCatalogId())
          .databaseName(databaseName)
          .skipArchive(awsProperties.glueCatalogSkipArchive())
          .tableInput(TableInput.builder()
              .name(tableName)
              .tableType(GLUE_EXTERNAL_TABLE_TYPE)
              .parameters(parameters)
              .build())
          .build());
    } else {
      LOG.debug("Committing new Glue table: {}", tableName());
      glue.createTable(CreateTableRequest.builder()
          .catalogId(awsProperties.glueCatalogId())
          .databaseName(databaseName)
          .tableInput(TableInput.builder()
              .name(tableName)
              .tableType(GLUE_EXTERNAL_TABLE_TYPE)
              .parameters(parameters)
              .build())
          .build());
    }
  }

  private void cleanupMetadataAndUnlock(boolean exceptionThrown, String metadataLocation) {
    try {
      if (exceptionThrown) {
        // if anything went wrong, clean up the uncommitted metadata file
        io().deleteFile(metadataLocation);
      }
    } catch (RuntimeException e) {
      LOG.error("Fail to cleanup metadata file at {}", metadataLocation, e);
      throw e;
    } finally {
      lockManager.release(commitLockEntityId, metadataLocation);
    }
  }
}
