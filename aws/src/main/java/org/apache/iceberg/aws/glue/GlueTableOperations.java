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
  private final FileIO fileIO;

  GlueTableOperations(GlueClient glue, String catalogName, AwsProperties awsProperties,
                      FileIO fileIO, TableIdentifier tableIdentifier) {
    this.glue = glue;
    this.awsProperties = awsProperties;
    this.databaseName = IcebergToGlueConverter.getDatabaseName(tableIdentifier);
    this.tableName = IcebergToGlueConverter.getTableName(tableIdentifier);
    this.fullTableName = String.format("%s.%s.%s", catalogName, databaseName, tableName);
    this.fileIO = fileIO;
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
    Table glueTable = getGlueTable();
    checkMetadataLocation(glueTable, base);
    Map<String, String> properties = prepareProperties(glueTable, newMetadataLocation);
    try {
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
      if (exceptionThrown) {
        io().deleteFile(newMetadataLocation);
      }
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
}
