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
package org.apache.iceberg.snowflake;

import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SnowflakeTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeTableOperations.class);

  private final FileIO fileIO;
  private final TableIdentifier tableIdentifier;
  private final SnowflakeIdentifier snowflakeIdentifierForTable;
  private final String fullTableName;

  private final SnowflakeClient snowflakeClient;

  protected SnowflakeTableOperations(
      SnowflakeClient snowflakeClient,
      FileIO fileIO,
      String catalogName,
      TableIdentifier tableIdentifier) {
    this.snowflakeClient = snowflakeClient;
    this.fileIO = fileIO;
    this.tableIdentifier = tableIdentifier;
    this.snowflakeIdentifierForTable = NamespaceHelpers.toSnowflakeIdentifier(tableIdentifier);
    this.fullTableName = String.format("%s.%s", catalogName, tableIdentifier);
  }

  @Override
  public void doRefresh() {
    LOG.debug("Getting metadata location for table {}", tableIdentifier);
    String location = loadTableMetadataLocation();
    Preconditions.checkState(
        location != null && !location.isEmpty(),
        "Got null or empty location %s for table %s",
        location,
        tableIdentifier);
    refreshFromMetadataLocation(location);
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected String tableName() {
    return fullTableName;
  }

  @VisibleForTesting
  String fullTableName() {
    return tableName();
  }

  private String loadTableMetadataLocation() {
    SnowflakeTableMetadata metadata =
        snowflakeClient.loadTableMetadata(snowflakeIdentifierForTable);

    if (metadata == null) {
      throw new NoSuchTableException("Cannot find table %s", snowflakeIdentifierForTable);
    }

    if (!metadata.getStatus().equals("success")) {
      LOG.warn(
          "Got non-successful table metadata: {} with metadataLocation {} for table {}",
          metadata.getStatus(),
          metadata.icebergMetadataLocation(),
          snowflakeIdentifierForTable);
    }

    return metadata.icebergMetadataLocation();
  }
}
