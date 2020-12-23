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

package org.apache.iceberg.jdbc;

import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JdbcTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcTableOperations.class);
  private final String catalogName;
  private final TableIdentifier tableIdentifier;
  private final FileIO fileIO;
  private final TableSQL tableSQL;

  protected JdbcTableOperations(TableSQL tableSQL, FileIO fileIO, String catalogName, TableIdentifier tableIdentifier) {
    this.catalogName = catalogName;
    this.tableIdentifier = tableIdentifier;
    this.fileIO = fileIO;
    this.tableSQL = tableSQL;
  }

  @Override
  public void doRefresh() {
    String metadataLocation = null;
    Map<String, String> table;

    try {
      table = tableSQL.getTable(tableIdentifier);
    } catch (SQLException | InterruptedException e) {
      // unknown exception happened when getting table from catalog
      throw new RuntimeException(String.format("Failed to get table from catalog %s.%s", catalogName,
              tableIdentifier), e);
    }

    // Table not exists AND currentMetadataLocation is not NULL!
    if (table.isEmpty() && currentMetadataLocation() != null) {
      throw new NoSuchTableException("Failed to get table from catalog %s.%s!" +
              " maybe another process deleted it!", catalogName, tableIdentifier);
    }
    // Table not exists in the catalog! metadataLocation is null here!
    if (table.isEmpty()) {
      refreshFromMetadataLocation(metadataLocation);
      return;
    }
    // Table exists but metadataLocation is null
    if (table.getOrDefault("metadata_location", null) == null) {
      throw new RuntimeException(String.format("Failed to get metadata location if the table %s.%s", catalogName,
              tableIdentifier));
    }

    metadataLocation = table.get("metadata_location");
    refreshFromMetadataLocation(metadataLocation);
  }

  @Override
  public void doCommit(TableMetadata base, TableMetadata metadata) {
    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);
    String oldMetadataLocation = null;
    try {
      if (tableSQL.exists(tableIdentifier)) {
        Map<String, String> table = tableSQL.getTable(tableIdentifier);
        oldMetadataLocation = table.get("metadata_location");
        validateMetadataLocation(table, base);
        // Start atomic update
        int updatedRecords = tableSQL.doCommit(tableIdentifier, oldMetadataLocation, newMetadataLocation);
        if (updatedRecords == 1) {
          LOG.debug("Successfully committed to existing table: {}", tableIdentifier);
        } else {
          throw new CommitFailedException("Failed to commit table: %s.%s! maybe another process changed it!",
                  catalogName, tableIdentifier);
        }
      } else {
        tableSQL.doCommitCreate(tableIdentifier, newMetadataLocation);
        LOG.debug("Successfully committed to new table: {}", tableIdentifier);
      }
    } catch (SQLException | InterruptedException e) {
      throw new CommitFailedException(e, "Failed to commit table: %s.%s", catalogName, tableIdentifier);
    }
  }

  private void validateMetadataLocation(Map<String, String> table, TableMetadata base) {
    String catalogMetadataLocation = !table.isEmpty() ? table.get("metadata_location") : null;
    String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
    if (!Objects.equals(baseMetadataLocation, catalogMetadataLocation)) {
      throw new CommitFailedException(
              "Cannot commit %s because base metadata location '%s' is not same as the current Catalog location '%s'",
              tableIdentifier, baseMetadataLocation, catalogMetadataLocation);
    }
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected String tableName() {
    return tableIdentifier.toString();
  }

}
