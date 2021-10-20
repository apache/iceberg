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
  private final CatalogDb db;

  protected JdbcTableOperations(CatalogDb db, FileIO fileIO, String catalogName,
                                TableIdentifier tableIdentifier) {
    this.db = db;
    this.catalogName = catalogName;
    this.tableIdentifier = tableIdentifier;
    this.fileIO = fileIO;
  }

  @Override
  public void doRefresh() {
    String newMetadataLocation;

    try {
      newMetadataLocation = db.getTablePointer(JdbcUtil.namespaceToString(tableIdentifier.namespace()), tableIdentifier.name());
    } catch (CatalogDbException e) {
      throw JdbcUtil.toIcebergExceptionIfPossible(e, tableIdentifier.namespace(), tableIdentifier, null);
    }

    if (newMetadataLocation == null) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException("Failed to load table %s from catalog %s: dropped by another process",
            tableIdentifier, catalogName);
      }
      this.disableRefresh();
      return;
    }
    refreshFromMetadataLocation(newMetadataLocation);
  }

  @Override
  public void doCommit(TableMetadata base, TableMetadata metadata) {
    final String namespace = JdbcUtil.namespaceToString(tableIdentifier.namespace());
    final String tableName = tableIdentifier.name();
    final String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);
    try {
      String tableMetadataLocation = db.getTablePointer(namespace, tableName);

      if (tableMetadataLocation != null) {
        String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
        if (!Objects.equals(baseMetadataLocation, tableMetadataLocation)) {
          throw new CommitFailedException("Cannot commit %s: metadata location %s has changed from %s",
                  tableIdentifier, baseMetadataLocation, tableMetadataLocation);
        }
        // Start atomic update
        LOG.debug("Committing existing table: {}", tableName());
        db.updateTable(namespace, tableName, tableMetadataLocation, newMetadataLocation);
      } else {
        // table not exists create it
        LOG.debug("Committing new table: {}", tableName());
        db.insertTable(catalogName, namespace, tableName, newMetadataLocation);
      }
    } catch (CatalogDbException e) {
      throw JdbcUtil.toIcebergExceptionIfPossible(e, null, null, null);
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
