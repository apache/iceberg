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

import java.sql.DataTruncation;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLWarning;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.catalog.IndexIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchIndexException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.index.BaseIndexOperations;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JDBC implementation of Iceberg IndexOperations. */
public class JdbcIndexOperations extends BaseIndexOperations {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcIndexOperations.class);
  private final String catalogName;
  private final IndexIdentifier indexIdentifier;
  private final FileIO fileIO;
  private final JdbcClientPool connections;
  private final Map<String, String> catalogProperties;

  protected JdbcIndexOperations(
      JdbcClientPool dbConnPool,
      FileIO fileIO,
      String catalogName,
      IndexIdentifier indexIdentifier,
      Map<String, String> catalogProperties) {
    this.catalogName = catalogName;
    this.indexIdentifier = indexIdentifier;
    this.fileIO = fileIO;
    this.connections = dbConnPool;
    this.catalogProperties = catalogProperties;
  }

  @Override
  protected void doRefresh() {
    Map<String, String> index;

    try {
      index = JdbcUtil.loadIndex(connections, catalogName, indexIdentifier);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted during refresh");
    } catch (SQLException e) {
      // SQL exception happened when getting index from catalog
      throw new UncheckedSQLException(
          e, "Failed to get index %s from catalog %s", indexIdentifier, catalogName);
    }

    if (index.isEmpty()) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchIndexException("Index does not exist: %s", indexIdentifier);
      } else {
        this.disableRefresh();
        return;
      }
    }

    String newMetadataLocation = index.get(JdbcTableOperations.METADATA_LOCATION_PROP);
    Preconditions.checkState(
        newMetadataLocation != null,
        "Invalid index %s: metadata location is null",
        indexIdentifier);
    refreshFromMetadataLocation(newMetadataLocation);
  }

  @Override
  protected void doCommit(IndexMetadata base, IndexMetadata metadata) {
    String newMetadataLocation = writeNewMetadataIfRequired(metadata);
    try {
      Map<String, String> index = JdbcUtil.loadIndex(connections, catalogName, indexIdentifier);
      if (base != null) {
        validateMetadataLocation(index, base);
        String oldMetadataLocation = base.metadataFileLocation();
        // Start atomic update
        LOG.debug("Committing existing index: {}", indexName());
        updateIndex(newMetadataLocation, oldMetadataLocation);
      } else {
        // index does not exist, create it
        LOG.debug("Committing new index: {}", indexName());
        createIndex(newMetadataLocation);
      }

    } catch (SQLIntegrityConstraintViolationException e) {
      if (currentMetadataLocation() == null) {
        throw new AlreadyExistsException(e, "Index already exists: %s", indexIdentifier);
      } else {
        throw new UncheckedSQLException(e, "Index already exists: %s", indexIdentifier);
      }

    } catch (SQLTimeoutException e) {
      throw new UncheckedSQLException(e, "Database Connection timeout");
    } catch (SQLTransientConnectionException | SQLNonTransientConnectionException e) {
      throw new UncheckedSQLException(e, "Database Connection failed");
    } catch (DataTruncation e) {
      throw new UncheckedSQLException(e, "Database data truncation error");
    } catch (SQLWarning e) {
      throw new UncheckedSQLException(e, "Database warning");
    } catch (SQLException e) {
      // SQLite doesn't set SQLState or throw SQLIntegrityConstraintViolationException
      if (e.getMessage() != null && e.getMessage().contains("constraint failed")) {
        throw new AlreadyExistsException("Index already exists: %s", indexIdentifier);
      }

      throw new UncheckedSQLException(e, "Unknown failure");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted during commit");
    }
  }

  @Override
  protected String indexName() {
    return indexIdentifier.toString();
  }

  @Override
  protected FileIO io() {
    return fileIO;
  }

  private void validateMetadataLocation(Map<String, String> index, IndexMetadata base) {
    String catalogMetadataLocation = index.get(JdbcTableOperations.METADATA_LOCATION_PROP);
    String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;

    if (!Objects.equals(baseMetadataLocation, catalogMetadataLocation)) {
      throw new CommitFailedException(
          "Cannot commit %s: metadata location %s has changed from %s",
          indexIdentifier, baseMetadataLocation, catalogMetadataLocation);
    }
  }

  private void updateIndex(String newMetadataLocation, String oldMetadataLocation)
      throws SQLException, InterruptedException {
    int updatedRecords =
        JdbcUtil.updateIndex(
            connections, catalogName, indexIdentifier, newMetadataLocation, oldMetadataLocation);

    if (updatedRecords == 1) {
      LOG.debug("Successfully committed to existing index: {}", indexIdentifier);
    } else {
      throw new CommitFailedException(
          "Failed to update index %s from catalog %s", indexIdentifier, catalogName);
    }
  }

  private void createIndex(String newMetadataLocation) throws SQLException, InterruptedException {
    if (PropertyUtil.propertyAsBoolean(catalogProperties, JdbcUtil.STRICT_MODE_PROPERTY, false)
        && !JdbcUtil.namespaceExists(catalogName, connections, indexIdentifier.namespace())) {
      throw new NoSuchTableException(
          "Cannot create index %s in catalog %s. Namespace %s does not exist",
          indexIdentifier, catalogName, indexIdentifier.namespace());
    }

    if (!JdbcUtil.tableExists(
        JdbcUtil.SchemaVersion.V1, catalogName, connections, indexIdentifier.tableIdentifier())) {
      throw new NoSuchTableException(
          "Cannot create index %s. Table does not exist: %s",
          indexIdentifier, indexIdentifier.tableIdentifier());
    }

    if (JdbcUtil.indexExists(catalogName, connections, indexIdentifier)) {
      throw new AlreadyExistsException("Index already exists: %s", indexIdentifier);
    }

    int insertRecord =
        JdbcUtil.doCommitCreateIndex(
            connections, catalogName, indexIdentifier, newMetadataLocation);

    if (insertRecord == 1) {
      LOG.debug("Successfully committed to new index: {}", indexIdentifier);
    } else {
      throw new CommitFailedException(
          "Failed to create index %s in catalog %s", indexIdentifier, catalogName);
    }
  }
}
