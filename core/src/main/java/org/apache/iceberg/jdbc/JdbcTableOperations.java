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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLWarning;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JdbcTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcTableOperations.class);
  private final String catalogName;
  private final TableIdentifier tableIdentifier;
  private final FileIO fileIO;
  private final JdbcClientPool connections;
  private final Map<String, String> catalogProperties;

  protected JdbcTableOperations(
      JdbcClientPool dbConnPool,
      FileIO fileIO,
      String catalogName,
      TableIdentifier tableIdentifier,
      Map<String, String> catalogProperties) {
    this.catalogName = catalogName;
    this.tableIdentifier = tableIdentifier;
    this.fileIO = fileIO;
    this.connections = dbConnPool;
    this.catalogProperties = catalogProperties;
  }

  @Override
  public void doRefresh() {
    Map<String, String> table;

    try {
      table = getTable();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted during refresh");
    } catch (SQLException e) {
      // SQL exception happened when getting table from catalog
      throw new UncheckedSQLException(
          e, "Failed to get table %s from catalog %s", tableIdentifier, catalogName);
    }

    if (table.isEmpty()) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(
            "Failed to load table %s from catalog %s: dropped by another process",
            tableIdentifier, catalogName);
      } else {
        this.disableRefresh();
        return;
      }
    }

    String newMetadataLocation = table.get(METADATA_LOCATION_PROP);
    Preconditions.checkState(
        newMetadataLocation != null,
        "Invalid table %s: metadata location is null",
        tableIdentifier);
    refreshFromMetadataLocation(newMetadataLocation);
  }

  @Override
  public void doCommit(TableMetadata base, TableMetadata metadata) {
    boolean newTable = base == null;
    String newMetadataLocation = writeNewMetadataIfRequired(newTable, metadata);
    try {
      Map<String, String> table = getTable();

      if (base != null) {
        validateMetadataLocation(table, base);
        String oldMetadataLocation = base.metadataFileLocation();
        // Start atomic update
        LOG.debug("Committing existing table: {}", tableName());
        updateTable(newMetadataLocation, oldMetadataLocation);
      } else {
        // table not exists create it
        LOG.debug("Committing new table: {}", tableName());
        createTable(newMetadataLocation);
      }

    } catch (SQLIntegrityConstraintViolationException e) {

      if (currentMetadataLocation() == null) {
        throw new AlreadyExistsException(e, "Table already exists: %s", tableIdentifier);
      } else {
        throw new UncheckedSQLException(e, "Table already exists: %s", tableIdentifier);
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
      if (e.getMessage().contains("constraint failed")) {
        throw new AlreadyExistsException("Table already exists: %s", tableIdentifier);
      }
      throw new UncheckedSQLException(e, "Unknown failure");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted during commit");
    }
  }

  private void updateTable(String newMetadataLocation, String oldMetadataLocation)
      throws SQLException, InterruptedException {
    int updatedRecords =
        connections.run(
            conn -> {
              try (PreparedStatement sql = conn.prepareStatement(JdbcUtil.DO_COMMIT_SQL)) {
                // UPDATE
                sql.setString(1, newMetadataLocation);
                sql.setString(2, oldMetadataLocation);
                // WHERE
                sql.setString(3, catalogName);
                sql.setString(4, JdbcUtil.namespaceToString(tableIdentifier.namespace()));
                sql.setString(5, tableIdentifier.name());
                sql.setString(6, oldMetadataLocation);
                return sql.executeUpdate();
              }
            });

    if (updatedRecords == 1) {
      LOG.debug("Successfully committed to existing table: {}", tableIdentifier);
    } else {
      throw new CommitFailedException(
          "Failed to update table %s from catalog %s", tableIdentifier, catalogName);
    }
  }

  private void createTable(String newMetadataLocation) throws SQLException, InterruptedException {
    Namespace namespace = tableIdentifier.namespace();
    if (PropertyUtil.propertyAsBoolean(catalogProperties, JdbcUtil.STRICT_MODE_PROPERTY, false)
        && !JdbcUtil.namespaceExists(catalogName, connections, namespace)) {
      throw new NoSuchNamespaceException(
          "Cannot create table %s in catalog %s. Namespace %s does not exist",
          tableIdentifier, catalogName, namespace);
    }

    int insertRecord =
        connections.run(
            conn -> {
              try (PreparedStatement sql =
                  conn.prepareStatement(JdbcUtil.DO_COMMIT_CREATE_TABLE_SQL)) {
                sql.setString(1, catalogName);
                sql.setString(2, JdbcUtil.namespaceToString(namespace));
                sql.setString(3, tableIdentifier.name());
                sql.setString(4, newMetadataLocation);
                return sql.executeUpdate();
              }
            });

    if (insertRecord == 1) {
      LOG.debug("Successfully committed to new table: {}", tableIdentifier);
    } else {
      throw new CommitFailedException(
          "Failed to create table %s in catalog %s", tableIdentifier, catalogName);
    }
  }

  private void validateMetadataLocation(Map<String, String> table, TableMetadata base) {
    String catalogMetadataLocation = table.get(METADATA_LOCATION_PROP);
    String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;

    if (!Objects.equals(baseMetadataLocation, catalogMetadataLocation)) {
      throw new CommitFailedException(
          "Cannot commit %s: metadata location %s has changed from %s",
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

  private Map<String, String> getTable()
      throws UncheckedSQLException, SQLException, InterruptedException {
    return connections.run(
        conn -> {
          Map<String, String> table = Maps.newHashMap();

          try (PreparedStatement sql = conn.prepareStatement(JdbcUtil.GET_TABLE_SQL)) {
            sql.setString(1, catalogName);
            sql.setString(2, JdbcUtil.namespaceToString(tableIdentifier.namespace()));
            sql.setString(3, tableIdentifier.name());
            ResultSet rs = sql.executeQuery();

            if (rs.next()) {
              table.put(JdbcUtil.CATALOG_NAME, rs.getString(JdbcUtil.CATALOG_NAME));
              table.put(JdbcUtil.TABLE_NAMESPACE, rs.getString(JdbcUtil.TABLE_NAMESPACE));
              table.put(JdbcUtil.TABLE_NAME, rs.getString(JdbcUtil.TABLE_NAME));
              table.put(METADATA_LOCATION_PROP, rs.getString(METADATA_LOCATION_PROP));
              table.put(
                  PREVIOUS_METADATA_LOCATION_PROP, rs.getString(PREVIOUS_METADATA_LOCATION_PROP));
            }

            rs.close();
          }

          return table;
        });
  }
}
