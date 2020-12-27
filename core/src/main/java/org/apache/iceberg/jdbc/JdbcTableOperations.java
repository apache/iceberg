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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JdbcTableOperations extends BaseMetastoreTableOperations {

  public static final String SQL_UPDATE_METADATA_LOCATION = "UPDATE " + JdbcCatalog.SQL_TABLE_NAME +
          " SET metadata_location = ? , previous_metadata_location = ? " +
          " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? AND metadata_location = ?";
  public static final String SQL_INSERT = "INSERT INTO " + JdbcCatalog.SQL_TABLE_NAME +
          " (catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location) " +
          " VALUES (?,?,?,?,null)";
  private static final Logger LOG = LoggerFactory.getLogger(JdbcTableOperations.class);
  private final String catalogName;
  private final TableIdentifier tableIdentifier;
  private final FileIO fileIO;
  private final JdbcClientPool dbConnPool;

  protected JdbcTableOperations(JdbcClientPool dbConnPool, FileIO fileIO, String catalogName,
                                TableIdentifier tableIdentifier) {
    this.catalogName = catalogName;
    this.tableIdentifier = tableIdentifier;
    this.fileIO = fileIO;
    this.dbConnPool = dbConnPool;
  }

  @Override
  public void doRefresh() {
    Map<String, String> table;

    try {
      table = this.getTable();
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
      refreshFromMetadataLocation(null);
      return;
    }
    // Table exists but metadataLocation is null
    if (table.getOrDefault("metadata_location", null) == null) {
      throw new RuntimeException(String.format("Failed to get metadata location if the table %s.%s", catalogName,
              tableIdentifier));
    }

    refreshFromMetadataLocation(table.get("metadata_location"));
  }

  @Override
  public void doCommit(TableMetadata base, TableMetadata metadata) {
    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);
    try {
      Map<String, String> table = this.getTable();
      if (!table.isEmpty()) {
        validateMetadataLocation(table, base);
        String oldMetadataLocation = base.metadataFileLocation();
        // Start atomic update
        PreparedStatement sql = dbConnPool.run(c -> c.prepareStatement(SQL_UPDATE_METADATA_LOCATION));
        // UPDATE
        sql.setString(1, newMetadataLocation);
        sql.setString(2, oldMetadataLocation);
        // WHERE
        sql.setString(3, catalogName);
        sql.setString(4, JdbcUtil.namespaceToString(tableIdentifier.namespace()));
        sql.setString(5, tableIdentifier.name());
        sql.setString(6, oldMetadataLocation);
        int updatedRecords = sql.executeUpdate();

        if (updatedRecords == 1) {
          LOG.debug("Successfully committed to existing table: {}", tableIdentifier);
        } else {
          throw new CommitFailedException("Failed to commit table: %s.%s! maybe another process changed it!",
                  catalogName, tableIdentifier);
        }
      } else {
        // table not exists create it!
        PreparedStatement sql = dbConnPool.run(c -> c.prepareStatement(SQL_INSERT));
        sql.setString(1, catalogName);
        sql.setString(2, JdbcUtil.namespaceToString(tableIdentifier.namespace()));
        sql.setString(3, tableIdentifier.name());
        sql.setString(4, newMetadataLocation);
        int insertRecord = sql.executeUpdate();
        if (insertRecord == 1) {
          LOG.debug("Successfully committed to new table: {}", tableIdentifier);
        } else {
          throw new CommitFailedException("Failed to commit table: %s.%s", catalogName, tableIdentifier);
        }
      }
    } catch (SQLException e) {
      if (e.getSQLState().startsWith("23")) { // Unique index or primary key violation
        throw new AlreadyExistsException(e, "Table already exists! maybe another process created it!");
      } else {
        throw new CommitFailedException(e, "Failed to commit table: %s.%s", catalogName, tableIdentifier);
      }
    } catch (InterruptedException e) {
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

  private Map<String, String> getTable() throws SQLException, InterruptedException {
    Map<String, String> table = Maps.newHashMap();
    PreparedStatement sql = dbConnPool.run(c -> c.prepareStatement(JdbcCatalog.SQL_SELECT_TABLE));
    sql.setString(1, catalogName);
    sql.setString(2, JdbcUtil.namespaceToString(tableIdentifier.namespace()));
    sql.setString(3, tableIdentifier.name());
    ResultSet rs = sql.executeQuery();
    if (rs.next()) {
      table.put("catalog_name", rs.getString("catalog_name"));
      table.put("table_namespace", rs.getString("table_namespace"));
      table.put("table_name", rs.getString("table_name"));
      table.put("metadata_location", rs.getString("metadata_location"));
      table.put("previous_metadata_location", rs.getString("previous_metadata_location"));
    }
    rs.close();
    return table;
  }

}
