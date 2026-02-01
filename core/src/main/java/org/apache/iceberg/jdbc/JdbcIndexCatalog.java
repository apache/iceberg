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

import java.io.Closeable;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.IndexIdentifier;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchIndexException;
import org.apache.iceberg.index.BaseIndexCatalog;
import org.apache.iceberg.index.ImmutableIndexSummary;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexOperations;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.IndexSummary;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.LocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC implementation of an Index Catalog that stores index metadata in a JDBC database.
 *
 * <p>This catalog uses a JDBC connection pool to store and retrieve index metadata. The indexes
 * table schema includes catalog_name, table_namespace, table_name, index_name, and
 * metadata_location.
 */
public class JdbcIndexCatalog extends BaseIndexCatalog implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcIndexCatalog.class);
  private static final Joiner SLASH = Joiner.on("/");

  private final JdbcClientPool connections;
  private final Catalog tableCatalog;
  private FileIO io;
  private String catalogName;
  private String warehouseLocation;
  private CloseableGroup closeableGroup;
  private Map<String, String> catalogProperties;

  /**
   * Creates a new JdbcIndexCatalog.
   *
   * @param tableCatalog the catalog that manages tables
   * @param connections the JDBC client pool for database connections
   * @param fileIO the FileIO to use for reading and writing index metadata
   */
  public JdbcIndexCatalog(Catalog tableCatalog, JdbcClientPool connections, FileIO fileIO) {
    Preconditions.checkArgument(tableCatalog != null, "Table catalog cannot be null");
    Preconditions.checkArgument(connections != null, "JDBC client pool cannot be null");
    this.tableCatalog = tableCatalog;
    this.connections = connections;
    this.io = fileIO;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
    this.catalogName = name != null ? name : JdbcIndexCatalog.class.getSimpleName();
    this.catalogProperties = ImmutableMap.copyOf(properties);

    String warehouse =
        LocationUtil.stripTrailingSlash(
            properties.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, ""));
    this.warehouseLocation = warehouse;

    if (this.io == null) {
      String ioImpl =
          properties.getOrDefault(
              CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.hadoop.HadoopFileIO");
      this.io = CatalogUtil.loadFileIO(ioImpl, properties, null);
    }

    this.closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(io);
    closeableGroup.setSuppressCloseFailure(true);

    initializeCatalogTables();
  }

  private void initializeCatalogTables() {
    LOG.trace("Creating database tables (if missing) to store iceberg indexes");

    try {
      atomicCreateTable(
          JdbcUtil.CATALOG_INDEX_TABLE_NAME,
          JdbcUtil.CREATE_CATALOG_INDEX_SQL,
          "to store iceberg catalog indexes");
    } catch (SQLTimeoutException e) {
      throw new UncheckedSQLException(e, "Cannot initialize JDBC index catalog: Query timed out");
    } catch (SQLTransientConnectionException | SQLNonTransientConnectionException e) {
      throw new UncheckedSQLException(e, "Cannot initialize JDBC index catalog: Connection failed");
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Cannot initialize JDBC index catalog");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted in call to initialize");
    }
  }

  private void atomicCreateTable(String tableName, String sqlCommand, String reason)
      throws SQLException, InterruptedException {
    connections.run(
        conn -> {
          DatabaseMetaData dbMeta = conn.getMetaData();

          Predicate<String> tableTest =
              tblName -> {
                try {
                  ResultSet result =
                      dbMeta.getTables(
                          null /* catalog name */,
                          null /* schemaPattern */,
                          tblName /* tableNamePattern */,
                          null /* types */);
                  return result.next();
                } catch (SQLException e) {
                  return false;
                }
              };

          Predicate<String> tableExists =
              tblName ->
                  tableTest.test(tblName) || tableTest.test(tblName.toUpperCase(Locale.ROOT));

          if (tableExists.test(tableName)) {
            return true;
          }

          LOG.debug("Creating table {} {}", tableName, reason);
          try {
            conn.prepareStatement(sqlCommand).execute();
            return true;
          } catch (SQLException e) {
            if (tableExists.test(tableName)) {
              return true;
            }
            throw e;
          }
        });
  }

  @Override
  protected Catalog tableCatalog() {
    return tableCatalog;
  }

  @Override
  protected IndexOperations newIndexOps(IndexIdentifier identifier) {
    return new JdbcIndexOperations(connections, io, catalogName, identifier, catalogProperties);
  }

  @Override
  protected String defaultIndexLocation(IndexIdentifier identifier) {
    return SLASH.join(
        warehouseLocation,
        SLASH.join(identifier.tableIdentifier().namespace().levels()),
        identifier.tableIdentifier().name(),
        "indexes",
        identifier.name());
  }

  @Override
  protected List<IndexSummary> doListIndexes(TableIdentifier tableIdentifier) {
    List<IndexIdentifier> indexIdentifiers = listIndexIdentifiers(tableIdentifier);
    return indexIdentifiers.stream()
        .sorted(Comparator.comparing(IndexIdentifier::toString))
        .map(this::loadIndexSummary)
        .collect(Collectors.toList());
  }

  private List<IndexIdentifier> listIndexIdentifiers(TableIdentifier tableIdentifier) {
    try {
      return connections.run(
          conn -> {
            List<IndexIdentifier> result = Lists.newArrayList();
            try (PreparedStatement sql = conn.prepareStatement(JdbcUtil.LIST_INDEXES_SQL)) {
              sql.setString(1, catalogName);
              sql.setString(2, JdbcUtil.namespaceToString(tableIdentifier.namespace()));
              sql.setString(3, tableIdentifier.name());
              ResultSet rs = sql.executeQuery();

              while (rs.next()) {
                String indexName = rs.getString(JdbcUtil.INDEX_NAME);
                result.add(IndexIdentifier.of(tableIdentifier, indexName));
              }

              rs.close();
            }
            return result;
          });
    } catch (SQLException e) {
      throw new UncheckedSQLException(
          e, "Failed to list indexes for table %s in catalog %s", tableIdentifier, catalogName);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted while listing indexes");
    }
  }

  private IndexSummary loadIndexSummary(IndexIdentifier identifier) {
    IndexOperations ops = newIndexOps(identifier);
    IndexMetadata metadata = ops.current();
    if (metadata == null) {
      throw new NoSuchIndexException("Index does not exist: %s", identifier);
    }

    long[] availableSnapshots =
        metadata.snapshots().stream().mapToLong(IndexSnapshot::tableSnapshotId).toArray();

    return ImmutableIndexSummary.builder()
        .id(identifier)
        .type(metadata.type())
        .indexColumnIds(metadata.indexColumnIds().stream().mapToInt(Integer::intValue).toArray())
        .optimizedColumnIds(
            metadata.optimizedColumnIds().stream().mapToInt(Integer::intValue).toArray())
        .availableTableSnapshots(availableSnapshots)
        .build();
  }

  @Override
  protected boolean doDropIndex(IndexIdentifier identifier) {
    try {
      int deletedRecords =
          connections.run(
              conn -> {
                try (PreparedStatement sql = conn.prepareStatement(JdbcUtil.DROP_INDEX_SQL)) {
                  sql.setString(1, catalogName);
                  sql.setString(2, JdbcUtil.namespaceToString(identifier.namespace()));
                  sql.setString(3, identifier.tableName());
                  sql.setString(4, identifier.name());
                  return sql.executeUpdate();
                }
              });

      if (deletedRecords == 0) {
        LOG.info("Skipping drop, index does not exist: {}", identifier);
        return false;
      }

      LOG.info("Dropped index: {}", identifier);
      return true;
    } catch (SQLException e) {
      throw new UncheckedSQLException(
          e, "Failed to drop index %s from catalog %s", identifier, catalogName);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted while dropping index");
    }
  }

  @Override
  protected Map<String, String> properties() {
    return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
  }

  @Override
  public void close() throws IOException {
    if (closeableGroup != null) {
      closeableGroup.close();
    }
  }
}
