/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.iceberg.ClientPool;

/**
 * Use plain jdbc driver to connect to a database, assuming SQLite-compatible SQL semantics.
 */
public class DefaultCatalogDb implements CatalogDb {
  // TODO is catalogName necessary in DB? We never check it on getTable.
  private static final String CATALOG_TABLE_NAME = "iceberg_tables";
  private static final String CATALOG_NAME = "catalog_name";
  private static final String TABLE_NAMESPACE = "table_namespace";
  private static final String TABLE_NAME = "table_name";
  private static final String METADATA_LOCATION = "metadata_location";
  private static final String PREVIOUS_METADATA_LOCATION = "previous_metadata_location";
  private static final String DO_COMMIT_SQL = "UPDATE " + CATALOG_TABLE_NAME +
      " SET " + METADATA_LOCATION + " = ? , " + PREVIOUS_METADATA_LOCATION + " = ? " +
      " WHERE " + CATALOG_NAME + " = ? AND " +
      TABLE_NAMESPACE + " = ? AND " +
      TABLE_NAME + " = ? AND " +
      METADATA_LOCATION + " = ?";
  private static final String CREATE_CATALOG_TABLE =
      "CREATE TABLE " + CATALOG_TABLE_NAME +
          "(" +
          CATALOG_NAME + " VARCHAR(255) NOT NULL," +
          TABLE_NAMESPACE + " VARCHAR(255) NOT NULL," +
          TABLE_NAME + " VARCHAR(255) NOT NULL," +
          METADATA_LOCATION + " VARCHAR(5500)," +
          PREVIOUS_METADATA_LOCATION + " VARCHAR(5500)," +
          "PRIMARY KEY (" + CATALOG_NAME + ", " + TABLE_NAMESPACE + ", " + TABLE_NAME + ")" +
          ")";
  private static final String GET_TABLE_SQL = "SELECT * FROM " + CATALOG_TABLE_NAME +
      " WHERE " + CATALOG_NAME + " = ? AND " + TABLE_NAMESPACE + " = ? AND " + TABLE_NAME + " = ? ";
  private static final String LIST_TABLES_SQL = "SELECT * FROM " + CATALOG_TABLE_NAME +
      " WHERE " + CATALOG_NAME + " = ? AND " + TABLE_NAMESPACE + " = ?";
  private static final String RENAME_TABLE_SQL = "UPDATE " + CATALOG_TABLE_NAME +
      " SET " + TABLE_NAMESPACE + " = ? , " + TABLE_NAME + " = ? " +
      " WHERE " + CATALOG_NAME + " = ? AND " + TABLE_NAMESPACE + " = ? AND " + TABLE_NAME + " = ? ";
  private static final String DROP_TABLE_SQL = "DELETE FROM " + CATALOG_TABLE_NAME +
      " WHERE " + CATALOG_NAME + " = ? AND " + TABLE_NAMESPACE + " = ? AND " + TABLE_NAME + " = ? ";
  private static final String GET_NAMESPACE_SQL = "SELECT " + TABLE_NAMESPACE + " FROM " + CATALOG_TABLE_NAME +
      " WHERE " + CATALOG_NAME + " = ? AND " + TABLE_NAMESPACE + " LIKE ? LIMIT 1";
  private static final String COUNT_NAMESPACE_TABLE_SQL = "SELECT COUNT(*) FROM " + CATALOG_TABLE_NAME +
      " WHERE " + CATALOG_NAME + " = ? AND " + TABLE_NAMESPACE + " LIKE ? ";
  private static final String LIST_NAMESPACES_SQL = "SELECT DISTINCT " + TABLE_NAMESPACE +
      " FROM " + CATALOG_TABLE_NAME +
      " WHERE " + CATALOG_NAME + " = ? AND " + TABLE_NAMESPACE + " LIKE ?";
  private static final String DO_COMMIT_CREATE_TABLE_SQL = "INSERT INTO " + CATALOG_TABLE_NAME +
      " (" + CATALOG_NAME + ", " + TABLE_NAMESPACE + ", " + TABLE_NAME +
      ", " + METADATA_LOCATION + ", " + PREVIOUS_METADATA_LOCATION + ") " +
      " VALUES (?,?,?,?,null)";

  private final String catalogName;
  private final JdbcClientPool connections;

  public DefaultCatalogDb(String catalogName, JdbcClientPool connections) {
    this.catalogName = catalogName;
    this.connections = connections;
  }

  @Override
  public void close() {
    connections.close();
  }

  /**
   * Ensures that SQL tables exist, o.w., create table(s).
   */
  @Override
  public void initialize() throws CatalogDbException {
    query(connections, "initialize db", conn -> {
      DatabaseMetaData dbMeta = conn.getMetaData();
      ResultSet tableExists = dbMeta.getTables(null, null, CATALOG_TABLE_NAME, null);

      if (tableExists.next()) {
        return null;
      }
      return conn.prepareStatement(CREATE_CATALOG_TABLE).execute();
    });
  }

  @Override
  public String getTablePointer(String namespaceName, String tableName) throws CatalogDbException {
    return query(connections, "get table pointer", conn -> {
      try (PreparedStatement sql = conn.prepareStatement(GET_TABLE_SQL)) {
        sql.setString(1, catalogName);
        sql.setString(2, namespaceName);
        sql.setString(3, tableName);
        try (ResultSet rs = sql.executeQuery()) {
          if (rs.next()) {
            return rs.getString(METADATA_LOCATION);
          }
          return null;
        }
      }
    });
  }

  @Override
  public List<String> listTables(String namespace) throws CatalogDbException {
    return query(connections, "list tables within namesapce", conn -> {
      List<String> results = new ArrayList<>();
      try (PreparedStatement sql = conn.prepareStatement(LIST_TABLES_SQL)) {
        sql.setString(1, catalogName);
        sql.setString(2, namespace);

        try (ResultSet rs = sql.executeQuery()) {
          while (rs.next()) {
            results.add(rs.getString(TABLE_NAME));
          }
          return results;
        }
      }
    });
  }

  @Override
  public List<String> listNamespaceByPrefix(String namespacePrefix) throws CatalogDbException {
    return query(connections, "list namespace by prefix", conn -> {
      List<String> result = new ArrayList<>();

      try (PreparedStatement sql = conn.prepareStatement(LIST_NAMESPACES_SQL)) {
        sql.setString(1, catalogName);
        sql.setString(2, namespacePrefix + "%");
        try (ResultSet rs = sql.executeQuery()) {
          while (rs.next()) {
            result.add(rs.getString(TABLE_NAMESPACE));
          }
        }
      }

      return result;
    });
  }

  @Override
  public boolean namespaceExists(String namespaceName) throws CatalogDbException {
    return query(connections, "check namespace exists", conn -> {
      try (PreparedStatement sql = conn.prepareStatement(GET_NAMESPACE_SQL)) {
        sql.setString(1, catalogName);
        sql.setString(2, namespaceName + "%");
        try (ResultSet rs = sql.executeQuery()) {
          if (rs.next()) {
            return true;
          }
          return false;
        }
      }
    });
  }

  @Override
  public boolean isNamespaceEmpty(String namespaceName) throws CatalogDbException {
    return query(connections, "check namespace is empty", conn -> {
      try (PreparedStatement sql = conn.prepareStatement(COUNT_NAMESPACE_TABLE_SQL)) {
        sql.setString(1, catalogName);
        sql.setString(2, namespaceName + "%");
        try (ResultSet rs = sql.executeQuery()) {
          if (rs.next()) {
            return rs.getInt(1) == 0;
          }
          throw new SQLException("COUNT query returns nothing.");
        }
      }
    });
  }

  @Override
  public void updateTable(
      String namespaceName,
      String tableName,
      String oldPointer,
      String newPointer) throws CatalogDbException {
    executeUpdateAndAssertExactOne(connections, "Update table", conn -> {
      try (PreparedStatement sql = conn.prepareStatement(DO_COMMIT_SQL)) {
        // UPDATE
        sql.setString(1, newPointer);
        sql.setString(2, oldPointer);
        // WHERE
        sql.setString(3, catalogName);
        sql.setString(4, namespaceName);
        sql.setString(5, tableName);
        sql.setString(6, oldPointer);
        return sql.executeUpdate();
      }
    });
  }

  @Override
  public void insertTable(String catalogName, String namespaceName, String tableName, String tablePointer) throws CatalogDbException {
    executeUpdateAndAssertExactOne(connections, "Insert table", conn -> {
      try (PreparedStatement sql = conn.prepareStatement(DO_COMMIT_CREATE_TABLE_SQL)) {
        sql.setString(1, catalogName);
        sql.setString(2, namespaceName);
        sql.setString(3, tableName);
        sql.setString(4, tablePointer);
        return sql.executeUpdate();
      }
    });
  }

  @Override
  public boolean dropTable(String namespaceName, String tableName) throws CatalogDbException {
    int dropped = query(connections, "Drop table", conn -> {
      try (PreparedStatement sql = conn.prepareStatement(DROP_TABLE_SQL)) {
        sql.setString(1, catalogName);
        sql.setString(2, namespaceName);
        sql.setString(3, tableName);
        return sql.executeUpdate();
      }
    });
    return dropped == 1;
  }

  @Override
  public void renameTable(String sourceNamespace, String sourceTable, String newNamespace, String newTable) throws CatalogDbException {
    executeUpdateAndAssertExactOne(connections, "Rename table", conn -> {
      try (PreparedStatement sql = conn.prepareStatement(RENAME_TABLE_SQL)) {
        // SET
        sql.setString(1, newNamespace);
        sql.setString(2, newTable);
        // WHERE
        sql.setString(3, catalogName);
        sql.setString(4, sourceNamespace);
        sql.setString(5, sourceTable);
        return sql.executeUpdate();
      }
    });
  }

  private static <R> R query(
      JdbcClientPool connections,
      String queryName,
      ClientPool.Action<R, Connection, SQLException> action) throws CatalogDbException {
    try {
      return connections.run(action::run);
    } catch (SQLException e) {
      throw CatalogDbException.fromSqlException(queryName, e);
    } catch (InterruptedException e) {
      throw CatalogDbException.wrapInterruption(queryName, e);
    }
  }

  private static void executeUpdateAndAssertExactOne(
      JdbcClientPool connections,
      String updateName,
      ClientPool.Action<Integer, Connection, SQLException> action) throws CatalogDbException {
    try {
      int updatedRow = connections.run(action);
      if (updatedRow == 1) {
        return;
      }
      if (updatedRow == 0) {
        throw new CatalogDbException(updateName + " failed: no entry was found.", CatalogDbException.Code.NOT_EXISTS);
      }
      throw new CatalogDbException(updateName + " failed: " + updatedRow + " (instead of 1) was updated. ", CatalogDbException.Code.UNKNOWN);
    } catch (SQLException e) {
      throw CatalogDbException.fromSqlException(updateName, e);
    } catch (InterruptedException e) {
      throw CatalogDbException.wrapInterruption(updateName, e);
    }
  }
}
