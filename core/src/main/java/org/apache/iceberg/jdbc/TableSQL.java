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

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableSQL {

  public static final String SQL_TABLE_NAME = "iceberg_tables";
  public static final String SQL_TABLE_DDL =
          "CREATE TABLE " + TableSQL.SQL_TABLE_NAME +
                  "(catalog_name VARCHAR(1255) NOT NULL," +
                  "table_namespace VARCHAR(1255) NOT NULL," +
                  "table_name VARCHAR(1255) NOT NULL," +
                  "metadata_location VARCHAR(32768)," +
                  "previous_metadata_location VARCHAR(32768)," +
                  "PRIMARY KEY (catalog_name, table_namespace, table_name)" +
                  ")";
  public static final String SQL_SELECT = "SELECT * FROM " + SQL_TABLE_NAME +
          " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? ";
  public static final String SQL_SELECT_ALL = "SELECT * FROM " + SQL_TABLE_NAME +
          " WHERE catalog_name = ? AND table_namespace = ?";
  public static final String SQL_UPDATE_METADATA_LOCATION = "UPDATE " + SQL_TABLE_NAME +
          " SET metadata_location = ? , previous_metadata_location = ? " +
          " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? AND metadata_location = ?";
  public static final String SQL_INSERT = "INSERT INTO " + SQL_TABLE_NAME +
          " (catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location) " +
          " VALUES (?,?,?,?,?)";
  public static final String SQL_UPDATE_TABLE_NAME = "UPDATE " + SQL_TABLE_NAME +
          " SET table_namespace = ? , table_name = ? " +
          " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? ";
  public static final String SQL_DELETE = "DELETE FROM " + SQL_TABLE_NAME +
          " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? ";
  public static final String SQL_SELECT_NAMESPACE = "SELECT table_namespace FROM " + SQL_TABLE_NAME +
          " WHERE table_namespace LIKE ? LIMIT 1";
  public static final String SQL_SELECT_NAMESPACES = "SELECT DISTINCT table_namespace FROM " + SQL_TABLE_NAME +
          " WHERE table_namespace LIKE ?";
  protected static final Joiner JOINER_DOT = Joiner.on('.');
  protected static final Splitter SPLITTER_DOT = Splitter.on('.');
  private static final Logger LOG = LoggerFactory.getLogger(TableSQL.class);
  private final String catalogName;
  private final JdbcClientPool dbConn;

  public TableSQL(JdbcClientPool dbConn, String catalogName) {
    this.dbConn = dbConn;
    this.catalogName = catalogName;
  }

  public static boolean tableExists(DatabaseMetaData jdbcMetadata, String tableName) throws SQLException {
    // need to check multiple times because some databases using different naming standards. ex: H2db keeping
    // tables with uppercase name
    boolean isExists = false;
    ResultSet tables = jdbcMetadata.getTables(null, null, tableName, new String[]{"TABLE"});
    if (tables.next()) {
      isExists = true;
    }
    tables.close();
    ResultSet tablesUpper = jdbcMetadata.getTables(null, null, tableName.toUpperCase(), new String[]{"TABLE"});
    if (tablesUpper.next()) {
      isExists = true;
    }
    tablesUpper.close();
    ResultSet tablesLower = jdbcMetadata.getTables(null, null, tableName.toLowerCase(), new String[]{"TABLE"});
    if (tablesLower.next()) {
      isExists = true;
    }
    tablesLower.close();
    return isExists;
  }

  public Namespace stringToNamespace(String string) {
    if (string == null) {
      return null;
    }
    return Namespace.of(Iterables.toArray(SPLITTER_DOT.split(string), String.class));
  }

  public String namespaceToString(Namespace namespace) {
    return JOINER_DOT.join(namespace.levels());
  }

  public TableIdentifier stringToTableIdentifier(String tableNamespace, String tableName) {
    return TableIdentifier.of(this.stringToNamespace(tableNamespace), tableName);
  }

  public boolean exists(TableIdentifier tableIdentifier) {
    try {
      return !this.getTable(tableIdentifier).isEmpty();
    } catch (SQLException | InterruptedException e) {
      return false;
    }
  }

  public boolean namespaceExists(Namespace namespace) {
    boolean exists = false;
    try {
      PreparedStatement sql = dbConn.run(c -> c.prepareStatement(SQL_SELECT_NAMESPACE));
      sql.setString(1, this.namespaceToString(namespace) + "%");
      ResultSet rs = sql.executeQuery();
      if (rs.next()) {
        exists = true;
      }
      rs.close();
    } catch (SQLException | InterruptedException e) {
      LOG.warn("SQLException! ", e);
    }
    return exists;
  }

  /**
   * List namespaces from the namespace. For example, if table a.b.t exists, use 'SELECT NAMESPACE IN a' this method
   * must return Namepace.of("a","b") {@link Namespace}.
   **/
  public List<Namespace> listNamespaces(Namespace namespace) throws SQLException, InterruptedException {
    List<Namespace> namespaces = Lists.newArrayList();
    List<Map<String, String>> results = Lists.newArrayList();
    PreparedStatement sql = dbConn.run(c -> c.prepareStatement(SQL_SELECT_NAMESPACES));
    // SELECT DISTINCT table_namespace FROM iceberg_tables WHERE table_namespace LIKE 'ns%'
    if (namespace.isEmpty()) {
      sql.setString(1, this.namespaceToString(namespace) + "%");
    } else {
      sql.setString(1, this.namespaceToString(namespace) + ".%");
    }
    ResultSet rs = sql.executeQuery();
    while (rs.next()) {
      rs.getString("table_namespace");
      namespaces.add(this.stringToNamespace(rs.getString("table_namespace")));
    }
    rs.close();
    int subNamespaceLevelLength = namespace.levels().length + 1;
    namespaces = namespaces.stream()
            .filter(n -> !n.equals(namespace)) // exclude itself
            .map(n -> Namespace.of(
                    Arrays.stream(n.levels()).limit(subNamespaceLevelLength).toArray(String[]::new)
                    )
            ) // only get sub namespaces/children
            .distinct()
            .collect(Collectors.toList());

    return namespaces;
  }

  public Map<String, String> getTable(TableIdentifier tableIdentifier) throws SQLException, InterruptedException {
    Map<String, String> table = Maps.newHashMap();
    PreparedStatement sql = dbConn.run(c -> c.prepareStatement(SQL_SELECT));
    sql.setString(1, catalogName);
    sql.setString(2, this.namespaceToString(tableIdentifier.namespace()));
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

  public List<TableIdentifier> listTables(Namespace namespace) throws SQLException, InterruptedException {
    List<TableIdentifier> results = Lists.newArrayList();
    PreparedStatement sql = dbConn.run(c -> c.prepareStatement(SQL_SELECT_ALL));
    sql.setString(1, catalogName);
    sql.setString(2, this.namespaceToString(namespace));
    ResultSet rs = sql.executeQuery();

    while (rs.next()) {
      final TableIdentifier table = this.stringToTableIdentifier(
              rs.getString("table_namespace"), rs.getString("table_name"));
      results.add(table);
    }
    rs.close();
    return results;
  }

  public int doCommit(TableIdentifier tableIdentifier, String oldMetadataLocation,
                      String newMetadataLocation) throws SQLException, InterruptedException {
    PreparedStatement sql = dbConn.run(c -> c.prepareStatement(SQL_UPDATE_METADATA_LOCATION));
    // UPDATE
    sql.setString(1, newMetadataLocation);
    sql.setString(2, oldMetadataLocation);
    // WHERE
    sql.setString(3, catalogName);
    sql.setString(4, this.namespaceToString(tableIdentifier.namespace()));
    sql.setString(5, tableIdentifier.name());
    sql.setString(6, oldMetadataLocation);
    return sql.executeUpdate();
  }

  public int doCommitCreate(TableIdentifier tableIdentifier, String oldMetadataLocation,
                            String newMetadataLocation) throws SQLException, InterruptedException {
    PreparedStatement sql = dbConn.run(c -> c.prepareStatement(SQL_INSERT));
    sql.setString(1, catalogName);
    sql.setString(2, this.namespaceToString(tableIdentifier.namespace()));
    sql.setString(3, tableIdentifier.name());
    sql.setString(4, newMetadataLocation);
    sql.setString(5, oldMetadataLocation);
    return sql.executeUpdate();

  }

  public int renameTable(TableIdentifier from, TableIdentifier to) throws SQLException, InterruptedException {
    PreparedStatement sql = dbConn.run(c -> c.prepareStatement(SQL_UPDATE_TABLE_NAME));
    sql.setString(1, this.namespaceToString(to.namespace()));
    sql.setString(2, to.name());
    sql.setString(3, catalogName);
    sql.setString(4, this.namespaceToString(from.namespace()));
    sql.setString(5, from.name());
    return sql.executeUpdate();

  }

  public int dropTable(TableIdentifier identifier) throws SQLException, InterruptedException {
    PreparedStatement sql = dbConn.run(c -> c.prepareStatement(SQL_DELETE));
    sql.setString(1, catalogName);
    sql.setString(2, this.namespaceToString(identifier.namespace()));
    sql.setString(3, identifier.name());
    int deletedRecords = sql.executeUpdate();

    if (deletedRecords > 0) {
      LOG.debug("Successfully dropped table {}.", identifier);
    } else {
      LOG.info("Cannot drop table {}! table not found in the Catalog.", identifier);
    }

    return deletedRecords;
  }


}
