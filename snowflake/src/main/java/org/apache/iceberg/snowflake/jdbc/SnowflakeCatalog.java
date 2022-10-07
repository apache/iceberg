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
package org.apache.iceberg.snowflake.jdbc;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeCatalog extends BaseMetastoreCatalog
    implements Closeable, SupportsNamespaces, Configurable<Object> {

  public static final String PROPERTY_PREFIX = "jdbc.";
  private static final String NAMESPACE_EXISTS_PROPERTY = "exists";
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeCatalog.class);
  private static final Joiner SLASH = Joiner.on("/");

  private FileIO io;
  private String catalogName = "SnowflakeCatalog";
  private Object conf;
  private Connection connection;

  public SnowflakeCatalog() {}

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {

    List<TableIdentifier> tableList = Lists.newArrayList();
    try {
      Statement st = connection.createStatement();
      ResultSet results = st.executeQuery("show iceberg tables");
      while (results.next()) {
        tableList.add(
            TableIdentifier.of(
                results.getString("database_name"),
                results.getString("schema_name"),
                results.getString("name")));
        LOG.debug(
            "{} | {} | {}",
            results.getString("database_name"),
            results.getString("schema_name"),
            results.getString("name"));
      }
      results.close();
      st.close();
    } catch (SQLException ex) {
      LOG.error("{}", ex.toString(), ex);
    }
    return tableList;
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    return false;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {}

  @Override
  public void initialize(String name, Map<String, String> properties) {
    String uri = properties.get(CatalogProperties.URI);
    Preconditions.checkNotNull(uri, "JDBC connection URI is required");

    if (name != null) {
      this.catalogName = name;
    }

    try {
      LOG.debug("Connecting to JDBC database {}", properties.get(CatalogProperties.URI));
      connection = DriverManager.getConnection(uri);
      Statement st = connection.createStatement();
      st.execute("alter session set ENABLE_ICEBERG_TABLES=true");
      st.close();

    } catch (SQLTimeoutException e) {
      throw new UncheckedSQLException(e, "Cannot initialize JDBC catalog: Query timed out");
    } catch (SQLTransientConnectionException | SQLNonTransientConnectionException e) {
      throw new UncheckedSQLException(e, "Cannot initialize JDBC catalog: Connection failed");
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Cannot initialize JDBC catalog");
    }
  }

  @Override
  public void close() throws IOException {}

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {}

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    return null;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    return null;
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    return false;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    return false;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    return false;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return null;
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return null;
  }

  @Override
  public void setConf(Object conf) {}
}
