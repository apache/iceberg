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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Map;
import java.util.Properties;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ClientPoolImpl;

public class JdbcClientPool extends ClientPoolImpl<Connection, SQLException> {

  private final String dbUrl;
  private final Map<String, String> properties;

  public JdbcClientPool(String dbUrl, Map<String, String> props) {
    this(
        Integer.parseInt(
            props.getOrDefault(
                CatalogProperties.CLIENT_POOL_SIZE,
                String.valueOf(CatalogProperties.CLIENT_POOL_SIZE_DEFAULT))),
        dbUrl,
        props);
  }

  public JdbcClientPool(int poolSize, String dbUrl, Map<String, String> props) {
    super(poolSize, SQLNonTransientConnectionException.class, true);
    properties = props;
    this.dbUrl = dbUrl;
  }

  @Override
  protected Connection newClient() {
    try {
      Properties dbProps = JdbcUtil.filterAndRemovePrefix(properties, JdbcCatalog.PROPERTY_PREFIX);
      return DriverManager.getConnection(dbUrl, dbProps);
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to connect: %s", dbUrl);
    }
  }

  @Override
  protected Connection reconnect(Connection client) {
    close(client);
    return newClient();
  }

  @Override
  protected void close(Connection client) {
    try {
      client.close();
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to close connection");
    }
  }
}
