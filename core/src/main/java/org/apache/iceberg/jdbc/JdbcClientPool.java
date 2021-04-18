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
import java.util.Properties;
import org.apache.iceberg.ClientPool;

public class JdbcClientPool extends ClientPool<Connection, SQLException> {

  private final String dbUrl;
  private final Properties dbProperties;

  JdbcClientPool(String dbUrl, Properties props) {
    this((Integer) props.getOrDefault("iceberg.jdbc.client-pool-size", 5), dbUrl, props);
  }

  public JdbcClientPool(int poolSize, String dbUrl, Properties props) {
    super(poolSize, SQLException.class);
    dbProperties = props;
    this.dbUrl = dbUrl;
  }

  @Override
  protected Connection newClient() {
    try {
      return DriverManager.getConnection(dbUrl, dbProperties);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to connect to Jdbc Databse!", e);
    }
  }

  @Override
  protected Connection reconnect(Connection client) {
    this.close(client);
    return newClient();
  }

  @Override
  protected void close(Connection client) {
    try {
      client.close();
    } catch (SQLException e) {
      throw new RuntimeException("Failed to connect to Jdbc Databse!", e);
    }
  }
}
