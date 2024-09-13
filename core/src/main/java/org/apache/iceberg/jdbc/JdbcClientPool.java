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
import java.sql.SQLTransientException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ClientPoolImpl;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class JdbcClientPool extends ClientPoolImpl<Connection, SQLException> implements ConnectionClientPool {

  /**
   * The following are common retryable SQLSTATEs error codes which are generic across vendors.
   *
   * <ul>
   *   <li>08000: Generic Connection Exception
   *   <li>08003: Connection does not exist
   *   <li>08006: Connection failure
   *   <li>08007: Transaction resolution unknown
   *   <li>40001: Serialization failure due to deadlock
   * </ul>
   *
   * See https://en.wikipedia.org/wiki/SQLSTATE for more details.
   */
  static final Set<String> COMMON_RETRYABLE_CONNECTION_SQL_STATES =
      ImmutableSet.of("08000", "08003", "08006", "08007", "40001");

  private final String dbUrl;
  private final Map<String, String> properties;

  private final Set<String> retryableStatusCodes;

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
    super(poolSize, SQLTransientException.class, true);
    properties = props;
    retryableStatusCodes = Sets.newHashSet();
    retryableStatusCodes.addAll(COMMON_RETRYABLE_CONNECTION_SQL_STATES);
    String configuredRetryableStatuses = props.get(JdbcUtil.RETRYABLE_STATUS_CODES);
    if (configuredRetryableStatuses != null) {
      retryableStatusCodes.addAll(
          Arrays.stream(configuredRetryableStatuses.split(","))
              .map(status -> status.replaceAll("\\s+", ""))
              .collect(Collectors.toSet()));
    }

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

  @Override
  public boolean isConnectionException(Exception e) {
    return super.isConnectionException(e)
        || (e instanceof SQLException
            && retryableStatusCodes.contains(((SQLException) e).getSQLState()));
  }
}
