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
package org.apache.iceberg.flink.maintenance.operator;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.jdbc.UncheckedInterruptedException;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcLockFactory implements TriggerLockFactory {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcLockFactory.class);
  private static final String INIT_LOCK_TABLES_PROPERTY = "maintenance.lock.jdbc.init-lock-tables";
  private static final String LOCK_TABLE_NAME = "maintenance_lock";
  private static final int MAX_PREFIX_LENGTH = 100;
  private static final String CREATE_LOCK_TABLE_SQL =
      "CREATE TABLE "
          + LOCK_TABLE_NAME
          + "(LOCK_TYPE CHAR(1) NOT NULL, ISSUER VARCHAR("
          + MAX_PREFIX_LENGTH
          + ") NOT NULL, LOCK_ID CHAR(36) NOT NULL, PRIMARY KEY (LOCK_TYPE, ISSUER))";
  private static final String CREATE_LOCK_SQL =
      "INSERT INTO " + LOCK_TABLE_NAME + " (LOCK_TYPE, ISSUER, LOCK_ID) VALUES (?, ?, ?)";
  private static final String GET_LOCK_SQL =
      "SELECT LOCK_ID FROM " + LOCK_TABLE_NAME + " WHERE LOCK_TYPE=? AND ISSUER=?";
  private static final String DELETE_LOCK_SQL =
      "DELETE FROM " + LOCK_TABLE_NAME + " WHERE LOCK_TYPE=? AND ISSUER=? AND LOCK_ID=?";

  private final String uri;
  private final String issuer;
  private final Map<String, String> properties;
  private transient JdbcClientPool pool;

  public JdbcLockFactory(String uri, String issuer, Map<String, String> properties) {
    Preconditions.checkNotNull(uri, "JDBC connection URI is required");
    Preconditions.checkArgument(
        issuer.length() < MAX_PREFIX_LENGTH,
        "Invalid prefix length: issuer should be shorter than %s",
        MAX_PREFIX_LENGTH);
    this.uri = uri;
    this.issuer = issuer;
    this.properties = properties;
  }

  @Override
  public void open() {
    this.pool = new JdbcClientPool(1, uri, properties);

    if (PropertyUtil.propertyAsBoolean(properties, INIT_LOCK_TABLES_PROPERTY, false)) {
      initializeLockTables();
    }
  }

  /** Only used in testing to share the jdbc pool */
  @VisibleForTesting
  void open(JdbcLockFactory other) {
    this.pool = other.pool;
  }

  @Override
  public Lock createLock() {
    return new Lock(pool, issuer, Type.MAINTENANCE);
  }

  @Override
  public Lock createRecoveryLock() {
    return new Lock(pool, issuer, Type.RECOVERY);
  }

  @Override
  public void close() throws IOException {
    pool.close();
  }

  private void initializeLockTables() {
    LOG.trace("Creating database tables (if missing) to store table maintenance locks");
    try {
      pool.run(
          conn -> {
            DatabaseMetaData dbMeta = conn.getMetaData();
            ResultSet tableExists =
                dbMeta.getTables(
                    null /* catalog name */,
                    null /* schemaPattern */,
                    LOCK_TABLE_NAME /* tableNamePattern */,
                    null /* types */);
            if (tableExists.next()) {
              return true;
            }

            LOG.debug("Creating table {} to store iceberg catalog tables", LOCK_TABLE_NAME);
            return conn.prepareStatement(CREATE_LOCK_TABLE_SQL).execute();
          });

    } catch (SQLTimeoutException e) {
      throw new UncheckedSQLException(
          e, "Cannot initialize JDBC table maintenance lock: Query timed out");
    } catch (SQLTransientConnectionException | SQLNonTransientConnectionException e) {
      throw new UncheckedSQLException(
          e, "Cannot initialize JDBC table maintenance lock: Connection failed");
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Cannot initialize JDBC table maintenance lock");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted in call to initialize");
    }
  }

  public static class Lock implements TriggerLockFactory.Lock {
    private final JdbcClientPool pool;
    private final String issuer;
    private final Type type;

    public Lock(JdbcClientPool pool, String issuer, Type type) {
      this.pool = pool;
      this.issuer = issuer;
      this.type = type;
    }

    @Override
    public boolean tryLock() {
      if (isHeld()) {
        LOG.info("Lock is already held");
        return false;
      }

      try {
        return pool.run(
            conn -> {
              try (PreparedStatement sql = conn.prepareStatement(CREATE_LOCK_SQL)) {
                sql.setString(1, type.key);
                sql.setString(2, issuer);
                sql.setString(3, UUID.randomUUID().toString());
                return sql.executeUpdate() == 1;
              }
            });
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new UncheckedInterruptedException(e, "Interrupted during tryLock");
      } catch (SQLException e) {
        // SQL exception happened when creating the lock
        throw new UncheckedSQLException(e, "Failed to create %s lock", type);
      }
    }

    @SuppressWarnings("checkstyle:NestedTryDepth")
    @Override
    public boolean isHeld() {
      try {
        return pool.run(
            conn -> {
              try (PreparedStatement sql = conn.prepareStatement(GET_LOCK_SQL)) {
                sql.setString(1, type.key);
                sql.setString(2, issuer);
                try (ResultSet rs = sql.executeQuery()) {
                  return rs.next();
                }
              }
            });
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new UncheckedInterruptedException(e, "Interrupted during isHeld");
      } catch (SQLException e) {
        // SQL exception happened when getting lock information
        throw new UncheckedSQLException(e, "Failed to get lock information for %s", type);
      }
    }

    @SuppressWarnings("checkstyle:NestedTryDepth")
    @Override
    public void unlock() {
      try {
        pool.run(
            conn -> {
              String lockId;
              try (PreparedStatement sql = conn.prepareStatement(GET_LOCK_SQL)) {
                sql.setString(1, type.key);
                sql.setString(2, issuer);
                try (ResultSet rs = sql.executeQuery()) {
                  if (rs.next()) {
                    lockId = rs.getString(1);
                  } else {
                    return null;
                  }
                }
              }

              try (PreparedStatement sql = conn.prepareStatement(DELETE_LOCK_SQL)) {
                sql.setString(1, type.key);
                sql.setString(2, issuer);
                sql.setString(3, lockId);
                sql.executeUpdate();
              }

              return null;
            });
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new UncheckedInterruptedException(e, "Interrupted during unlock");
      } catch (SQLException e) {
        // SQL exception happened when getting/updating lock information
        throw new UncheckedSQLException(e, "Failed to get/update lock information for %s", type);
      }
    }
  }

  private enum Type {
    MAINTENANCE("m"),
    RECOVERY("r");

    private String key;

    Type(String key) {
      this.key = key;
    }
  }
}
