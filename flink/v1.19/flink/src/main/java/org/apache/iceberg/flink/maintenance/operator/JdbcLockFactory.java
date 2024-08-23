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
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcLockFactory implements TriggerLockFactory {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcLockFactory.class);

  @VisibleForTesting
  static final String INIT_LOCK_TABLES_PROPERTY = "flink-maintenance.lock.jdbc.init-lock-tables";

  private static final String LOCK_TABLE_NAME = "flink_maintenance_lock";
  private static final int LOCK_ID_MAX_LENGTH = 100;
  private static final String CREATE_LOCK_TABLE_SQL =
      String.format(
          "CREATE TABLE %s "
              + "(LOCK_TYPE CHAR(1) NOT NULL, "
              + "LOCK_ID VARCHAR(%s) NOT NULL, "
              + "INSTANCE_ID CHAR(36) NOT NULL, PRIMARY KEY (LOCK_TYPE, LOCK_ID))",
          LOCK_TABLE_NAME, LOCK_ID_MAX_LENGTH);

  private static final String CREATE_LOCK_SQL =
      String.format(
          "INSERT INTO %s (LOCK_TYPE, LOCK_ID, INSTANCE_ID) VALUES (?, ?, ?)", LOCK_TABLE_NAME);
  private static final String GET_LOCK_SQL =
      String.format("SELECT INSTANCE_ID FROM %s WHERE LOCK_TYPE=? AND LOCK_ID=?", LOCK_TABLE_NAME);
  private static final String DELETE_LOCK_SQL =
      String.format(
          "DELETE FROM %s WHERE LOCK_TYPE=? AND LOCK_ID=? AND INSTANCE_ID=?", LOCK_TABLE_NAME);

  private final String uri;
  private final String lockId;
  private final Map<String, String> properties;
  private transient JdbcClientPool pool;

  /**
   * Creates a new {@link TriggerLockFactory}. The lockId should be unique between the users of the
   * same uri.
   *
   * @param uri of the jdbc connection
   * @param lockId which should indentify the job and the table
   * @param properties used for creating the jdbc connection pool
   */
  public JdbcLockFactory(String uri, String lockId, Map<String, String> properties) {
    Preconditions.checkNotNull(uri, "JDBC connection URI is required");
    Preconditions.checkNotNull(properties, "Properties map is required");
    Preconditions.checkArgument(
        lockId.length() < LOCK_ID_MAX_LENGTH,
        "Invalid prefix length: lockId should be shorter than %s",
        LOCK_ID_MAX_LENGTH);
    this.uri = uri;
    this.lockId = lockId;
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
    return new JdbcLock(pool, lockId, Type.MAINTENANCE);
  }

  @Override
  public Lock createRecoveryLock() {
    return new JdbcLock(pool, lockId, Type.RECOVERY);
  }

  @Override
  public void close() throws IOException {
    pool.close();
  }

  private void initializeLockTables() {
    LOG.debug("Creating database tables (if missing) to store table maintenance locks");
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
              LOG.debug("Flink maintenance lock table already exists");
              return true;
            }

            LOG.info("Creating Flink maintenance lock table {}", LOCK_TABLE_NAME);
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

  private static class JdbcLock implements TriggerLockFactory.Lock {
    private final JdbcClientPool pool;
    private final String lockId;
    private final Type type;

    private JdbcLock(JdbcClientPool pool, String lockId, Type type) {
      this.pool = pool;
      this.lockId = lockId;
      this.type = type;
    }

    @Override
    public boolean tryLock() {
      if (isHeld()) {
        LOG.info("Lock is already held for {}", this);
        return false;
      }

      String newInstanceId = UUID.randomUUID().toString();
      try {
        return pool.run(
            conn -> {
              try (PreparedStatement sql = conn.prepareStatement(CREATE_LOCK_SQL)) {
                sql.setString(1, type.key);
                sql.setString(2, lockId);
                sql.setString(3, newInstanceId);
                int count = sql.executeUpdate();
                LOG.info(
                    "Created {} lock with instanceId {} with row count {}",
                    this,
                    newInstanceId,
                    count);
                return count == 1;
              }
            });
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new UncheckedInterruptedException(e, "Interrupted during tryLock");
      } catch (SQLException e) {
        // SQL exception happened when creating the lock. Check if the lock creation was
        // successful behind the scenes.
        if (newInstanceId.equals(instanceId())) {
          return true;
        } else {
          throw new UncheckedSQLException(e, "Failed to create %s lock", this);
        }
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
                sql.setString(2, lockId);
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
        throw new UncheckedSQLException(e, "Failed to check the state of the lock %s", this);
      }
    }

    @SuppressWarnings("checkstyle:NestedTryDepth")
    @Override
    public void unlock() {
      try {
        // Possible concurrency issue:
        // - `unlock` and `tryLock` happens at the same time when there is an existing lock
        //
        // Steps:
        // 1. `unlock` removes the lock in the database, but there is a temporary connection failure
        // 2. `lock` founds that there is no lock, so creates a new lock
        // 3. `unlock` retires the lock removal and removes the new lock
        //
        // To prevent the situation above we fetch the current lockId, and remove the lock
        // only with the given id.
        String instanceId = instanceId();

        if (instanceId != null) {
          pool.run(
              conn -> {
                try (PreparedStatement sql = conn.prepareStatement(DELETE_LOCK_SQL)) {
                  sql.setString(1, type.key);
                  sql.setString(2, lockId);
                  sql.setString(3, instanceId);
                  long count = sql.executeUpdate();
                  LOG.info(
                      "Deleted {} lock with instanceId {} with row count {}",
                      this,
                      instanceId,
                      count);
                } catch (SQLException e) {
                  // SQL exception happened when deleting lock information
                  throw new UncheckedSQLException(
                      e, "Failed to delete %s lock with instanceId %s", this, instanceId);
                }

                return null;
              });
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new UncheckedInterruptedException(e, "Interrupted during unlock");
      } catch (SQLException e) {
        // SQL exception happened when getting/updating lock information
        throw new UncheckedSQLException(e, "Failed to remove lock %s", this);
      }
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("type", type).add("lockId", lockId).toString();
    }

    @SuppressWarnings("checkstyle:NestedTryDepth")
    private String instanceId() {
      try {
        return pool.run(
            conn -> {
              try (PreparedStatement sql = conn.prepareStatement(GET_LOCK_SQL)) {
                sql.setString(1, type.key);
                sql.setString(2, lockId);
                try (ResultSet rs = sql.executeQuery()) {
                  if (rs.next()) {
                    return rs.getString(1);
                  } else {
                    return null;
                  }
                }
              } catch (SQLException e) {
                // SQL exception happened when getting lock information
                throw new UncheckedSQLException(e, "Failed to get lock information for %s", type);
              }
            });
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new UncheckedInterruptedException(e, "Interrupted during unlock");
      } catch (SQLException e) {
        throw new UncheckedSQLException(e, "Failed to get lock information for %s", type);
      }
    }
  }

  private enum Type {
    MAINTENANCE("m"),
    RECOVERY("r");

    private final String key;

    Type(String key) {
      this.key = key;
    }
  }
}
