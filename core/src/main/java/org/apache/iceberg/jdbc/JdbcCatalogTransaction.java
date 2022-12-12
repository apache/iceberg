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
import java.sql.SQLException;
import org.apache.iceberg.BaseCatalogTransaction;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.Tasks;

public class JdbcCatalogTransaction extends BaseCatalogTransaction {
  private final JdbcClientPool connections;

  public JdbcCatalogTransaction(
      BaseMetastoreCatalog origin, IsolationLevel isolationLevel, JdbcClientPool connections) {
    super(origin, isolationLevel);
    this.connections = connections;
    beginTransaction();
  }

  private void beginTransaction() {
    try {
      connections.run(
          conn -> {
            try (PreparedStatement sql = conn.prepareStatement(JdbcUtil.BEGIN_TX)) {
              return sql.execute();
            }
          });
    } catch (SQLException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void commitTransaction() {
    Preconditions.checkState(!hasCommitted(), "Transaction has already committed changes");

    try {
      for (TableIdentifier readTable : initiallyReadTableMetadata().keySet()) {
        // we need to check all read tables to determine whether they changed outside the catalog
        // TX after we initially read them
        if (IsolationLevel.SERIALIZABLE == isolationLevel()) {
          TableMetadata currentTableMetadata =
              ((BaseTable) origin().loadTable(readTable)).operations().current();

          if (!currentTableMetadata
              .metadataFileLocation()
              .equals(initiallyReadTableMetadata().get(readTable).metadataFileLocation())) {
            throw new ValidationException(
                "%s isolation violation: Found table metadata updates to table '%s' after it was read",
                isolationLevel(), readTable);
          }
        }
      }

      // FIXME: it seems that these are partially updated and not rolled back anymore
      Tasks.foreach(txByTable().values()).run(Transaction::commitTransaction);

      connections.run(
          conn -> {
            try (PreparedStatement sql = conn.prepareStatement(JdbcUtil.COMMIT_TX)) {
              return sql.execute();
            }
          });

      setHasCommitted(true);
    } catch (CommitStateUnknownException e) {
      throw e;
    } catch (RuntimeException e) {
      rollback();
      throw e;
    } catch (SQLException | InterruptedException e) {
      rollback();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void rollback() {
    try {
      super.rollback();
      connections.run(
          conn -> {
            try (PreparedStatement sql = conn.prepareStatement(JdbcUtil.ROLLBACK_TX)) {
              return sql.execute();
            }
          });
    } catch (SQLException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
