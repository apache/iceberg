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

package org.apache.iceberg.hive;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.thrift.TException;


/**
 * A {@link HiveTableOperations} that does not override any existing Hive metadata.
 *
 * The behaviour of this class differs from {@link HiveTableOperations} in the following ways:
 * 1. Does not modify serde information of existing Hive table, this means that if Iceberg schema is updated
 *    Hive schema will remain stale
 * 2. If the Hive table already exists, no error is thrown. Instead Iceberg metadata is added to the table
 *
 * This behaviour is useful if the Iceberg metadata is being generated/updated in response to Hive metadata being
 * updated.
 */
public class HiveMetadataPreservingTableOperations extends HiveTableOperations {
  private final HiveClientPool metaClients;
  private final String database;
  private final String tableName;
  private static final DynMethods.UnboundMethod ALTER_TABLE = DynMethods.builder("alter_table")
      .impl(HiveMetaStoreClient.class, "alter_table_with_environmentContext",
          String.class, String.class, Table.class, EnvironmentContext.class)
      .impl(HiveMetaStoreClient.class, "alter_table",
          String.class, String.class, Table.class, EnvironmentContext.class)
      .build();

  protected HiveMetadataPreservingTableOperations(Configuration conf, HiveClientPool metaClients,
      String catalogName, String database, String table) {
    super(conf, metaClients, catalogName, database, table);
    this.metaClients = metaClients;
    this.database = database;
    this.tableName = table;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = null;
    try {
      final Table table = metaClients.run(client -> client.getTable(database, tableName));
      String tableType = table.getParameters().get(TABLE_TYPE_PROP);

      if (tableType == null || !tableType.equalsIgnoreCase(ICEBERG_TABLE_TYPE_VALUE)) {
        // [LINKEDIN] If table type is not Iceberg, that means there is no Iceberg metadata for the table yet.
        // So do not throw an error, instead just continue, currentMetadata will continue to remain null
        // which is what doRefresh would do if the table did not exist and further operations should work correctly

        // throw new IllegalArgumentException(String.format("Type of %s.%s is %s, not %s",
        //     database, tableName,
        //    tableType /* actual type */, ICEBERG_TABLE_TYPE_VALUE /* expected type */));
      } else {
        metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);
        if (metadataLocation == null) {
          String errMsg = String.format("%s.%s is missing %s property", database, tableName, METADATA_LOCATION_PROP);
          throw new IllegalArgumentException(errMsg);
        }
        if (!io().newInputFile(metadataLocation).exists()) {
          String errMsg = String.format("%s property for %s.%s points to a non-existent file %s",
              METADATA_LOCATION_PROP, database, tableName, metadataLocation);
          throw new IllegalArgumentException(errMsg);
        }
      }
    } catch (NoSuchObjectException e) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException("No such table: %s.%s", database, tableName);
      }

    } catch (TException e) {
      String errMsg = String.format("Failed to get table info from metastore %s.%s", database, tableName);
      throw new RuntimeException(errMsg, e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during refresh", e);
    }

    refreshFromMetadataLocation(metadataLocation);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

    boolean threw = true;
    Optional<Long> lockId = Optional.empty();
    try {
      lockId = Optional.of(acquireLock());
      // TODO add lock heart beating for cases where default lock timeout is too low.
      Table tbl;
      // [LINKEDIN] Instead of checking if base != null to check for table existence, we query metastore for existence
      // base can be null if not Iceberg metadata exists, but Hive table exists, so we want to get the current table
      // definition and not create a new definition
      boolean tableExists = metaClients.run(client -> client.tableExists(database, tableName));
      if (tableExists) {
        tbl = metaClients.run(client -> client.getTable(database, tableName));
      } else {
        final long currentTimeMillis = System.currentTimeMillis();
        tbl = new Table(tableName,
            database,
            System.getProperty("user.name"),
            (int) currentTimeMillis / 1000,
            (int) currentTimeMillis / 1000,
            Integer.MAX_VALUE,
            storageDescriptor(metadata, false),
            Collections.emptyList(),
            new HashMap<>(),
            null,
            null,
            TableType.EXTERNAL_TABLE.toString());
        tbl.getParameters().put("EXTERNAL", "TRUE"); // using the external table type also requires this
      }

      // [LINKEDIN] Do not touch the Hive schema of the table, just modify Iceberg specific properties
      // tbl.setSd(storageDescriptor(metadata)); // set to pickup any schema changes
      final String metadataLocation = tbl.getParameters().get(METADATA_LOCATION_PROP);
      String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
      if (!Objects.equals(baseMetadataLocation, metadataLocation)) {
        throw new CommitFailedException(
            "Base metadata location '%s' is not same as the current table metadata location '%s' for %s.%s",
            baseMetadataLocation, metadataLocation, database, tableName);
      }

      setParameters(newMetadataLocation, tbl, false);

      if (tableExists) {
        metaClients.run(client -> {
          EnvironmentContext envContext = new EnvironmentContext(
              ImmutableMap.of(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE)
          );
          ALTER_TABLE.invoke(client, database, tableName, tbl, envContext);
          return null;
        });
      } else {
        metaClients.run(client -> {
          client.createTable(tbl);
          return null;
        });
      }
      threw = false;
    } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
      throw new AlreadyExistsException("Table already exists: %s.%s", database, tableName);

    } catch (TException | UnknownHostException e) {
      if (e.getMessage().contains("Table/View 'HIVE_LOCKS' does not exist")) {
        throw new RuntimeException("Failed to acquire locks from metastore because 'HIVE_LOCKS' doesn't " +
            "exist, this probably happened when using embedded metastore or doesn't create a " +
            "transactional meta table. To fix this, use an alternative metastore", e);
      }

      throw new RuntimeException(String.format("Metastore operation failed for %s.%s", database, tableName), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during commit", e);

    } finally {
      if (threw) {
        // if anything went wrong, clean up the uncommitted metadata file
        io().deleteFile(newMetadataLocation);
      }
      unlock(lockId);
    }
  }
}
