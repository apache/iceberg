/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iceberg.hive;

import com.google.common.collect.Lists;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

/**
 * TODO we should be able to extract some more commonalities to BaseMetastoreTableOperations to
 * avoid code duplication between this class and Metacat Tables.
 */
public class HiveTableOperations extends BaseMetastoreTableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(HiveTableOperations.class);

  private final HiveClientPool metaClients;
  private final String database;
  private final String tableName;

  protected HiveTableOperations(Configuration conf, HiveClientPool metaClients, String database, String table) {
    super(conf);
    this.metaClients = metaClients;
    this.database = database;
    this.tableName = table;
  }

  @Override
  public TableMetadata refresh() {
    String metadataLocation = null;
    try {
      final Table table = metaClients.run(client -> client.getTable(database, tableName));
      String tableType = table.getParameters().get(TABLE_TYPE_PROP);

      if (tableType == null || !tableType.equalsIgnoreCase(ICEBERG_TABLE_TYPE_VALUE)) {
        throw new IllegalArgumentException(format("Invalid tableName, not Iceberg: %s.%s", database, table));
      }

      metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);
      if (metadataLocation == null) {
        throw new IllegalArgumentException(format("%s.%s is missing %s property", database, tableName, METADATA_LOCATION_PROP));
      }

    } catch (NoSuchObjectException e) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(format("No such table: %s.%s", database, tableName));
      }

    } catch (TException e) {
      throw new RuntimeException(format("Failed to get table info from metastore %s.%s", database, tableName), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during refresh", e);
    }

    refreshFromMetadataLocation(metadataLocation);

    return current();
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    // if the metadata is already out of date, reject it
    if (base != current()) {
      throw new CommitFailedException("Cannot commit: stale table metadata for %s.%s", database, tableName);
    }

    // if the metadata is not changed, return early
    if (base == metadata) {
      LOG.info("Nothing to commit.");
      return;
    }

    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

    boolean threw = true;
    Optional<Long> lockId = Optional.empty();
    try {
      lockId = Optional.of(acquireLock());
      // TODO add lock heart beating for cases where default lock timeout is too low.
      Table tbl;
      if (base != null) {
        tbl = metaClients.run(client -> client.getTable(database, tableName));
      } else {
        final long currentTimeMillis = System.currentTimeMillis();
        tbl = new Table(tableName,
                database,
                System.getProperty("user.name"),
                (int) currentTimeMillis / 1000,
                (int) currentTimeMillis / 1000,
                Integer.MAX_VALUE,
                storageDescriptor(metadata),
                Collections.emptyList(),
                new HashMap<>(),
                null,
                null,
                ICEBERG_TABLE_TYPE_VALUE);
      }

      tbl.setSd(storageDescriptor(metadata)); // set to pickup any schema changes
      final String metadataLocation = tbl.getParameters().get(METADATA_LOCATION_PROP);
      if (!Objects.equals(currentMetadataLocation(), metadataLocation)) {
        throw new CommitFailedException(format("metadataLocation = %s is not same as table metadataLocation %s for %s.%s",
                currentMetadataLocation(), metadataLocation, database, tableName));
      }

      setParameters(newMetadataLocation, tbl);

      if (base != null) {
        metaClients.run(client -> {
          client.alter_table(database, tableName, tbl);
          return null;
        });
      } else {
        metaClients.run(client -> {
          client.createTable(tbl);
          return null;
        });
      }
      threw = false;
    } catch (TException | UnknownHostException e) {
      throw new RuntimeException(format("Metastore operation failed for %s.%s", database, tableName), e);

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

    requestRefresh();
  }

  private void setParameters(String newMetadataLocation, Table tbl) {
    Map<String, String> parameters = tbl.getParameters();

    if (parameters == null) {
      parameters = new HashMap<>();
    }

    parameters.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH));
    parameters.put(METADATA_LOCATION_PROP, newMetadataLocation);

    if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
      parameters.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
    }

    tbl.setParameters(parameters);
  }

  private StorageDescriptor storageDescriptor(TableMetadata metadata) {

    final StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(columns(metadata.schema()));
    storageDescriptor.setLocation(metadata.location());
    storageDescriptor.setOutputFormat("org.apache.hadoop.mapred.FileInputFormat");
    storageDescriptor.setInputFormat("org.apache.hadoop.mapred.FileOutputFormat");
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    storageDescriptor.setSerdeInfo(serDeInfo);
    return storageDescriptor;
  }

  private final List<FieldSchema> columns(Schema schema) {
    return schema.columns().stream().map(col -> new FieldSchema(col.name(), HiveTypeConverter.convert(col.type()), "")).collect(Collectors.toList());
  }

  private long acquireLock() throws UnknownHostException, TException, InterruptedException {
    final LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, database);
    lockComponent.setTablename(tableName);
    final LockRequest lockRequest = new LockRequest(Lists.newArrayList(lockComponent),
            System.getProperty("user.name"),
            InetAddress.getLocalHost().getHostName());
    LockResponse lockResponse = metaClients.run(client -> client.lock(lockRequest));
    LockState state = lockResponse.getState();
    long lockId = lockResponse.getLockid();
    //TODO add timeout
    while (state.equals(LockState.WAITING)) {
      lockResponse = metaClients.run(client -> client.checkLock(lockId));
      state = lockResponse.getState();
    }

    if (!state.equals(LockState.ACQUIRED)) {
      throw new CommitFailedException(format("Could not acquire the lock on %s.%s, " +
              "lock request ended in state %s", database, tableName, state));
    }
    return lockId;
  }

  private void unlock(Optional<Long> lockId) {
    if (lockId.isPresent()) {
      try {
        metaClients.run(client -> {
          client.unlock(lockId.get());
          return null;
        });
      } catch (Exception e) {
        throw new RuntimeException(format("Failed to unlock %s.%s", database, tableName) , e);
      }
    }
  }
}
