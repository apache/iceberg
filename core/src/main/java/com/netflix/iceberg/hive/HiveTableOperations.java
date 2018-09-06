package com.netflix.iceberg.hive;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.netflix.iceberg.BaseMetastoreTableOperations;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.TableMetadata;
import com.netflix.iceberg.exceptions.CommitFailedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SerdeType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.netflix.iceberg.hive.HiveTypeConverter.convert;

/**
 * TODO we should be able to extract some more commonalities to BaseMetastoreTableOperations to
 * avoid code duplication between this class and Metacat Tables.
 */
public class HiveTableOperations extends BaseMetastoreTableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(HiveTableOperations.class);

  private final IMetaStoreClient metaStoreClient;
  private final String catalog; // TODO I am not sure what is catalog in hive metastore context.
  private final String database;
  private final String table;

  protected HiveTableOperations(Configuration conf, IMetaStoreClient metaStoreClient, String catalog, String database, String table) {
    super(conf);
    this.metaStoreClient = metaStoreClient;
    this.catalog = catalog;
    this.database = database;
    this.table = table;
  }

  @Override
  public TableMetadata refresh() {
    try {
      final Table table = metaStoreClient.getTable(catalog, database, this.table);
      String tableType = table.getParameters().get(TABLE_TYPE_PROP);

      Preconditions.checkArgument(
              tableType != null && tableType.equalsIgnoreCase(ICEBERG_TABLE_TYPE_VALUE),
              "Invalid table, not Iceberg: %s.%s.%s", catalog, database, table);
      String metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);
      Preconditions.checkNotNull(metadataLocation,
              "Invalid table, missing metadata_location: %s.%s.%s", catalog, database, table);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    return current();
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    // if the metadata is already out of date, reject it
    if (base != current()) {
      throw new CommitFailedException("Cannot commit changes based on stale table metadata");
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
      lockId = acquireLock();
      // TODO add lock heart beating for cases where default lock timeout is too low.
      Table tbl;
      if (base != null) {
        tbl = metaStoreClient.getTable(catalog, database, this.table);
      } else {
        final long currentTimeMillis = System.currentTimeMillis();
        final StorageDescriptor storageDescriptor = getStorageDescriptor(metadata.schema());
        tbl = new Table(table,
                database,
                System.getProperty("user.name"),
                (int) currentTimeMillis / 1000,
                (int) currentTimeMillis / 1000,
                Integer.MAX_VALUE,
                storageDescriptor,
                Collections.emptyList(),
                new HashMap<>(),
                null,
                null,
                ICEBERG_TABLE_TYPE_VALUE);
      }

      Map<String, String> parameters = tbl.getParameters();
      if (parameters == null) {
        parameters = new HashMap<>();
      }
      parameters.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH));
      parameters.put(METADATA_LOCATION_PROP, metadata.location());
      if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
        parameters.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
      }

      // unnecessary but setting in case table.getParameters() starts returning a deep copy.
      tbl.setParameters(parameters);
      if (base != null) {
        metaStoreClient.alter_table(database, table, tbl);
      } else {
        metaStoreClient.createTable(tbl);
      }
      threw = false;
    } catch (TException | UnknownHostException e) {
      throw new RuntimeException(e);
    } finally {
      if (threw) {
        // if anything went wrong, clean up the uncommitted metadata file
        deleteFile(newMetadataLocation);
      }
      unlock(lockId);
    }

    refresh();
  }

  private StorageDescriptor getStorageDescriptor(Schema schema) {
    final StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(getColumns(schema));
    storageDescriptor.setLocation(hiveTableLocation());
    storageDescriptor.setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
    storageDescriptor.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerdeType(SerdeType.HIVE);
    serDeInfo.setSerializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
    return storageDescriptor;
  }

  private final List<FieldSchema> getColumns(Schema schema) {
    return schema.columns().stream().map(col -> new FieldSchema(col.name(), convert(col.type()), "")).collect(Collectors.toList());
  }

  private Optional<Long> acquireLock() throws UnknownHostException, TException {
    final LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, database);
    lockComponent.setTablename(this.table);
    final LockRequest lockRequest = new LockRequest(Lists.newArrayList(lockComponent), System.getProperty("user.name"), InetAddress.getLocalHost().getHostName());
    LockResponse lockResponse = metaStoreClient.lock(lockRequest);
    LockState state = lockResponse.getState();
    Optional<Long> lockId = Optional.of(lockResponse.getLockid());
    while (state.equals(LockState.WAITING)) {
      lockResponse = metaStoreClient.checkLock(lockResponse.getLockid());
      state = lockResponse.getState();
    }

    if (!state.equals(LockState.ACQUIRED)) {
      throw new IllegalStateException("Could not acquire the lock on table, lock request ended in state " + state);
    }
    return lockId;
  }

  private void unlock(Optional<Long> lockId) {
    if (lockId.isPresent()) {
      try {
        metaStoreClient.unlock(lockId.get());
      } catch (TException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
