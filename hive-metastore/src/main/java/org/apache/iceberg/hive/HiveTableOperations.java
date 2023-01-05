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

import static org.apache.iceberg.TableProperties.GC_ENABLED;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
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
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.BiMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableBiMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO we should be able to extract some more commonalities to BaseMetastoreTableOperations to
 * avoid code duplication between this class and Metacat Tables.
 */
public class HiveTableOperations extends BaseMetastoreTableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(HiveTableOperations.class);

  private static final String HIVE_ACQUIRE_LOCK_TIMEOUT_MS = "iceberg.hive.lock-timeout-ms";
  private static final String HIVE_LOCK_CHECK_MIN_WAIT_MS = "iceberg.hive.lock-check-min-wait-ms";
  private static final String HIVE_LOCK_CHECK_MAX_WAIT_MS = "iceberg.hive.lock-check-max-wait-ms";
  private static final String HIVE_LOCK_HEARTBEAT_INTERVAL_MS =
      "iceberg.hive.lock-heartbeat-interval-ms";
  private static final String HIVE_ICEBERG_METADATA_REFRESH_MAX_RETRIES =
      "iceberg.hive.metadata-refresh-max-retries";
  private static final String HIVE_TABLE_LEVEL_LOCK_EVICT_MS =
      "iceberg.hive.table-level-lock-evict-ms";

  // the max size is based on HMS backend database. For Hive versions below 2.3, the max table
  // parameter size is 4000
  // characters, see https://issues.apache.org/jira/browse/HIVE-12274
  // set to 0 to not expose Iceberg metadata in HMS Table properties.
  private static final String HIVE_TABLE_PROPERTY_MAX_SIZE = "iceberg.hive.table-property-max-size";
  private static final long HIVE_TABLE_PROPERTY_MAX_SIZE_DEFAULT = 32672;
  private static final long HIVE_ACQUIRE_LOCK_TIMEOUT_MS_DEFAULT = 3 * 60 * 1000; // 3 minutes
  private static final long HIVE_LOCK_CHECK_MIN_WAIT_MS_DEFAULT = 50; // 50 milliseconds
  private static final long HIVE_LOCK_CHECK_MAX_WAIT_MS_DEFAULT = 5 * 1000; // 5 seconds
  private static final long HIVE_LOCK_HEARTBEAT_INTERVAL_MS_DEFAULT = 4 * 60 * 1000; // 4 minutes
  private static final int HIVE_ICEBERG_METADATA_REFRESH_MAX_RETRIES_DEFAULT = 2;
  private static final long HIVE_TABLE_LEVEL_LOCK_EVICT_MS_DEFAULT = TimeUnit.MINUTES.toMillis(10);
  private static final BiMap<String, String> ICEBERG_TO_HMS_TRANSLATION =
      ImmutableBiMap.of(
          // gc.enabled in Iceberg and external.table.purge in Hive are meant to do the same things
          // but with different names
          GC_ENABLED, "external.table.purge");

  private static Cache<String, ReentrantLock> commitLockCache;

  private static synchronized void initTableLevelLockCache(long evictionTimeout) {
    if (commitLockCache == null) {
      commitLockCache =
          Caffeine.newBuilder().expireAfterAccess(evictionTimeout, TimeUnit.MILLISECONDS).build();
    }
  }

  /**
   * Provides key translation where necessary between Iceberg and HMS props. This translation is
   * needed because some properties control the same behaviour but are named differently in Iceberg
   * and Hive. Therefore changes to these property pairs should be synchronized.
   *
   * <p>Example: Deleting data files upon DROP TABLE is enabled using gc.enabled=true in Iceberg and
   * external.table.purge=true in Hive. Hive and Iceberg users are unaware of each other's control
   * flags, therefore inconsistent behaviour can occur from e.g. a Hive user's point of view if
   * external.table.purge=true is set on the HMS table but gc.enabled=false is set on the Iceberg
   * table, resulting in no data file deletion.
   *
   * @param hmsProp The HMS property that should be translated to Iceberg property
   * @return Iceberg property equivalent to the hmsProp. If no such translation exists, the original
   *     hmsProp is returned
   */
  public static String translateToIcebergProp(String hmsProp) {
    return ICEBERG_TO_HMS_TRANSLATION.inverse().getOrDefault(hmsProp, hmsProp);
  }

  private static class WaitingForLockException extends RuntimeException {
    WaitingForLockException(String message) {
      super(message);
    }
  }

  private final String fullName;
  private final String database;
  private final String tableName;
  private final Configuration conf;
  private final long lockAcquireTimeout;
  private final long lockCheckMinWaitTime;
  private final long lockCheckMaxWaitTime;
  private final long lockHeartbeatIntervalTime;
  private final long maxHiveTablePropertySize;
  private final int metadataRefreshMaxRetries;
  private final FileIO fileIO;
  private final ClientPool<IMetaStoreClient, TException> metaClients;
  private final ScheduledExecutorService exitingScheduledExecutorService;

  protected HiveTableOperations(
      Configuration conf,
      ClientPool metaClients,
      FileIO fileIO,
      String catalogName,
      String database,
      String table) {
    this.conf = conf;
    this.metaClients = metaClients;
    this.fileIO = fileIO;
    this.fullName = catalogName + "." + database + "." + table;
    this.database = database;
    this.tableName = table;
    this.lockAcquireTimeout =
        conf.getLong(HIVE_ACQUIRE_LOCK_TIMEOUT_MS, HIVE_ACQUIRE_LOCK_TIMEOUT_MS_DEFAULT);
    this.lockCheckMinWaitTime =
        conf.getLong(HIVE_LOCK_CHECK_MIN_WAIT_MS, HIVE_LOCK_CHECK_MIN_WAIT_MS_DEFAULT);
    this.lockCheckMaxWaitTime =
        conf.getLong(HIVE_LOCK_CHECK_MAX_WAIT_MS, HIVE_LOCK_CHECK_MAX_WAIT_MS_DEFAULT);
    this.lockHeartbeatIntervalTime =
        conf.getLong(HIVE_LOCK_HEARTBEAT_INTERVAL_MS, HIVE_LOCK_HEARTBEAT_INTERVAL_MS_DEFAULT);
    this.metadataRefreshMaxRetries =
        conf.getInt(
            HIVE_ICEBERG_METADATA_REFRESH_MAX_RETRIES,
            HIVE_ICEBERG_METADATA_REFRESH_MAX_RETRIES_DEFAULT);
    this.maxHiveTablePropertySize =
        conf.getLong(HIVE_TABLE_PROPERTY_MAX_SIZE, HIVE_TABLE_PROPERTY_MAX_SIZE_DEFAULT);
    long tableLevelLockCacheEvictionTimeout =
        conf.getLong(HIVE_TABLE_LEVEL_LOCK_EVICT_MS, HIVE_TABLE_LEVEL_LOCK_EVICT_MS_DEFAULT);
    this.exitingScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("iceberg-hive-lock-heartbeat-%d")
                .build());
    initTableLevelLockCache(tableLevelLockCacheEvictionTimeout);
  }

  @Override
  protected String tableName() {
    return fullName;
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = null;
    try {
      Table table = metaClients.run(client -> client.getTable(database, tableName));
      validateTableIsIceberg(table, fullName);

      metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);

    } catch (NoSuchObjectException e) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException("No such table: %s.%s", database, tableName);
      }

    } catch (TException e) {
      String errMsg =
          String.format("Failed to get table info from metastore %s.%s", database, tableName);
      throw new RuntimeException(errMsg, e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during refresh", e);
    }

    refreshFromMetadataLocation(metadataLocation, metadataRefreshMaxRetries);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    String newMetadataLocation =
        base == null && metadata.metadataFileLocation() != null
            ? metadata.metadataFileLocation()
            : writeNewMetadata(metadata, currentVersion() + 1);
    boolean hiveEngineEnabled = hiveEngineEnabled(metadata, conf);
    boolean keepHiveStats = conf.getBoolean(ConfigProperties.KEEP_HIVE_STATS, false);

    CommitStatus commitStatus = CommitStatus.FAILURE;
    boolean updateHiveTable = false;
    Optional<Long> lockId = Optional.empty();
    // getting a process-level lock per table to avoid concurrent commit attempts to the same table
    // from the same
    // JVM process, which would result in unnecessary and costly HMS lock acquisition requests
    ReentrantLock tableLevelMutex = commitLockCache.get(fullName, t -> new ReentrantLock(true));
    tableLevelMutex.lock();
    HiveLockHeartbeat hiveLockHeartbeat = null;
    try {
      lockId = Optional.of(acquireLock());
      hiveLockHeartbeat =
          new HiveLockHeartbeat(metaClients, lockId.get(), lockHeartbeatIntervalTime);
      hiveLockHeartbeat.schedule(exitingScheduledExecutorService);

      Table tbl = loadHmsTable();

      if (tbl != null) {
        // If we try to create the table but the metadata location is already set, then we had a
        // concurrent commit
        if (base == null
            && tbl.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)
                != null) {
          throw new AlreadyExistsException("Table already exists: %s.%s", database, tableName);
        }

        updateHiveTable = true;
        LOG.debug("Committing existing table: {}", fullName);
      } else {
        tbl = newHmsTable(metadata);
        LOG.debug("Committing new table: {}", fullName);
      }

      tbl.setSd(storageDescriptor(metadata, hiveEngineEnabled)); // set to pickup any schema changes

      String metadataLocation = tbl.getParameters().get(METADATA_LOCATION_PROP);
      String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
      if (!Objects.equals(baseMetadataLocation, metadataLocation)) {
        throw new CommitFailedException(
            "Base metadata location '%s' is not same as the current table metadata location '%s' for %s.%s",
            baseMetadataLocation, metadataLocation, database, tableName);
      }

      // get Iceberg props that have been removed
      Set<String> removedProps = Collections.emptySet();
      if (base != null) {
        removedProps =
            base.properties().keySet().stream()
                .filter(key -> !metadata.properties().containsKey(key))
                .collect(Collectors.toSet());
      }

      Map<String, String> summary =
          Optional.ofNullable(metadata.currentSnapshot())
              .map(Snapshot::summary)
              .orElseGet(ImmutableMap::of);
      setHmsTableParameters(
          newMetadataLocation, tbl, metadata, removedProps, hiveEngineEnabled, summary);

      if (!keepHiveStats) {
        tbl.getParameters().remove(StatsSetupConst.COLUMN_STATS_ACCURATE);
      }

      try {
        if (hiveLockHeartbeat.future.isCancelled()
            || hiveLockHeartbeat.encounteredException != null) {
          throw new CommitFailedException(
              "Failed to heartbeat for hive lock. %s",
              hiveLockHeartbeat.encounteredException.getMessage());
        }

        persistTable(tbl, updateHiveTable);
        if (hiveLockHeartbeat.future.isCancelled()
            || hiveLockHeartbeat.encounteredException != null) {
          throw new CommitStateUnknownException(
              "Failed to heartbeat for hive lock while "
                  + "committing changes. This can lead to a concurrent commit attempt be able to overwrite this commit. "
                  + "Please check the commit history. If you are running into this issue, try reducing "
                  + "iceberg.hive.lock-heartbeat-interval-ms.",
              hiveLockHeartbeat.encounteredException);
        }

        commitStatus = CommitStatus.SUCCESS;
      } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
        throw new AlreadyExistsException(e, "Table already exists: %s.%s", database, tableName);

      } catch (InvalidObjectException e) {
        throw new ValidationException(e, "Invalid Hive object for %s.%s", database, tableName);

      } catch (CommitFailedException | CommitStateUnknownException e) {
        throw e;

      } catch (Throwable e) {
        if (e.getMessage() != null
            && e.getMessage().contains("Table/View 'HIVE_LOCKS' does not exist")) {
          throw new RuntimeException(
              "Failed to acquire locks from metastore because the underlying metastore "
                  + "table 'HIVE_LOCKS' does not exist. This can occur when using an embedded metastore which does not "
                  + "support transactions. To fix this use an alternative metastore.",
              e);
        }

        LOG.error(
            "Cannot tell if commit to {}.{} succeeded, attempting to reconnect and check.",
            database,
            tableName,
            e);
        commitStatus = checkCommitStatus(newMetadataLocation, metadata);
        switch (commitStatus) {
          case SUCCESS:
            break;
          case FAILURE:
            throw e;
          case UNKNOWN:
            throw new CommitStateUnknownException(e);
        }
      }
    } catch (TException | UnknownHostException e) {
      throw new RuntimeException(
          String.format("Metastore operation failed for %s.%s", database, tableName), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during commit", e);

    } finally {
      if (hiveLockHeartbeat != null) {
        hiveLockHeartbeat.cancel();
      }

      cleanupMetadataAndUnlock(commitStatus, newMetadataLocation, lockId, tableLevelMutex);
    }

    LOG.info(
        "Committed to table {} with the new metadata location {}", fullName, newMetadataLocation);
  }

  @VisibleForTesting
  void persistTable(Table hmsTable, boolean updateHiveTable)
      throws TException, InterruptedException {
    if (updateHiveTable) {
      metaClients.run(
          client -> {
            MetastoreUtil.alterTable(client, database, tableName, hmsTable);
            return null;
          });
    } else {
      metaClients.run(
          client -> {
            client.createTable(hmsTable);
            return null;
          });
    }
  }

  @VisibleForTesting
  Table loadHmsTable() throws TException, InterruptedException {
    try {
      return metaClients.run(client -> client.getTable(database, tableName));
    } catch (NoSuchObjectException nte) {
      LOG.trace("Table not found {}", fullName, nte);
      return null;
    }
  }

  private Table newHmsTable(TableMetadata metadata) {
    Preconditions.checkNotNull(metadata, "'metadata' parameter can't be null");
    final long currentTimeMillis = System.currentTimeMillis();

    Table newTable =
        new Table(
            tableName,
            database,
            metadata.property(HiveCatalog.HMS_TABLE_OWNER, System.getProperty("user.name")),
            (int) currentTimeMillis / 1000,
            (int) currentTimeMillis / 1000,
            Integer.MAX_VALUE,
            null,
            Collections.emptyList(),
            Maps.newHashMap(),
            null,
            null,
            TableType.EXTERNAL_TABLE.toString());

    newTable
        .getParameters()
        .put("EXTERNAL", "TRUE"); // using the external table type also requires this
    return newTable;
  }

  private void setHmsTableParameters(
      String newMetadataLocation,
      Table tbl,
      TableMetadata metadata,
      Set<String> obsoleteProps,
      boolean hiveEngineEnabled,
      Map<String, String> summary) {
    Map<String, String> parameters =
        Optional.ofNullable(tbl.getParameters()).orElseGet(Maps::newHashMap);

    // push all Iceberg table properties into HMS
    metadata.properties().entrySet().stream()
        .filter(entry -> !entry.getKey().equalsIgnoreCase(HiveCatalog.HMS_TABLE_OWNER))
        .forEach(
            entry -> {
              String key = entry.getKey();
              // translate key names between Iceberg and HMS where needed
              String hmsKey = ICEBERG_TO_HMS_TRANSLATION.getOrDefault(key, key);
              parameters.put(hmsKey, entry.getValue());
            });
    if (metadata.uuid() != null) {
      parameters.put(TableProperties.UUID, metadata.uuid());
    }

    // remove any props from HMS that are no longer present in Iceberg table props
    obsoleteProps.forEach(parameters::remove);

    parameters.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH));
    parameters.put(METADATA_LOCATION_PROP, newMetadataLocation);

    if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
      parameters.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
    }

    // If needed set the 'storage_handler' property to enable query from Hive
    if (hiveEngineEnabled) {
      parameters.put(
          hive_metastoreConstants.META_TABLE_STORAGE,
          "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler");
    } else {
      parameters.remove(hive_metastoreConstants.META_TABLE_STORAGE);
    }

    // Set the basic statistics
    if (summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP) != null) {
      parameters.put(StatsSetupConst.NUM_FILES, summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    }
    if (summary.get(SnapshotSummary.TOTAL_RECORDS_PROP) != null) {
      parameters.put(StatsSetupConst.ROW_COUNT, summary.get(SnapshotSummary.TOTAL_RECORDS_PROP));
    }
    if (summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP) != null) {
      parameters.put(StatsSetupConst.TOTAL_SIZE, summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP));
    }

    setSnapshotStats(metadata, parameters);
    setSchema(metadata, parameters);
    setPartitionSpec(metadata, parameters);
    setSortOrder(metadata, parameters);

    tbl.setParameters(parameters);
  }

  @VisibleForTesting
  void setSnapshotStats(TableMetadata metadata, Map<String, String> parameters) {
    parameters.remove(TableProperties.CURRENT_SNAPSHOT_ID);
    parameters.remove(TableProperties.CURRENT_SNAPSHOT_TIMESTAMP);
    parameters.remove(TableProperties.CURRENT_SNAPSHOT_SUMMARY);

    Snapshot currentSnapshot = metadata.currentSnapshot();
    if (exposeInHmsProperties() && currentSnapshot != null) {
      parameters.put(
          TableProperties.CURRENT_SNAPSHOT_ID, String.valueOf(currentSnapshot.snapshotId()));
      parameters.put(
          TableProperties.CURRENT_SNAPSHOT_TIMESTAMP,
          String.valueOf(currentSnapshot.timestampMillis()));
      setSnapshotSummary(parameters, currentSnapshot);
    }

    parameters.put(TableProperties.SNAPSHOT_COUNT, String.valueOf(metadata.snapshots().size()));
  }

  @VisibleForTesting
  void setSnapshotSummary(Map<String, String> parameters, Snapshot currentSnapshot) {
    try {
      String summary = JsonUtil.mapper().writeValueAsString(currentSnapshot.summary());
      if (summary.length() <= maxHiveTablePropertySize) {
        parameters.put(TableProperties.CURRENT_SNAPSHOT_SUMMARY, summary);
      } else {
        LOG.warn(
            "Not exposing the current snapshot({}) summary in HMS since it exceeds {} characters",
            currentSnapshot.snapshotId(),
            maxHiveTablePropertySize);
      }
    } catch (JsonProcessingException e) {
      LOG.warn(
          "Failed to convert current snapshot({}) summary to a json string",
          currentSnapshot.snapshotId(),
          e);
    }
  }

  @VisibleForTesting
  void setSchema(TableMetadata metadata, Map<String, String> parameters) {
    parameters.remove(TableProperties.CURRENT_SCHEMA);
    if (exposeInHmsProperties() && metadata.schema() != null) {
      String schema = SchemaParser.toJson(metadata.schema());
      setField(parameters, TableProperties.CURRENT_SCHEMA, schema);
    }
  }

  @VisibleForTesting
  void setPartitionSpec(TableMetadata metadata, Map<String, String> parameters) {
    parameters.remove(TableProperties.DEFAULT_PARTITION_SPEC);
    if (exposeInHmsProperties() && metadata.spec() != null && metadata.spec().isPartitioned()) {
      String spec = PartitionSpecParser.toJson(metadata.spec());
      setField(parameters, TableProperties.DEFAULT_PARTITION_SPEC, spec);
    }
  }

  @VisibleForTesting
  void setSortOrder(TableMetadata metadata, Map<String, String> parameters) {
    parameters.remove(TableProperties.DEFAULT_SORT_ORDER);
    if (exposeInHmsProperties()
        && metadata.sortOrder() != null
        && metadata.sortOrder().isSorted()) {
      String sortOrder = SortOrderParser.toJson(metadata.sortOrder());
      setField(parameters, TableProperties.DEFAULT_SORT_ORDER, sortOrder);
    }
  }

  private void setField(Map<String, String> parameters, String key, String value) {
    if (value.length() <= maxHiveTablePropertySize) {
      parameters.put(key, value);
    } else {
      LOG.warn(
          "Not exposing {} in HMS since it exceeds {} characters", key, maxHiveTablePropertySize);
    }
  }

  private boolean exposeInHmsProperties() {
    return maxHiveTablePropertySize > 0;
  }

  private StorageDescriptor storageDescriptor(TableMetadata metadata, boolean hiveEngineEnabled) {

    final StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(HiveSchemaUtil.convert(metadata.schema()));
    storageDescriptor.setLocation(metadata.location());
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setParameters(Maps.newHashMap());
    if (hiveEngineEnabled) {
      storageDescriptor.setInputFormat("org.apache.iceberg.mr.hive.HiveIcebergInputFormat");
      storageDescriptor.setOutputFormat("org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");
      serDeInfo.setSerializationLib("org.apache.iceberg.mr.hive.HiveIcebergSerDe");
    } else {
      storageDescriptor.setOutputFormat("org.apache.hadoop.mapred.FileOutputFormat");
      storageDescriptor.setInputFormat("org.apache.hadoop.mapred.FileInputFormat");
      serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    }
    storageDescriptor.setSerdeInfo(serDeInfo);
    return storageDescriptor;
  }

  @SuppressWarnings("ReverseDnsLookup")
  @VisibleForTesting
  long acquireLock() throws UnknownHostException, TException, InterruptedException {
    final LockComponent lockComponent =
        new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, database);
    lockComponent.setTablename(tableName);
    final LockRequest lockRequest =
        new LockRequest(
            Lists.newArrayList(lockComponent),
            System.getProperty("user.name"),
            InetAddress.getLocalHost().getHostName());
    LockResponse lockResponse = metaClients.run(client -> client.lock(lockRequest));
    AtomicReference<LockState> state = new AtomicReference<>(lockResponse.getState());
    long lockId = lockResponse.getLockid();

    final long start = System.currentTimeMillis();
    long duration = 0;
    boolean timeout = false;

    try {
      if (state.get().equals(LockState.WAITING)) {
        // Retry count is the typical "upper bound of retries" for Tasks.run() function. In fact,
        // the maximum number of
        // attempts the Tasks.run() would try is `retries + 1`. Here, for checking locks, we use
        // timeout as the
        // upper bound of retries. So it is just reasonable to set a large retry count. However, if
        // we set
        // Integer.MAX_VALUE, the above logic of `retries + 1` would overflow into
        // Integer.MIN_VALUE. Hence,
        // the retry is set conservatively as `Integer.MAX_VALUE - 100` so it doesn't hit any
        // boundary issues.
        Tasks.foreach(lockId)
            .retry(Integer.MAX_VALUE - 100)
            .exponentialBackoff(lockCheckMinWaitTime, lockCheckMaxWaitTime, lockAcquireTimeout, 1.5)
            .throwFailureWhenFinished()
            .onlyRetryOn(WaitingForLockException.class)
            .run(
                id -> {
                  try {
                    LockResponse response = metaClients.run(client -> client.checkLock(id));
                    LockState newState = response.getState();
                    state.set(newState);
                    if (newState.equals(LockState.WAITING)) {
                      throw new WaitingForLockException(
                          String.format("Waiting for lock on table %s.%s", database, tableName));
                    }
                  } catch (InterruptedException e) {
                    Thread.interrupted(); // Clear the interrupt status flag
                    LOG.warn(
                        "Interrupted while waiting for lock on table {}.{}",
                        database,
                        tableName,
                        e);
                  }
                },
                TException.class);
      }
    } catch (WaitingForLockException waitingForLockException) {
      timeout = true;
      duration = System.currentTimeMillis() - start;
    } finally {
      if (!state.get().equals(LockState.ACQUIRED)) {
        unlock(Optional.of(lockId));
      }
    }

    // timeout and do not have lock acquired
    if (timeout && !state.get().equals(LockState.ACQUIRED)) {
      throw new CommitFailedException(
          "Timed out after %s ms waiting for lock on %s.%s", duration, database, tableName);
    }

    if (!state.get().equals(LockState.ACQUIRED)) {
      throw new CommitFailedException(
          "Could not acquire the lock on %s.%s, " + "lock request ended in state %s",
          database, tableName, state);
    }
    return lockId;
  }

  private void cleanupMetadataAndUnlock(
      CommitStatus commitStatus,
      String metadataLocation,
      Optional<Long> lockId,
      ReentrantLock tableLevelMutex) {
    try {
      if (commitStatus == CommitStatus.FAILURE) {
        // If we are sure the commit failed, clean up the uncommitted metadata file
        io().deleteFile(metadataLocation);
      }
    } catch (RuntimeException e) {
      LOG.error("Failed to cleanup metadata file at {}", metadataLocation, e);
    } finally {
      unlock(lockId);
      tableLevelMutex.unlock();
    }
  }

  private void unlock(Optional<Long> lockId) {
    if (lockId.isPresent()) {
      try {
        doUnlock(lockId.get());
      } catch (Exception e) {
        LOG.warn("Failed to unlock {}.{}", database, tableName, e);
      }
    }
  }

  @VisibleForTesting
  void doUnlock(long lockId) throws TException, InterruptedException {
    metaClients.run(
        client -> {
          client.unlock(lockId);
          return null;
        });
  }

  static void validateTableIsIceberg(Table table, String fullName) {
    String tableType = table.getParameters().get(TABLE_TYPE_PROP);
    NoSuchIcebergTableException.check(
        tableType != null && tableType.equalsIgnoreCase(ICEBERG_TABLE_TYPE_VALUE),
        "Not an iceberg table: %s (type=%s)",
        fullName,
        tableType);
  }

  /**
   * Returns if the hive engine related values should be enabled on the table, or not.
   *
   * <p>The decision is made like this:
   *
   * <ol>
   *   <li>Table property value {@link TableProperties#ENGINE_HIVE_ENABLED}
   *   <li>If the table property is not set then check the hive-site.xml property value {@link
   *       ConfigProperties#ENGINE_HIVE_ENABLED}
   *   <li>If none of the above is enabled then use the default value {@link
   *       TableProperties#ENGINE_HIVE_ENABLED_DEFAULT}
   * </ol>
   *
   * @param metadata Table metadata to use
   * @param conf The hive configuration to use
   * @return if the hive engine related values should be enabled or not
   */
  private static boolean hiveEngineEnabled(TableMetadata metadata, Configuration conf) {
    if (metadata.properties().get(TableProperties.ENGINE_HIVE_ENABLED) != null) {
      // We know that the property is set, so default value will not be used,
      return metadata.propertyAsBoolean(TableProperties.ENGINE_HIVE_ENABLED, false);
    }

    return conf.getBoolean(
        ConfigProperties.ENGINE_HIVE_ENABLED, TableProperties.ENGINE_HIVE_ENABLED_DEFAULT);
  }

  private static class HiveLockHeartbeat implements Runnable {
    private final ClientPool<IMetaStoreClient, TException> hmsClients;
    private final long lockId;
    private final long intervalMs;
    private ScheduledFuture<?> future;
    private volatile Exception encounteredException = null;

    HiveLockHeartbeat(
        ClientPool<IMetaStoreClient, TException> hmsClients, long lockId, long intervalMs) {
      this.hmsClients = hmsClients;
      this.lockId = lockId;
      this.intervalMs = intervalMs;
      this.future = null;
    }

    @Override
    public void run() {
      try {
        hmsClients.run(
            client -> {
              client.heartbeat(0, lockId);
              return null;
            });
      } catch (TException | InterruptedException e) {
        this.encounteredException = e;
        throw new CommitFailedException(e, "Failed to heartbeat for lock: %d", lockId);
      }
    }

    public void schedule(ScheduledExecutorService scheduler) {
      future =
          scheduler.scheduleAtFixedRate(this, intervalMs / 2, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void cancel() {
      if (future != null) {
        future.cancel(false);
      }
    }
  }
}
