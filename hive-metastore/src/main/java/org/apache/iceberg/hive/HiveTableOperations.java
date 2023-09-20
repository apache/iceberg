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
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.iceberg.BaseMetastoreOperations;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.encryption.StandardEncryptionManagerFactory;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.BiMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableBiMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.JsonUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO we should be able to extract some more commonalities to BaseMetastoreTableOperations to
 * avoid code duplication between this class and Metacat Tables.
 */
public class HiveTableOperations extends BaseMetastoreTableOperations
    implements HiveOperationsBase {
  private static final Logger LOG = LoggerFactory.getLogger(HiveTableOperations.class);

  private static final String HIVE_ICEBERG_METADATA_REFRESH_MAX_RETRIES =
      "iceberg.hive.metadata-refresh-max-retries";
  private static final int HIVE_ICEBERG_METADATA_REFRESH_MAX_RETRIES_DEFAULT = 2;
  private static final BiMap<String, String> ICEBERG_TO_HMS_TRANSLATION =
      ImmutableBiMap.of(
          // gc.enabled in Iceberg and external.table.purge in Hive are meant to do the same things
          // but with different names
          GC_ENABLED, "external.table.purge");

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

  private final String fullName;
  private final String catalogName;
  private final String database;
  private final String tableName;
  private final Configuration conf;
  private final long maxHiveTablePropertySize;
  private final int metadataRefreshMaxRetries;
  private final FileIO fileIO;
  private final StandardEncryptionManagerFactory encryptionManagerFactory;
  private final ClientPool<IMetaStoreClient, TException> metaClients;

  /** Tests only */
  protected HiveTableOperations(
      Configuration conf,
      ClientPool<IMetaStoreClient, TException> metaClients,
      FileIO fileIO,
      String catalogName,
      String database,
      String table) {
    this(conf, metaClients, fileIO, null, catalogName, database, table);
  }

  protected HiveTableOperations(
      Configuration conf,
      ClientPool metaClients,
      FileIO fileIO,
      StandardEncryptionManagerFactory encryptionManagerFactory,
      String catalogName,
      String database,
      String table) {
    this.conf = conf;
    this.metaClients = metaClients;
    this.fileIO = fileIO;
    this.encryptionManagerFactory = encryptionManagerFactory;
    this.fullName = catalogName + "." + database + "." + table;
    this.catalogName = catalogName;
    this.database = database;
    this.tableName = table;
    this.metadataRefreshMaxRetries =
        conf.getInt(
            HIVE_ICEBERG_METADATA_REFRESH_MAX_RETRIES,
            HIVE_ICEBERG_METADATA_REFRESH_MAX_RETRIES_DEFAULT);
    this.maxHiveTablePropertySize =
        conf.getLong(HIVE_TABLE_PROPERTY_MAX_SIZE, HIVE_TABLE_PROPERTY_MAX_SIZE_DEFAULT);
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
  public EncryptionManager encryption() {
    if (encryptionManagerFactory == null || current() == null || current().formatVersion() < 2) {
      return PlaintextEncryptionManager.instance();
    }

    return encryptionManagerFactory.create(current().properties());
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = null;
    try {
      Table table = metaClients.run(client -> client.getTable(database, tableName));
      HiveOperationsBase.validateTableIsIceberg(table, fullName);

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
    boolean newTable = base == null;
    String newMetadataLocation = writeNewMetadataIfRequired(newTable, metadata);
    boolean hiveEngineEnabled = hiveEngineEnabled(metadata, conf);
    boolean keepHiveStats = conf.getBoolean(ConfigProperties.KEEP_HIVE_STATS, false);

    BaseMetastoreOperations.CommitStatus commitStatus =
        BaseMetastoreOperations.CommitStatus.FAILURE;
    boolean updateHiveTable = false;

    HiveLock lock = lockObject(base);
    try {
      lock.lock();

      Table tbl = loadHmsTable();

      if (tbl != null) {
        // If we try to create the table but the metadata location is already set, then we had a
        // concurrent commit
        if (newTable
            && tbl.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)
                != null) {
          throw new AlreadyExistsException("Table already exists: %s.%s", database, tableName);
        }

        updateHiveTable = true;
        LOG.debug("Committing existing table: {}", fullName);
      } else {
        tbl =
            newHmsTable(
                metadata.property(HiveCatalog.HMS_TABLE_OWNER, HiveHadoopUtil.currentUser()));
        LOG.debug("Committing new table: {}", fullName);
      }

      tbl.setSd(
          HiveOperationsBase.storageDescriptor(
              metadata.schema(),
              metadata.location(),
              hiveEngineEnabled)); // set to pickup any schema changes

      String metadataLocation = tbl.getParameters().get(METADATA_LOCATION_PROP);
      String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
      if (!Objects.equals(baseMetadataLocation, metadataLocation)) {
        throw new CommitFailedException(
            "Cannot commit: Base metadata location '%s' is not same as the current table metadata location '%s' for %s.%s",
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
        tbl.getParameters().put(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
      }

      lock.ensureActive();

      try {
        persistTable(
            tbl, updateHiveTable, hiveLockEnabled(base, conf) ? null : baseMetadataLocation);
        lock.ensureActive();

        commitStatus = BaseMetastoreOperations.CommitStatus.SUCCESS;
      } catch (LockException le) {
        commitStatus = BaseMetastoreOperations.CommitStatus.UNKNOWN;
        throw new CommitStateUnknownException(
            "Failed to heartbeat for hive lock while "
                + "committing changes. This can lead to a concurrent commit attempt be able to overwrite this commit. "
                + "Please check the commit history. If you are running into this issue, try reducing "
                + "iceberg.hive.lock-heartbeat-interval-ms.",
            le);
      } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
        throw new AlreadyExistsException(e, "Table already exists: %s.%s", database, tableName);

      } catch (InvalidObjectException e) {
        throw new ValidationException(e, "Invalid Hive object for %s.%s", database, tableName);

      } catch (CommitFailedException | CommitStateUnknownException e) {
        throw e;

      } catch (Throwable e) {
        if (e.getMessage() != null
            && e.getMessage()
                .contains(
                    "The table has been modified. The parameter value for key '"
                        + HiveTableOperations.METADATA_LOCATION_PROP
                        + "' is")) {
          throw new CommitFailedException(
              e, "The table %s.%s has been modified concurrently", database, tableName);
        }

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
        commitStatus =
            BaseMetastoreOperations.CommitStatus.valueOf(
                checkCommitStatus(newMetadataLocation, metadata).name());
        switch (commitStatus) {
          case SUCCESS:
            break;
          case FAILURE:
            throw e;
          case UNKNOWN:
            throw new CommitStateUnknownException(e);
        }
      }
    } catch (TException e) {
      throw new RuntimeException(
          String.format("Metastore operation failed for %s.%s", database, tableName), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during commit", e);

    } catch (LockException e) {
      throw new CommitFailedException(e);

    } finally {
      HiveOperationsBase.cleanupMetadataAndUnlock(io(), commitStatus, newMetadataLocation, lock);
    }

    LOG.info(
        "Committed to table {} with the new metadata location {}", fullName, newMetadataLocation);
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
    setSchema(metadata.schema(), parameters);
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

  @Override
  public long maxHiveTablePropertySize() {
    return maxHiveTablePropertySize;
  }

  @Override
  public String database() {
    return database;
  }

  @Override
  public String table() {
    return tableName;
  }

  @Override
  public TableType tableType() {
    return TableType.EXTERNAL_TABLE;
  }

  @Override
  public ClientPool<IMetaStoreClient, TException> metaClients() {
    return metaClients;
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

  /**
   * Returns if the hive locking should be enabled on the table, or not.
   *
   * <p>The decision is made like this:
   *
   * <ol>
   *   <li>Table property value {@link TableProperties#HIVE_LOCK_ENABLED}
   *   <li>If the table property is not set then check the hive-site.xml property value {@link
   *       ConfigProperties#LOCK_HIVE_ENABLED}
   *   <li>If none of the above is enabled then use the default value {@link
   *       TableProperties#HIVE_LOCK_ENABLED_DEFAULT}
   * </ol>
   *
   * @param metadata Table metadata to use
   * @param conf The hive configuration to use
   * @return if the hive engine related values should be enabled or not
   */
  private static boolean hiveLockEnabled(TableMetadata metadata, Configuration conf) {
    if (metadata != null && metadata.properties().get(TableProperties.HIVE_LOCK_ENABLED) != null) {
      // We know that the property is set, so default value will not be used,
      return metadata.propertyAsBoolean(TableProperties.HIVE_LOCK_ENABLED, false);
    }

    return conf.getBoolean(
        ConfigProperties.LOCK_HIVE_ENABLED, TableProperties.HIVE_LOCK_ENABLED_DEFAULT);
  }

  @VisibleForTesting
  HiveLock lockObject(TableMetadata metadata) {
    if (hiveLockEnabled(metadata, conf)) {
      return new MetastoreLock(conf, metaClients, catalogName, database, tableName);
    } else {
      return new NoLock();
    }
  }
}
