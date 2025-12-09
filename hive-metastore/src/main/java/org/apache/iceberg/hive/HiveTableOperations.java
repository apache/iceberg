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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreOperations;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
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

  private final String fullName;
  private final String catalogName;
  private final String database;
  private final String tableName;
  private final Configuration conf;
  private final long maxHiveTablePropertySize;
  private final int metadataRefreshMaxRetries;
  private final FileIO fileIO;
  private final KeyManagementClient keyManagementClient;
  private final ClientPool<IMetaStoreClient, TException> metaClients;

  private EncryptionManager encryptionManager;
  private EncryptingFileIO encryptingFileIO;
  private String tableKeyId;
  private int encryptionDekLength;

  // keys loaded from the latest metadata
  private Optional<List<EncryptedKey>> encryptedKeysFromMetadata = Optional.empty();

  // keys added to EM (e.g. as a result of a FileAppend) but not committed into the latest metadata
  // yet
  private Optional<List<EncryptedKey>> encryptedKeysPending = Optional.empty();

  protected HiveTableOperations(
      Configuration conf,
      ClientPool<IMetaStoreClient, TException> metaClients,
      FileIO fileIO,
      KeyManagementClient keyManagementClient,
      String catalogName,
      String database,
      String table) {
    this.conf = conf;
    this.metaClients = metaClients;
    this.fileIO = fileIO;
    this.keyManagementClient = keyManagementClient;
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
    if (tableKeyId == null) {
      return fileIO;
    }

    if (encryptingFileIO == null) {
      encryptingFileIO = EncryptingFileIO.combine(fileIO, encryption());
    }

    return encryptingFileIO;
  }

  @Override
  public EncryptionManager encryption() {
    if (encryptionManager != null) {
      return encryptionManager;
    }

    if (tableKeyId != null) {
      if (keyManagementClient == null) {
        throw new RuntimeException(
            "Cant create encryption manager, because key management client is not set");
      }

      Map<String, String> encryptionProperties = Maps.newHashMap();
      encryptionProperties.put(TableProperties.ENCRYPTION_TABLE_KEY, tableKeyId);
      encryptionProperties.put(
          TableProperties.ENCRYPTION_DEK_LENGTH, String.valueOf(encryptionDekLength));

      List<EncryptedKey> keys = Lists.newLinkedList();
      encryptedKeysFromMetadata.ifPresent(keys::addAll);
      encryptedKeysPending.ifPresent(keys::addAll);

      encryptionManager =
          EncryptionUtil.createEncryptionManager(keys, encryptionProperties, keyManagementClient);
    } else {
      return PlaintextEncryptionManager.instance();
    }

    return encryptionManager;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = null;
    String tableKeyIdFromHMS = null;
    String dekLengthFromHMS = null;
    String metadataHashFromHMS = null;
    try {
      Table table = metaClients.run(client -> client.getTable(database, tableName));

      // Check if we are trying to load an Iceberg View as a Table
      HiveOperationsBase.validateIcebergViewNotLoadedAsIcebergTable(table, fullName);
      // Check if it is a valid Iceberg Table
      HiveOperationsBase.validateTableIsIceberg(table, fullName);

      metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);
      /* Table key ID must be retrieved from a catalog service, and not from untrusted storage
      (e.g. metadata json file) that can be tampered with. For example, an attacker can remove
      the table key parameter (along with existing snapshots) in the file, making the writers
      produce unencrypted files. Table key ID is taken directly from HMS catalog */
      tableKeyIdFromHMS = table.getParameters().get(TableProperties.ENCRYPTION_TABLE_KEY);
      dekLengthFromHMS = table.getParameters().get(TableProperties.ENCRYPTION_DEK_LENGTH);
      metadataHashFromHMS = table.getParameters().get(METADATA_HASH_PROP);
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

    if (tableKeyIdFromHMS != null) {
      checkIntegrityForEncryption(tableKeyIdFromHMS, dekLengthFromHMS, metadataHashFromHMS);

      tableKeyId = tableKeyIdFromHMS;
      encryptionDekLength =
          (dekLengthFromHMS != null)
              ? Integer.parseInt(dekLengthFromHMS)
              : TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT;

      encryptedKeysFromMetadata = Optional.ofNullable(current().encryptionKeys());

      if (encryptionManager != null) {
        encryptedKeysPending = Optional.of(Lists.newLinkedList());

        Set<String> keyIdsFromMetadata =
            encryptedKeysFromMetadata.orElseGet(Lists::newLinkedList).stream()
                .map(EncryptedKey::keyId)
                .collect(Collectors.toSet());

        for (EncryptedKey keyFromEM : EncryptionUtil.encryptionKeys(encryptionManager).values()) {
          if (!keyIdsFromMetadata.contains(keyFromEM.keyId())) {
            encryptedKeysPending.get().add(keyFromEM);
          }
        }

      } else {
        encryptedKeysPending = Optional.empty();
      }

      // Force re-creation of encryption manager with updated keys
      encryptingFileIO = null;
      encryptionManager = null;
    }
  }

  @SuppressWarnings({"checkstyle:CyclomaticComplexity", "MethodLength"})
  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    boolean newTable = base == null;
    final TableMetadata tableMetadata;
    encryptionPropsFromMetadata(metadata.properties());

    String newMetadataLocation;
    EncryptionManager encrManager = encryption();
    if (encrManager instanceof StandardEncryptionManager) {
      // Add new encryption keys to the metadata
      TableMetadata.Builder builder = TableMetadata.buildFrom(metadata);
      for (Map.Entry<String, EncryptedKey> entry :
          EncryptionUtil.encryptionKeys(encrManager).entrySet()) {
        builder.addEncryptionKey(entry.getValue());
      }

      tableMetadata = builder.build();
    } else {
      tableMetadata = metadata;
    }

    newMetadataLocation = writeNewMetadataIfRequired(newTable, tableMetadata);

    boolean hiveEngineEnabled = hiveEngineEnabled(tableMetadata, conf);
    boolean keepHiveStats = conf.getBoolean(ConfigProperties.KEEP_HIVE_STATS, false);

    BaseMetastoreOperations.CommitStatus commitStatus =
        BaseMetastoreOperations.CommitStatus.FAILURE;
    boolean updateHiveTable = false;

    HiveLock lock = lockObject(base != null ? base : tableMetadata);
    try {
      lock.lock();

      Table tbl = loadHmsTable();

      if (tbl != null) {
        // If we try to create the table but the metadata location is already set, then we had a
        // concurrent commit
        if (newTable
            && tbl.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)
                != null) {
          if (TableType.VIRTUAL_VIEW.name().equalsIgnoreCase(tbl.getTableType())) {
            throw new AlreadyExistsException(
                "View with same name already exists: %s.%s", database, tableName);
          }
          throw new AlreadyExistsException("Table already exists: %s.%s", database, tableName);
        }

        updateHiveTable = true;
        LOG.debug("Committing existing table: {}", fullName);
      } else {
        tbl =
            newHmsTable(
                tableMetadata.property(HiveCatalog.HMS_TABLE_OWNER, HiveHadoopUtil.currentUser()));
        LOG.debug("Committing new table: {}", fullName);
      }

      tbl.setSd(
          HiveOperationsBase.storageDescriptor(
              tableMetadata.schema(),
              tableMetadata.location(),
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
                .filter(key -> !tableMetadata.properties().containsKey(key))
                .collect(Collectors.toSet());
      }

      if (removedProps.contains(TableProperties.ENCRYPTION_TABLE_KEY)) {
        throw new IllegalArgumentException("Cannot remove key in encrypted table");
      }

      if (base != null
          && !Objects.equals(
              base.properties().get(TableProperties.ENCRYPTION_TABLE_KEY),
              metadata.properties().get(TableProperties.ENCRYPTION_TABLE_KEY))) {
        throw new IllegalArgumentException("Cannot modify key in encrypted table");
      }

      HMSTablePropertyHelper.updateHmsTableForIcebergTable(
          newMetadataLocation,
          tbl,
          tableMetadata,
          removedProps,
          hiveEngineEnabled,
          maxHiveTablePropertySize,
          currentMetadataLocation());

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
            && e.getMessage().contains("Table/View 'HIVE_LOCKS' does not exist")) {
          throw new RuntimeException(
              "Failed to acquire locks from metastore because the underlying metastore "
                  + "table 'HIVE_LOCKS' does not exist. This can occur when using an embedded metastore which does not "
                  + "support transactions. To fix this use an alternative metastore.",
              e);
        }

        commitStatus = BaseMetastoreOperations.CommitStatus.UNKNOWN;
        if (e.getMessage() != null
            && e.getMessage()
                .contains(
                    "The table has been modified. The parameter value for key '"
                        + HiveTableOperations.METADATA_LOCATION_PROP
                        + "' is")) {
          // It's possible the HMS client incorrectly retries a successful operation, due to network
          // issue for example, and triggers this exception. So we need double-check to make sure
          // this is really a concurrent modification. Hitting this exception means no pending
          // requests, if any, can succeed later, so it's safe to check status in strict mode
          commitStatus = checkCommitStatusStrict(newMetadataLocation, tableMetadata);
          if (commitStatus == BaseMetastoreOperations.CommitStatus.FAILURE) {
            throw new CommitFailedException(
                e, "The table %s.%s has been modified concurrently", database, tableName);
          }
        } else {
          LOG.error(
              "Cannot tell if commit to {}.{} succeeded, attempting to reconnect and check.",
              database,
              tableName,
              e);
          commitStatus = checkCommitStatus(newMetadataLocation, tableMetadata);
        }

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

  @Override
  public TableOperations temp(TableMetadata uncommittedMetadata) {
    return new TableOperations() {
      @Override
      public TableMetadata current() {
        return uncommittedMetadata;
      }

      @Override
      public TableMetadata refresh() {
        throw new UnsupportedOperationException(
            "Cannot call refresh on temporary table operations");
      }

      @Override
      public void commit(TableMetadata base, TableMetadata metadata) {
        throw new UnsupportedOperationException("Cannot call commit on temporary table operations");
      }

      @Override
      public String metadataFileLocation(String fileName) {
        return HiveTableOperations.this.metadataFileLocation(uncommittedMetadata, fileName);
      }

      @Override
      public LocationProvider locationProvider() {
        return LocationProviders.locationsFor(
            uncommittedMetadata.location(), uncommittedMetadata.properties());
      }

      @Override
      public FileIO io() {
        HiveTableOperations.this.encryptionPropsFromMetadata(uncommittedMetadata.properties());
        return HiveTableOperations.this.io();
      }

      @Override
      public EncryptionManager encryption() {
        return HiveTableOperations.this.encryption();
      }

      @Override
      public long newSnapshotId() {
        return HiveTableOperations.this.newSnapshotId();
      }
    };
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

  private void encryptionPropsFromMetadata(Map<String, String> tableProperties) {
    if (tableKeyId == null) {
      tableKeyId = tableProperties.get(TableProperties.ENCRYPTION_TABLE_KEY);
    }

    if (tableKeyId != null && encryptionDekLength <= 0) {
      encryptionDekLength =
          PropertyUtil.propertyAsInt(
              tableProperties,
              TableProperties.ENCRYPTION_DEK_LENGTH,
              TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT);
    }
  }

  private void checkIntegrityForEncryption(
      String encryptionKeyIdFromHMS, String dekLengthFromHMS, String metadataHashFromHMS) {
    TableMetadata metadata = current();
    if (StringUtils.isNotEmpty(metadataHashFromHMS)) {
      HMSTablePropertyHelper.verifyMetadataHash(metadata, metadataHashFromHMS);
      return;
    }

    LOG.warn(
        "Full metadata integrity check skipped because no metadata hash was recorded in HMS for table {}."
            + " Falling back to encryption property based check.",
        tableName);

    Map<String, String> propertiesFromMetadata = metadata.properties();

    String encryptionKeyIdFromMetadata =
        propertiesFromMetadata.get(TableProperties.ENCRYPTION_TABLE_KEY);
    if (!Objects.equals(encryptionKeyIdFromHMS, encryptionKeyIdFromMetadata)) {
      String errMsg =
          String.format(
              "Metadata file might have been modified. Encryption key id %s differs from HMS value %s",
              encryptionKeyIdFromMetadata, encryptionKeyIdFromHMS);
      throw new RuntimeException(errMsg);
    }

    String dekLengthFromMetadata =
        propertiesFromMetadata.get(TableProperties.ENCRYPTION_DEK_LENGTH);
    if (!Objects.equals(dekLengthFromHMS, dekLengthFromMetadata)) {
      String errMsg =
          String.format(
              "Metadata file might have been modified. DEK length %s differs from HMS value %s",
              dekLengthFromMetadata, dekLengthFromHMS);
      throw new RuntimeException(errMsg);
    }
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
