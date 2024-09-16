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

import static java.util.Collections.emptySet;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.view.BaseViewOperations;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewRepresentation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Hive implementation of Iceberg {@link org.apache.iceberg.view.ViewOperations}. */
final class HiveViewOperations extends BaseViewOperations implements HiveOperationsBase {
  private static final Logger LOG = LoggerFactory.getLogger(HiveViewOperations.class);

  private final String fullName;
  private final String database;
  private final String viewName;
  private final FileIO fileIO;
  private final ClientPool<IMetaStoreClient, TException> metaClients;
  private final long maxHiveTablePropertySize;
  private final Configuration conf;
  private final String catalogName;

  HiveViewOperations(
      Configuration conf,
      ClientPool<IMetaStoreClient, TException> metaClients,
      FileIO fileIO,
      String catalogName,
      TableIdentifier viewIdentifier) {
    this.conf = conf;
    this.catalogName = catalogName;
    this.metaClients = metaClients;
    this.fileIO = fileIO;
    this.fullName = CatalogUtil.fullTableName(catalogName, viewIdentifier);
    this.database = viewIdentifier.namespace().level(0);
    this.viewName = viewIdentifier.name();
    this.maxHiveTablePropertySize =
        conf.getLong(HIVE_TABLE_PROPERTY_MAX_SIZE, HIVE_TABLE_PROPERTY_MAX_SIZE_DEFAULT);
  }

  @Override
  public void doRefresh() {
    String metadataLocation = null;
    Table table;

    try {
      table = metaClients.run(client -> client.getTable(database, viewName));
      HiveOperationsBase.validateTableIsIcebergView(table, fullName);

      metadataLocation =
          table.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);

    } catch (NoSuchObjectException e) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchViewException("View does not exist: %s.%s", database, viewName);
      }
    } catch (TException e) {
      String errMsg =
          String.format("Failed to get view info from metastore %s.%s", database, viewName);
      throw new RuntimeException(errMsg, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during refresh", e);
    }

    refreshFromMetadataLocation(metadataLocation);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  @Override
  public void doCommit(ViewMetadata base, ViewMetadata metadata) {
    boolean newView = base == null;
    String newMetadataLocation = writeNewMetadataIfRequired(metadata);
    boolean hiveEngineEnabled = false;

    CommitStatus commitStatus = CommitStatus.FAILURE;
    boolean updateHiveView = false;

    HiveLock lock = lockObject();
    try {
      lock.lock();

      Table tbl = loadHmsTable();

      if (tbl != null) {
        // If we try to create the view but the metadata location is already set, then we had a
        // concurrent commit
        if (newView
            && tbl.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)
                != null) {
          throw new AlreadyExistsException(
              "%s already exists: %s.%s",
              TableType.VIRTUAL_VIEW.name().equalsIgnoreCase(tbl.getTableType())
                  ? ContentType.VIEW.value()
                  : ContentType.TABLE.value(),
              database,
              viewName);
        }

        updateHiveView = true;
        LOG.debug("Committing existing view: {}", fullName);
      } else {
        tbl = newHMSView(metadata);
        LOG.debug("Committing new view: {}", fullName);
      }

      tbl.setSd(
          HiveOperationsBase.storageDescriptor(
              metadata.schema(),
              metadata.location(),
              hiveEngineEnabled)); // set to pick up any schema changes

      String metadataLocation =
          tbl.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
      String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
      if (!Objects.equals(baseMetadataLocation, metadataLocation)) {
        throw new CommitFailedException(
            "Cannot commit: Base metadata location '%s' is not same as the current view metadata location '%s' for %s.%s",
            baseMetadataLocation, metadataLocation, database, viewName);
      }

      // get Iceberg props that have been removed
      Set<String> removedProps = emptySet();
      if (base != null) {
        removedProps =
            base.properties().keySet().stream()
                .filter(key -> !metadata.properties().containsKey(key))
                .collect(Collectors.toSet());
      }

      setHmsTableParameters(newMetadataLocation, tbl, metadata, removedProps);

      lock.ensureActive();

      try {
        persistTable(tbl, updateHiveView, hiveLockEnabled(conf) ? null : baseMetadataLocation);
        lock.ensureActive();

        commitStatus = CommitStatus.SUCCESS;
      } catch (LockException le) {
        commitStatus = CommitStatus.UNKNOWN;
        throw new CommitStateUnknownException(
            "Failed to heartbeat for hive lock while "
                + "committing changes. This can lead to a concurrent commit attempt be able to overwrite this commit. "
                + "Please check the commit history. If you are running into this issue, try reducing "
                + "iceberg.hive.lock-heartbeat-interval-ms.",
            le);
      } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
        throw new AlreadyExistsException(e, "View already exists: %s.%s", database, viewName);

      } catch (InvalidObjectException e) {
        throw new ValidationException(e, "Invalid Hive object for %s.%s", database, viewName);

      } catch (CommitFailedException | CommitStateUnknownException e) {
        throw e;

      } catch (Throwable e) {
        if (e.getMessage() != null
            && e.getMessage()
                .contains(
                    "The table has been modified. The parameter value for key '"
                        + BaseMetastoreTableOperations.METADATA_LOCATION_PROP
                        + "' is")) {
          throw new CommitFailedException(
              e, "The view %s.%s has been modified concurrently", database, viewName);
        }

        if (e.getMessage() != null
            && e.getMessage().contains("Table/View 'HIVE_LOCKS' does not exist")) {
          throw new RuntimeException(
              "Failed to acquire locks from metastore because the underlying metastore "
                  + "view 'HIVE_LOCKS' does not exist. This can occur when using an embedded metastore which does not "
                  + "support transactions. To fix this use an alternative metastore.",
              e);
        }

        LOG.error(
            "Cannot tell if commit to {}.{} succeeded, attempting to reconnect and check.",
            database,
            viewName,
            e);
        commitStatus =
            checkCommitStatus(
                viewName,
                newMetadataLocation,
                metadata.properties(),
                () -> checkCurrentMetadataLocation(newMetadataLocation));
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
          String.format("Metastore operation failed for %s.%s", database, viewName), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during commit", e);

    } catch (LockException e) {
      throw new CommitFailedException(e);

    } finally {
      HiveOperationsBase.cleanupMetadataAndUnlock(io(), commitStatus, newMetadataLocation, lock);
    }

    LOG.info(
        "Committed to view {} with the new metadata location {}", fullName, newMetadataLocation);
  }

  /**
   * Validate if the new metadata location is the current metadata location.
   *
   * @param newMetadataLocation newly written metadata location
   * @return true if the new metadata location is the current metadata location
   */
  private boolean checkCurrentMetadataLocation(String newMetadataLocation) {
    ViewMetadata metadata = refresh();
    return newMetadataLocation.equals(metadata.metadataFileLocation());
  }

  private void setHmsTableParameters(
      String newMetadataLocation, Table tbl, ViewMetadata metadata, Set<String> obsoleteProps) {
    Map<String, String> parameters =
        Optional.ofNullable(tbl.getParameters()).orElseGet(Maps::newHashMap);

    // push all Iceberg view properties into HMS
    metadata.properties().entrySet().stream()
        .filter(entry -> !entry.getKey().equalsIgnoreCase(HiveCatalog.HMS_TABLE_OWNER))
        .forEach(entry -> parameters.put(entry.getKey(), entry.getValue()));
    if (metadata.uuid() != null) {
      parameters.put("uuid", metadata.uuid());
    }

    // remove any props from HMS that are no longer present in Iceberg view props
    obsoleteProps.forEach(parameters::remove);

    parameters.put(
        BaseMetastoreTableOperations.TABLE_TYPE_PROP,
        ICEBERG_VIEW_TYPE_VALUE.toUpperCase(Locale.ENGLISH));
    parameters.put(BaseMetastoreTableOperations.METADATA_LOCATION_PROP, newMetadataLocation);

    if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
      parameters.put(
          BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
    }

    setSchema(metadata.schema(), parameters);
    tbl.setParameters(parameters);
  }

  private static boolean hiveLockEnabled(Configuration conf) {
    return conf.getBoolean(ConfigProperties.LOCK_HIVE_ENABLED, true);
  }

  private Table newHMSView(ViewMetadata metadata) {
    final long currentTimeMillis = System.currentTimeMillis();
    String hmsTableOwner =
        PropertyUtil.propertyAsString(
            metadata.properties(), HiveCatalog.HMS_TABLE_OWNER, HiveHadoopUtil.currentUser());
    String sqlQuery = sqlFor(metadata);

    return new Table(
        table(),
        database(),
        hmsTableOwner,
        (int) currentTimeMillis / 1000,
        (int) currentTimeMillis / 1000,
        Integer.MAX_VALUE,
        null,
        Collections.emptyList(),
        Maps.newHashMap(),
        sqlQuery,
        sqlQuery,
        tableType().name());
  }

  private String sqlFor(ViewMetadata metadata) {
    SQLViewRepresentation closest = null;
    for (ViewRepresentation representation : metadata.currentVersion().representations()) {
      if (representation instanceof SQLViewRepresentation) {
        SQLViewRepresentation sqlViewRepresentation = (SQLViewRepresentation) representation;
        if (sqlViewRepresentation.dialect().equalsIgnoreCase("hive")) {
          return sqlViewRepresentation.sql();
        } else if (closest == null) {
          closest = sqlViewRepresentation;
        }
      }
    }

    return closest == null ? null : closest.sql();
  }

  @VisibleForTesting
  HiveLock lockObject() {
    if (hiveLockEnabled(conf)) {
      return new MetastoreLock(conf, metaClients, catalogName, database, viewName);
    } else {
      return new NoLock();
    }
  }

  @Override
  protected String viewName() {
    return fullName;
  }

  @Override
  public TableType tableType() {
    return TableType.VIRTUAL_VIEW;
  }

  @Override
  public ClientPool<IMetaStoreClient, TException> metaClients() {
    return metaClients;
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
    return viewName;
  }

  @Override
  public FileIO io() {
    return fileIO;
  }
}
