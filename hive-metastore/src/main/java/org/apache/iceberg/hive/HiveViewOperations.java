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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
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
import org.apache.iceberg.exceptions.NoSuchIcebergViewException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.MetastoreOperationsUtil;
import org.apache.iceberg.view.BaseViewOperations;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Hive implementation of Iceberg ViewOperations. */
final class HiveViewOperations extends BaseViewOperations implements HiveOperationsBase {
  private static final Logger LOG = LoggerFactory.getLogger(HiveViewOperations.class);

  private final String fullName;
  private final String database;
  private final String viewName;
  private final FileIO fileIO;
  private final ClientPool<IMetaStoreClient, TException> metaClients;
  private final long maxHiveTablePropertySize;

  HiveViewOperations(
      Configuration conf,
      ClientPool<IMetaStoreClient, TException> metaClients,
      FileIO fileIO,
      String catalogName,
      TableIdentifier viewIdentifier) {
    String dbName = viewIdentifier.namespace().level(0);
    this.metaClients = metaClients;
    this.fileIO = fileIO;
    this.fullName = CatalogUtil.fullTableName(catalogName, viewIdentifier);
    this.database = dbName;
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
      HiveViewOperations.validateTableIsIcebergView(table, fullName);

      metadataLocation =
          table.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);

    } catch (NoSuchObjectException | NoSuchIcebergViewException e) {
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

  @Override
  public void doCommit(ViewMetadata base, ViewMetadata metadata) {
    boolean newView = base == null;
    String newMetadataLocation = writeNewMetadataIfRequired(metadata);
    boolean updateHiveView = false;
    BaseMetastoreTableOperations.CommitStatus commitStatus =
        BaseMetastoreTableOperations.CommitStatus.FAILURE;

    try {
      Table view = null;

      try {
        view = metaClients.run(client -> client.getTable(database, viewName));
      } catch (NoSuchObjectException nte) {
        LOG.trace("View not found {}.{}", database, viewName, nte);
      }

      if (view != null) {
        if (!view.getTableType().equalsIgnoreCase(TableType.VIRTUAL_VIEW.name())) {
          throw new AlreadyExistsException(
              "Table with same name already exists: %s.%s", database, viewName);
        }

        // If we try to create the view but the metadata location is already set, then we had a
        // concurrent commit
        if (newView
            && view.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)
                != null) {
          throw new AlreadyExistsException("View already exists: %s.%s", database, viewName);
        }

        updateHiveView = true;
        LOG.debug("Committing existing view: {}", fullName);
      } else {
        view = newHmsTable(metadata.properties());
        LOG.debug("Committing new view: {}", fullName);
      }

      view.setSd(
          HiveOperationsBase.storageDescriptor(
              metadata.schema(), metadata.location(), false)); // set to pickup any schema changes

      String metadataLocation =
          view.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
      String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;

      if (!Objects.equals(baseMetadataLocation, metadataLocation)) {
        throw new CommitFailedException(
            "Base metadata location '%s' is not same as the current view metadata location '%s' for %s.%s",
            baseMetadataLocation, metadataLocation, database, viewName);
      }

      setHmsParameters(
          view,
          BaseMetastoreTableOperations.ICEBERG_VIEW_TYPE_VALUE.toUpperCase(Locale.ENGLISH),
          newMetadataLocation,
          metadata.schema(),
          metadata.uuid(),
          Collections.emptySet(),
          this::currentMetadataLocation);

      commitStatus =
          persistView(
              view,
              updateHiveView,
              baseMetadataLocation,
              newMetadataLocation,
              metadata.properties());

    } catch (TException e) {
      throw new RuntimeException(
          String.format("Metastore operation failed for %s.%s", database, viewName), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during commit", e);

    } finally {
      HiveOperationsBase.cleanupMetadata(fileIO, commitStatus.name(), newMetadataLocation);
    }

    LOG.info(
        "Committed to view {} with the new metadata location {}", fullName, newMetadataLocation);
  }

  private BaseMetastoreTableOperations.CommitStatus persistView(
      Table view,
      boolean updateHiveView,
      String baseMetadataLocation,
      String newMetadataLocation,
      Map<String, String> properties)
      throws TException, InterruptedException {
    BaseMetastoreTableOperations.CommitStatus commitStatus;

    try {
      persistTable(view, updateHiveView, baseMetadataLocation);
      commitStatus = BaseMetastoreTableOperations.CommitStatus.SUCCESS;
    } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
      throw new AlreadyExistsException(
          "%s already exists: %s.%s",
          view.getTableType().equalsIgnoreCase(TableType.VIRTUAL_VIEW.name()) ? "View" : "Table",
          view.getDbName(),
          view.getTableName());
    } catch (InvalidObjectException e) {
      throw new ValidationException(e, "Invalid Hive object for %s.%s", database, viewName);
    } catch (CommitFailedException | CommitStateUnknownException e) {
      throw e;
    } catch (Throwable e) {
      if (e.getMessage()
          .contains(
              "The view has been modified. The parameter value for key '"
                  + BaseMetastoreTableOperations.METADATA_LOCATION_PROP
                  + "' is")) {
        throw new CommitFailedException(
            e, "The view %s.%s has been modified concurrently", database, viewName);
      }

      LOG.error(
          "Cannot tell if commit to {}.{} succeeded, attempting to reconnect and check.",
          database,
          viewName,
          e);

      commitStatus =
          MetastoreOperationsUtil.checkCommitStatus(
              viewName(),
              newMetadataLocation,
              properties,
              this::calculateCommitStatusWithUpdatedLocation);

      switch (commitStatus) {
        case SUCCESS:
          break;
        case FAILURE:
          throw e;
        case UNKNOWN:
          throw new CommitStateUnknownException(e);
      }
    }
    return commitStatus;
  }

  private boolean calculateCommitStatusWithUpdatedLocation(String newMetadataLocation) {
    ViewMetadata metadata = refresh();
    String currentMetadataFileLocation = metadata.metadataFileLocation();
    return newMetadataLocation.equals(currentMetadataFileLocation);
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

  @Override
  protected String viewName() {
    return fullName;
  }

  static void validateTableIsIcebergView(Table table, String fullName) {
    String tableType = table.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP);
    NoSuchIcebergViewException.check(
        table.getTableType().equalsIgnoreCase(TableType.VIRTUAL_VIEW.name())
            && tableType != null
            && tableType.equalsIgnoreCase(BaseMetastoreTableOperations.ICEBERG_VIEW_TYPE_VALUE),
        "Not an iceberg view: %s (type=%s) (tableType=%s)",
        fullName,
        tableType,
        table.getTableType());
  }
}
