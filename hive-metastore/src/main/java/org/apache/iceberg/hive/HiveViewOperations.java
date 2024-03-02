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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchIcebergViewException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.BaseViewOperations;
import org.apache.iceberg.view.ViewMetadata;
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
    String dbName = viewIdentifier.namespace().level(0);
    this.conf = conf;
    this.catalogName = catalogName;
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
      HiveOperationsBase.validateTableIsIcebergView(table, fullName);

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
    String newMetadataLocation = writeNewMetadataIfRequired(metadata);
    String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;

    LOG.info(
        "Committed to view {} with the new metadata location {}", fullName, newMetadataLocation);
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
