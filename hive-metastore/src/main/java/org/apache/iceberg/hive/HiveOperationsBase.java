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
import java.util.Map;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreOperations;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchIcebergViewException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** All the HMS operations like table,view,materialized_view should implement this. */
interface HiveOperationsBase {

  Logger LOG = LoggerFactory.getLogger(HiveOperationsBase.class);
  // The max size is based on HMS backend database. For Hive versions below 2.3, the max table
  // parameter size is 4000
  // characters, see https://issues.apache.org/jira/browse/HIVE-12274
  // set to 0 to not expose Iceberg metadata in HMS Table properties.
  String HIVE_TABLE_PROPERTY_MAX_SIZE = "iceberg.hive.table-property-max-size";
  long HIVE_TABLE_PROPERTY_MAX_SIZE_DEFAULT = 32672;
  String NO_LOCK_EXPECTED_KEY = "expected_parameter_key";
  String NO_LOCK_EXPECTED_VALUE = "expected_parameter_value";
  String ICEBERG_VIEW_TYPE_VALUE = "iceberg-view";

  enum ContentType {
    TABLE("Table"),
    VIEW("View");

    private final String value;

    ContentType(String value) {
      this.value = value;
    }

    public String value() {
      return value;
    }
  }

  TableType tableType();

  ClientPool<IMetaStoreClient, TException> metaClients();

  long maxHiveTablePropertySize();

  String database();

  String table();

  default Table loadHmsTable() throws TException, InterruptedException {
    try {
      return metaClients().run(client -> client.getTable(database(), table()));
    } catch (NoSuchObjectException nte) {
      LOG.trace("Table not found {}", database() + "." + table(), nte);
      return null;
    }
  }

  default Map<String, String> hmsEnvContext(String metadataLocation) {
    return metadataLocation == null
        ? ImmutableMap.of()
        : ImmutableMap.of(
            NO_LOCK_EXPECTED_KEY,
            BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
            NO_LOCK_EXPECTED_VALUE,
            metadataLocation);
  }

  default boolean exposeInHmsProperties() {
    return maxHiveTablePropertySize() > 0;
  }

  default void setSchema(Schema schema, Map<String, String> parameters) {
    parameters.remove(TableProperties.CURRENT_SCHEMA);
    if (exposeInHmsProperties() && schema != null) {
      String jsonSchema = SchemaParser.toJson(schema);
      setField(parameters, TableProperties.CURRENT_SCHEMA, jsonSchema);
    }
  }

  default void setField(Map<String, String> parameters, String key, String value) {
    if (value.length() <= maxHiveTablePropertySize()) {
      parameters.put(key, value);
    } else {
      LOG.warn(
          "Not exposing {} in HMS since it exceeds {} characters", key, maxHiveTablePropertySize());
    }
  }

  static void validateTableIsIceberg(Table table, String fullName) {
    String tableType = table.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP);
    NoSuchIcebergTableException.check(
        tableType != null
            && tableType.equalsIgnoreCase(BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE),
        "Not an iceberg table: %s (type=%s)",
        fullName,
        tableType);
  }

  static void validateTableIsIcebergView(Table table, String fullName) {
    String tableTypeProp = table.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP);
    NoSuchIcebergViewException.check(
        TableType.VIRTUAL_VIEW.name().equalsIgnoreCase(table.getTableType())
            && ICEBERG_VIEW_TYPE_VALUE.equalsIgnoreCase(tableTypeProp),
        "Not an iceberg view: %s (type=%s) (tableType=%s)",
        fullName,
        tableTypeProp,
        table.getTableType());
  }

  default void persistTable(Table hmsTable, boolean updateHiveTable, String metadataLocation)
      throws TException, InterruptedException {
    if (updateHiveTable) {
      metaClients()
          .run(
              client -> {
                MetastoreUtil.alterTable(
                    client, database(), table(), hmsTable, hmsEnvContext(metadataLocation));
                return null;
              });
    } else {
      metaClients()
          .run(
              client -> {
                client.createTable(hmsTable);
                return null;
              });
    }
  }

  static StorageDescriptor storageDescriptor(
      Schema schema, String location, boolean hiveEngineEnabled) {
    final StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(HiveSchemaUtil.convert(schema));
    storageDescriptor.setLocation(location);
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

  static void cleanupMetadata(FileIO io, String commitStatus, String metadataLocation) {
    try {
      if (commitStatus.equalsIgnoreCase("FAILURE")) {
        // If we are sure the commit failed, clean up the uncommitted metadata file
        io.deleteFile(metadataLocation);
      }
    } catch (RuntimeException e) {
      LOG.error("Failed to cleanup metadata file at {}", metadataLocation, e);
    }
  }

  static void cleanupMetadataAndUnlock(
      FileIO io,
      BaseMetastoreOperations.CommitStatus commitStatus,
      String metadataLocation,
      HiveLock lock) {
    try {
      cleanupMetadata(io, commitStatus.name(), metadataLocation);
    } finally {
      lock.unlock();
    }
  }

  default Table newHmsTable(String hmsTableOwner) {
    Preconditions.checkNotNull(hmsTableOwner, "'hmsOwner' parameter can't be null");
    final long currentTimeMillis = System.currentTimeMillis();

    Table newTable =
        new Table(
            table(),
            database(),
            hmsTableOwner,
            (int) currentTimeMillis / 1000,
            (int) currentTimeMillis / 1000,
            Integer.MAX_VALUE,
            null,
            Collections.emptyList(),
            Maps.newHashMap(),
            null,
            null,
            tableType().name());

    if (tableType().equals(TableType.EXTERNAL_TABLE)) {
      newTable
          .getParameters()
          .put("EXTERNAL", "TRUE"); // using the external table type also requires this
    }

    return newTable;
  }
}
