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

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hive.iceberg.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.BiMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableBiMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.JsonUtil;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.GC_ENABLED;

public class IcebergTableConverter {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableConverter.class);

  private static final String HIVE_ICEBERG_METADATA_REFRESH_MAX_RETRIES = "iceberg.hive.metadata-refresh-max-retries";
  private static final int HIVE_ICEBERG_METADATA_REFRESH_MAX_RETRIES_DEFAULT = 2;
  public static final String TABLE_TYPE_PROP = "table_type";
  public static final String ICEBERG_TABLE_TYPE_VALUE = "iceberg";

  private static final BiMap<String, String> ICEBERG_TO_HMS_TRANSLATION = ImmutableBiMap.of(
      // gc.enabled in Iceberg and external.table.purge in Hive are meant to do the same things
      // but with different names
      GC_ENABLED, "external.table.purge", TableProperties.PARQUET_COMPRESSION, ParquetOutputFormat.COMPRESSION,
      TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, ParquetOutputFormat.BLOCK_SIZE);

  private final long maxHiveTablePropertySize;

  public IcebergTableConverter(long maxHiveTablePropertySize) {
    this.maxHiveTablePropertySize = maxHiveTablePropertySize;
  }

  public void setHmsTableParameters(String newMetadataLocation, Table tbl, TableMetadata metadata,
      Set<String> obsoleteProps, boolean hiveEngineEnabled) {
    Map<String, String> parameters = Optional.ofNullable(tbl.getParameters()).orElseGet(Maps::newHashMap);
    Map<String, String> summary = Optional.ofNullable(metadata.currentSnapshot()).map(Snapshot::summary)
        .orElseGet(ImmutableMap::of);
    // push all Iceberg table properties into HMS
    metadata.properties().entrySet().stream()
        .filter(entry -> !entry.getKey().equalsIgnoreCase(HiveCatalog.HMS_TABLE_OWNER)).forEach(entry -> {
          String key = entry.getKey();
          // translate key names between Iceberg and HMS where needed
          String hmsKey = ICEBERG_TO_HMS_TRANSLATION.getOrDefault(key, key);
          parameters.put(hmsKey, entry.getValue());
        });
    if (metadata.uuid() != null) {
      parameters.put(TableProperties.UUID, metadata.uuid());
    }

    // remove any props from HMS that are no longer present in Iceberg table props
    if (obsoleteProps != null) {
      obsoleteProps.forEach(parameters::remove);
    }
    parameters.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH));
    parameters.put(BaseMetastoreTableOperations.METADATA_LOCATION_PROP, newMetadataLocation);

//    if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
//      parameters.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
//    }

    setStorageHandler(parameters, hiveEngineEnabled);

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

  private void setStorageHandler(Map<String, String> parameters, boolean hiveEngineEnabled) {
    // If needed set the 'storage_handler' property to enable query from Hive
    if (hiveEngineEnabled) {
      parameters.put(hive_metastoreConstants.META_TABLE_STORAGE, HiveOperationsBase.HIVE_ICEBERG_STORAGE_HANDLER);
    } else {
      parameters.remove(hive_metastoreConstants.META_TABLE_STORAGE);
    }
  }

  @VisibleForTesting
  void setSnapshotStats(TableMetadata metadata, Map<String, String> parameters) {
    parameters.remove(TableProperties.CURRENT_SNAPSHOT_ID);
    parameters.remove(TableProperties.CURRENT_SNAPSHOT_TIMESTAMP);
    parameters.remove(TableProperties.CURRENT_SNAPSHOT_SUMMARY);

    Snapshot currentSnapshot = metadata.currentSnapshot();
    if (exposeInHmsProperties() && currentSnapshot != null) {
      parameters.put(TableProperties.CURRENT_SNAPSHOT_ID, String.valueOf(currentSnapshot.snapshotId()));
      parameters.put(TableProperties.CURRENT_SNAPSHOT_TIMESTAMP, String.valueOf(currentSnapshot.timestampMillis()));
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
        LOG.warn("Not exposing the current snapshot({}) summary in HMS since it exceeds {} characters",
            currentSnapshot.snapshotId(), maxHiveTablePropertySize);
      }
    } catch (JsonProcessingException e) {
      LOG.warn("Failed to convert current snapshot({}) summary to a json string", currentSnapshot.snapshotId(), e);
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
    if (exposeInHmsProperties() && metadata.sortOrder() != null && metadata.sortOrder().isSorted()) {
      String sortOrder = SortOrderParser.toJson(metadata.sortOrder());
      setField(parameters, TableProperties.DEFAULT_SORT_ORDER, sortOrder);
    }
  }

  private void setSchema(Schema schema, Map<String, String> parameters) {
    parameters.remove(TableProperties.CURRENT_SCHEMA);
    if (exposeInHmsProperties() && schema != null) {
      String jsonSchema = SchemaParser.toJson(schema);
      setField(parameters, TableProperties.CURRENT_SCHEMA, jsonSchema);
    }
  }

  private void setField(Map<String, String> parameters, String key, String value) {
    if (value.length() <= maxHiveTablePropertySize) {
      parameters.put(key, value);
    } else {
      LOG.warn("Not exposing {} in HMS since it exceeds {} characters", key, maxHiveTablePropertySize);
    }
  }

  boolean exposeInHmsProperties() {
    return maxHiveTablePropertySize > 0;
  }
}
