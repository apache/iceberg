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
package org.apache.iceberg;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class TableMetadataUtil {
  private TableMetadataUtil() {}

  public static TableMetadata replacePaths(
      TableMetadata metadata, String sourcePrefix, String targetPrefix) {
    String newLocation = newPath(metadata.location(), sourcePrefix, targetPrefix);
    List<Snapshot> newSnapshots = updatePathInSnapshots(metadata, sourcePrefix, targetPrefix);
    List<MetadataLogEntry> metadataLogEntries =
        updatePathInMetadataLogs(metadata, sourcePrefix, targetPrefix);
    long snapshotId =
        metadata.currentSnapshot() == null ? -1 : metadata.currentSnapshot().snapshotId();
    Map<String, String> properties =
        updateProperties(metadata.properties(), sourcePrefix, targetPrefix);

    return new TableMetadata(
        null,
        metadata.formatVersion(),
        metadata.uuid(),
        newLocation,
        metadata.lastSequenceNumber(),
        metadata.lastUpdatedMillis(),
        metadata.lastColumnId(),
        metadata.currentSchemaId(),
        metadata.schemas(),
        metadata.defaultSpecId(),
        metadata.specs(),
        metadata.lastAssignedPartitionId(),
        metadata.defaultSortOrderId(),
        metadata.sortOrders(),
        properties,
        snapshotId,
        newSnapshots,
        () -> newSnapshots,
        metadata.snapshotLog(),
        metadataLogEntries,
        metadata.refs(),
        metadata.statisticsFiles(),
        metadata.partitionStatisticsFiles(),
        metadata.changes());
  }

  private static Map<String, String> updateProperties(
      Map<String, String> tableProperties, String sourcePrefix, String targetPrefix) {
    Map properties = Maps.newHashMap(tableProperties);
    updatePathInProperty(properties, sourcePrefix, targetPrefix, TableProperties.OBJECT_STORE_PATH);
    updatePathInProperty(
        properties, sourcePrefix, targetPrefix, TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
    updatePathInProperty(
        properties, sourcePrefix, targetPrefix, TableProperties.WRITE_DATA_LOCATION);
    updatePathInProperty(
        properties, sourcePrefix, targetPrefix, TableProperties.WRITE_METADATA_LOCATION);

    return properties;
  }

  private static void updatePathInProperty(
      Map<String, String> properties,
      String sourcePrefix,
      String targetPrefix,
      String propertyName) {
    if (properties.containsKey(propertyName)) {
      properties.put(
          propertyName, newPath(properties.get(propertyName), sourcePrefix, targetPrefix));
    }
  }

  private static List<MetadataLogEntry> updatePathInMetadataLogs(
      TableMetadata metadata, String sourcePrefix, String targetPrefix) {
    List<MetadataLogEntry> metadataLogEntries =
        Lists.newArrayListWithCapacity(metadata.previousFiles().size());
    for (MetadataLogEntry metadataLog : metadata.previousFiles()) {
      MetadataLogEntry newMetadataLog =
          new MetadataLogEntry(
              metadataLog.timestampMillis(),
              newPath(metadataLog.file(), sourcePrefix, targetPrefix));
      metadataLogEntries.add(newMetadataLog);
    }
    return metadataLogEntries;
  }

  private static List<Snapshot> updatePathInSnapshots(
      TableMetadata metadata, String sourcePrefix, String targetPrefix) {
    List<Snapshot> newSnapshots = Lists.newArrayListWithCapacity(metadata.snapshots().size());
    for (Snapshot snapshot : metadata.snapshots()) {
      String newManifestListLocation =
          newPath(snapshot.manifestListLocation(), sourcePrefix, targetPrefix);
      Snapshot newSnapshot =
          new BaseSnapshot(
              snapshot.sequenceNumber(),
              snapshot.snapshotId(),
              snapshot.parentId(),
              snapshot.timestampMillis(),
              snapshot.operation(),
              snapshot.summary(),
              snapshot.schemaId(),
              newManifestListLocation);
      newSnapshots.add(newSnapshot);
    }
    return newSnapshots;
  }

  private static String newPath(String path, String sourcePrefix, String targetPrefix) {
    return path.replaceFirst(sourcePrefix, targetPrefix);
  }
}
