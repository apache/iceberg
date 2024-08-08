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
import java.util.stream.Collectors;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public final class TableMetadataUtil {

  private TableMetadataUtil() {
    // Utility class
  }

  public static TableMetadata replacePaths(
      TableMetadata metadata, String sourcePrefix, String targetPrefix) {
    String newLocation = replacePathPrefix(metadata.location(), sourcePrefix, targetPrefix);
    List<Snapshot> newSnapshots =
        updateSnapshotsPath(metadata.snapshots(), sourcePrefix, targetPrefix);
    List<MetadataLogEntry> updatedMetadataLogEntries =
        updateMetadataLogsPath(metadata.previousFiles(), sourcePrefix, targetPrefix);
    long snapshotId =
        metadata.currentSnapshot() == null ? -1 : metadata.currentSnapshot().snapshotId();
    Map<String, String> updatedProperties =
        updatePropertiesPath(metadata.properties(), sourcePrefix, targetPrefix);

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
        updatedProperties,
        snapshotId,
        newSnapshots,
        () -> newSnapshots,
        metadata.snapshotLog(),
        updatedMetadataLogEntries,
        metadata.refs(),
        metadata.statisticsFiles(),
        metadata.partitionStatisticsFiles(),
        metadata.changes());
  }

  private static Map<String, String> updatePropertiesPath(
      Map<String, String> properties, String sourcePrefix, String targetPrefix) {
    Map<String, String> updatedProperties = Maps.newHashMapWithExpectedSize(properties.size());
    properties.forEach(
        (key, value) ->
            updatedProperties.put(key, replacePathPrefix(value, sourcePrefix, targetPrefix)));
    updateSpecificProperties(updatedProperties, sourcePrefix, targetPrefix);
    return updatedProperties;
  }

  private static void updateSpecificProperties(
      Map<String, String> properties, String sourcePrefix, String targetPrefix) {
    // Update specific property paths
    updatePropertyPath(properties, sourcePrefix, targetPrefix, TableProperties.OBJECT_STORE_PATH);
    updatePropertyPath(
        properties, sourcePrefix, targetPrefix, TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
    updatePropertyPath(properties, sourcePrefix, targetPrefix, TableProperties.WRITE_DATA_LOCATION);
    updatePropertyPath(
        properties, sourcePrefix, targetPrefix, TableProperties.WRITE_METADATA_LOCATION);
  }

  private static void updatePropertyPath(
      Map<String, String> properties,
      String sourcePrefix,
      String targetPrefix,
      String propertyName) {
    properties.computeIfPresent(
        propertyName, (key, value) -> replacePathPrefix(value, sourcePrefix, targetPrefix));
  }

  private static List<MetadataLogEntry> updateMetadataLogsPath(
      List<MetadataLogEntry> metadataLogs, String sourcePrefix, String targetPrefix) {
    return metadataLogs.stream()
        .map(
            entry ->
                new MetadataLogEntry(
                    entry.timestampMillis(),
                    replacePathPrefix(entry.file(), sourcePrefix, targetPrefix)))
        .collect(Collectors.toList());
  }

  private static List<Snapshot> updateSnapshotsPath(
      List<Snapshot> snapshots, String sourcePrefix, String targetPrefix) {
    return snapshots.stream()
        .map(
            snapshot ->
                new BaseSnapshot(
                    snapshot.sequenceNumber(),
                    snapshot.snapshotId(),
                    snapshot.parentId(),
                    snapshot.timestampMillis(),
                    snapshot.operation(),
                    snapshot.summary(),
                    snapshot.schemaId(),
                    replacePathPrefix(snapshot.manifestListLocation(), sourcePrefix, targetPrefix)))
        .collect(Collectors.toList());
  }

  private static String replacePathPrefix(
      String originalPath, String sourcePrefix, String targetPrefix) {
    return originalPath.replaceFirst(sourcePrefix, targetPrefix);
  }
}
