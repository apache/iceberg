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

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.TableMetadata.INITIAL_SEQUENCE_NUMBER;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SerializableSupplier;

public class MetadataTestUtils {

  private static final long EXAMPLE_SNAPSHOT_ID = 3;
  private static final long EXAMPLE_PARENT_ID = 1;
  private static final Integer EXAMPLE_SCHEMA_ID = 2;
  private static final long EXAMPLE_SEQUENCE_NUMBER = 0;
  private static final int EXAMPLE_SPEC_ID = 5;

  public static final String EXAMPLE_MANIFEST_PATH_1 = "file:/tmp/manifest1.avro";
  public static final String EXAMPLE_MANIFEST_PATH_2 = "file:/tmp/manifest2.avro";

  private MetadataTestUtils() {}

  public static TableMetadataBuilder buildTestTableMetadata(int formatVersion) {
    return new TableMetadataBuilder(formatVersion);
  }

  public static class TableMetadataBuilder {
    private String metadataLocation;
    private int formatVersion;
    private String uuid;
    private String location;
    private long lastSequenceNumber;
    private Long lastUpdatedMillis;
    private int lastColumnId;
    private int currentSchemaId;
    private List<Schema> schemas;
    private int defaultSpecId;
    private List<PartitionSpec> specs;
    private int lastAssignedPartitionId;
    private int defaultSortOrderId;
    private List<SortOrder> sortOrders;
    private Map<String, String> properties;
    private long currentSnapshotId;
    private List<HistoryEntry> snapshotLog;
    private List<TableMetadata.MetadataLogEntry> previousFiles;
    private List<StatisticsFile> statisticsFiles;
    private List<PartitionStatisticsFile> partitionStatisticsFiles;
    private List<MetadataUpdate> changes;
    private SerializableSupplier<List<Snapshot>> snapshotsSupplier;
    private List<Snapshot> snapshots;
    private Map<String, SnapshotRef> refs;

    private TableMetadataBuilder(int formatVersion) {
      this.formatVersion = formatVersion;
      this.uuid = UUID.randomUUID().toString();
      this.lastSequenceNumber = INITIAL_SEQUENCE_NUMBER;
      this.lastUpdatedMillis = System.currentTimeMillis();
      this.lastColumnId = -1;
      this.currentSchemaId = -1;
      this.schemas = Lists.newArrayList();
      this.defaultSpecId = -1;
      this.specs = Lists.newArrayList();
      this.lastAssignedPartitionId = 999;
      this.defaultSortOrderId = -1;
      this.sortOrders = Lists.newArrayList();
      this.properties = Maps.newHashMap();
      this.snapshots = Lists.newArrayList();
      this.currentSnapshotId = -1;
      this.changes = Lists.newArrayList();
      this.snapshotLog = Lists.newArrayList();
      this.previousFiles = Lists.newArrayList();
      this.refs = Maps.newHashMap();
      this.statisticsFiles = Lists.newArrayList();
      this.partitionStatisticsFiles = Lists.newArrayList();
    }

    public TableMetadataBuilder setMetadataLocation(String metadataFileLocation) {
      this.metadataLocation = metadataFileLocation;
      return this;
    }

    public TableMetadataBuilder setFormatVersion(int formatVersion) {
      this.formatVersion = formatVersion;
      return this;
    }

    public TableMetadataBuilder setUuid(String uuid) {
      this.uuid = uuid;
      return this;
    }

    public TableMetadataBuilder setLocation(String location) {
      this.location = location;
      return this;
    }

    public TableMetadataBuilder setLastSequenceNumber(long lastSequenceNumber) {
      this.lastSequenceNumber = lastSequenceNumber;
      return this;
    }

    public TableMetadataBuilder setLastUpdatedMillis(long lastUpdatedMillis) {
      this.lastUpdatedMillis = lastUpdatedMillis;
      return this;
    }

    public TableMetadataBuilder setLastColumnId(int lastColumnId) {
      this.lastColumnId = lastColumnId;
      return this;
    }

    public TableMetadataBuilder setCurrentSchemaId(int currentSchemaId) {
      this.currentSchemaId = currentSchemaId;
      return this;
    }

    public TableMetadataBuilder setSchemas(List<Schema> schemas) {
      this.schemas = schemas;
      return this;
    }

    public TableMetadataBuilder setDefaultSpecId(int defaultSpecId) {
      this.defaultSpecId = defaultSpecId;
      return this;
    }

    public TableMetadataBuilder setSpecs(List<PartitionSpec> specs) {
      this.specs = specs;
      return this;
    }

    public TableMetadataBuilder setLastAssignedPartitionId(int lastAssignedPartitionId) {
      this.lastAssignedPartitionId = lastAssignedPartitionId;
      return this;
    }

    public TableMetadataBuilder setDefaultSortOrderId(int defaultSortOrderId) {
      this.defaultSortOrderId = defaultSortOrderId;
      return this;
    }

    public TableMetadataBuilder setSortOrders(List<SortOrder> sortOrders) {
      this.sortOrders = sortOrders;
      return this;
    }

    public TableMetadataBuilder setProperties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    public TableMetadataBuilder setCurrentSnapshotId(long snapshotId) {
      this.currentSnapshotId = snapshotId;
      return this;
    }

    public TableMetadataBuilder setSnapshotsSupplier(
        SerializableSupplier<List<Snapshot>> snapshotsSupplier) {
      this.snapshotsSupplier = snapshotsSupplier;
      return this;
    }

    public TableMetadataBuilder setSnapshots(List<Snapshot> snapshots) {
      this.snapshots = snapshots;
      return this;
    }

    public TableMetadataBuilder setSnapshotLog(List<HistoryEntry> snapshotLog) {
      this.snapshotLog = snapshotLog;
      return this;
    }

    public TableMetadataBuilder setMetadataHistory(
        List<TableMetadata.MetadataLogEntry> metadataHistory) {
      this.previousFiles = metadataHistory;
      return this;
    }

    public TableMetadataBuilder setRefs(Map<String, SnapshotRef> refs) {
      this.refs = refs;
      return this;
    }

    public TableMetadataBuilder setChanges(List<MetadataUpdate> changes) {
      this.changes = changes;
      return this;
    }

    public TableMetadataBuilder setStatisticsFiles(List<StatisticsFile> statisticsFiles) {
      this.statisticsFiles = statisticsFiles;
      return this;
    }

    public TableMetadataBuilder setPartitionStatisticsFiles(
        List<PartitionStatisticsFile> partitionStatisticsFiles) {
      this.partitionStatisticsFiles = partitionStatisticsFiles;
      return this;
    }

    public TableMetadata build() {
      return new TableMetadata(
          metadataLocation,
          formatVersion,
          uuid,
          location,
          lastSequenceNumber,
          lastUpdatedMillis,
          lastColumnId,
          currentSchemaId,
          ImmutableList.copyOf(schemas),
          defaultSpecId,
          ImmutableList.copyOf(specs),
          lastAssignedPartitionId,
          defaultSortOrderId,
          ImmutableList.copyOf(sortOrders),
          ImmutableMap.copyOf(properties),
          currentSnapshotId,
          ImmutableList.copyOf(snapshots),
          snapshotsSupplier,
          ImmutableList.copyOf(snapshotLog),
          ImmutableList.copyOf(previousFiles),
          ImmutableMap.copyOf(refs),
          ImmutableList.copyOf(statisticsFiles),
          ImmutableList.copyOf(partitionStatisticsFiles),
          ImmutableList.copyOf(changes));
    }
  }

  public static BaseSnapshotBuilder buildTestSnapshot() {
    return new BaseSnapshotBuilder();
  }

  public static BaseSnapshotBuilder buildTestSnapshotWithExampleValues() {
    return new BaseSnapshotBuilder()
        .setSequenceNumber(EXAMPLE_SEQUENCE_NUMBER)
        .setSnapshotId(EXAMPLE_SNAPSHOT_ID)
        .setParentId(EXAMPLE_PARENT_ID)
        .setOperation(DataOperations.REPLACE)
        .setSummary(ImmutableMap.of("files-added", "4", "files-deleted", "100"))
        .setSchemaId(EXAMPLE_SCHEMA_ID)
        .setTimestampMillis(System.currentTimeMillis());
  }

  public static class BaseSnapshotBuilder {
    private long snapshotId;
    private Long parentId;
    private long sequenceNumber;
    private long timestampMillis;
    private String manifestListLocation;
    private String operation;
    private Map<String, String> summary;
    private Integer schemaId;
    private String[] v1ManifestLocations;

    private BaseSnapshotBuilder() {
      this.snapshotId = -1L;
      this.sequenceNumber = -1L;
      this.timestampMillis = System.currentTimeMillis();
      this.summary = ImmutableMap.of();
    }

    public BaseSnapshotBuilder setSnapshotId(long snapshotId) {
      this.snapshotId = snapshotId;
      return this;
    }

    public BaseSnapshotBuilder setParentId(Long parentId) {
      this.parentId = parentId;
      return this;
    }

    public BaseSnapshotBuilder setSequenceNumber(long sequenceNumber) {
      this.sequenceNumber = sequenceNumber;
      return this;
    }

    public BaseSnapshotBuilder setTimestampMillis(long timestampMillis) {
      this.timestampMillis = timestampMillis;
      return this;
    }

    public BaseSnapshotBuilder setManifestListLocation(String manifestListLocation) {
      this.manifestListLocation = manifestListLocation;
      return this;
    }

    public BaseSnapshotBuilder setOperation(String operation) {
      this.operation = operation;
      return this;
    }

    public BaseSnapshotBuilder setSummary(Map<String, String> summary) {
      this.summary = summary;
      return this;
    }

    public BaseSnapshotBuilder setSchemaId(Integer schemaId) {
      this.schemaId = schemaId;
      return this;
    }

    public BaseSnapshotBuilder setV1ManifestLocations(String[] v1ManifestLocations) {
      this.v1ManifestLocations = v1ManifestLocations;
      return this;
    }

    public Snapshot build() {
      Preconditions.checkArgument(
          manifestListLocation != null || v1ManifestLocations != null,
          "Cannot set both ManifestListLocation and V1ManifestLocations");
      if (v1ManifestLocations != null) {
        return new BaseSnapshot(
            sequenceNumber,
            snapshotId,
            parentId,
            timestampMillis,
            operation,
            summary,
            schemaId,
            v1ManifestLocations);
      }
      return new BaseSnapshot(
          sequenceNumber,
          snapshotId,
          parentId,
          timestampMillis,
          operation,
          summary,
          schemaId,
          manifestListLocation);
    }

    public Snapshot buildWithExampleManifestList(Path temp, List<String> manifestFiles)
        throws IOException {
      Preconditions.checkArgument(
          manifestListLocation == null && v1ManifestLocations == null,
          "An example manifest list with manifest files will be created");

      this.manifestListLocation =
          createManifestListWithManifestFiles(snapshotId, parentId, temp, manifestFiles);
      return build();
    }
  }

  private static String createManifestListWithManifestFiles(
      long snapshotId, Long parentSnapshotId, Path temp, List<String> manifestFiles)
      throws IOException {
    File manifestList = File.createTempFile("manifests", null, temp.toFile());
    manifestList.deleteOnExit();

    List<ManifestFile> manifests =
        manifestFiles.stream()
            .map(name -> new GenericManifestFile(localInput(name), EXAMPLE_SPEC_ID, snapshotId))
            .collect(Collectors.toList());

    try (ManifestListWriter writer =
        ManifestLists.write(1, Files.localOutput(manifestList), snapshotId, parentSnapshotId, 0)) {
      writer.addAll(manifests);
    }

    return localInput(manifestList).location();
  }
}
