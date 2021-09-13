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
import java.util.UUID;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class TableMetadataUpdateBuilder {

  private InputFile file;
  private Integer formatVersion;
  private String uuid;
  private String location;
  private long lastSequenceNumber;
  private long lastUpdatedMillis;
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
  private List<Snapshot> snapshots;
  private List<HistoryEntry> snapshotLog;
  private List<TableMetadata.MetadataLogEntry> previousFiles;

  private final int baseFormatVersion;
  private final InputFile baseFile;
  private final long baseLastUpdateMillis;
  private final List<TableMetadata.MetadataLogEntry> basePreviousFiles;

  TableMetadataUpdateBuilder(TableMetadata base) {
    this.formatVersion = base.formatVersion();
    this.uuid = base.uuid();
    this.location = base.location();
    this.lastSequenceNumber = base.lastSequenceNumber();
    this.lastUpdatedMillis = base.lastUpdatedMillis();
    this.lastColumnId = base.lastColumnId();
    this.currentSchemaId = base.currentSchemaId();
    this.schemas = base.schemas();
    this.specs = base.specs();
    this.defaultSpecId = base.defaultSpecId();
    this.lastAssignedPartitionId = base.lastAssignedPartitionId();
    this.defaultSortOrderId = base.defaultSortOrderId();
    this.sortOrders = base.sortOrders();
    this.properties = base.properties();
    this.currentSnapshotId = base.currentSnapshotId();
    this.snapshots = base.snapshots();
    this.snapshotLog = base.snapshotLog();

    this.baseFormatVersion = base.formatVersion();
    this.baseFile = base.file();
    this.baseLastUpdateMillis = base.lastUpdatedMillis();
    this.basePreviousFiles = base.previousFiles();
  }

  public TableMetadataUpdateBuilder withFile(InputFile inputFile) {
    this.file = inputFile;
    return this;
  }

  public TableMetadataUpdateBuilder withLocation(String inputLocation) {
    this.location = inputLocation;
    return this;
  }

  public TableMetadataUpdateBuilder withFormatVersion(int inputFormatVersion) {
    this.formatVersion = inputFormatVersion;
    if (formatVersion != baseFormatVersion) {
      Preconditions.checkArgument(formatVersion <= TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION,
          "Cannot upgrade table to unsupported format version: v%s (supported: v%s)",
          formatVersion, TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION);
      Preconditions.checkArgument(formatVersion >= baseFormatVersion,
          "Cannot downgrade v%s table to v%s", baseFormatVersion, formatVersion);
    }
    return this;
  }

  public TableMetadataUpdateBuilder withUUID(String inputUuid) {
    this.uuid = inputUuid;
    return this;
  }

  public TableMetadataUpdateBuilder generateUUID() {
    return withUUID(UUID.randomUUID().toString());
  }

  public TableMetadataUpdateBuilder withLastSequenceNumber(long inputLastSequenceNumber) {
    this.lastSequenceNumber = inputLastSequenceNumber;
    return this;
  }

  public TableMetadataUpdateBuilder refreshLastUpdateMillis() {
    return withLastUpdatedMillis(System.currentTimeMillis());
  }

  public TableMetadataUpdateBuilder withLastUpdatedMillis(Long inputLastUpdatedMillis) {
    this.lastUpdatedMillis = inputLastUpdatedMillis;
    return this;
  }

  public TableMetadataUpdateBuilder withLastColumnId(int inputLastColumnId) {
    this.lastColumnId = inputLastColumnId;
    return this;
  }

  public TableMetadataUpdateBuilder withCurrentSchemaId(int inputCurrentSchemaId) {
    this.currentSchemaId = inputCurrentSchemaId;
    return this;
  }

  public TableMetadataUpdateBuilder withSchemas(List<Schema> inputSchemas) {
    this.schemas = inputSchemas;
    return this;
  }

  public TableMetadataUpdateBuilder withDefaultSpecId(int inputDefaultSpecId) {
    this.defaultSpecId = inputDefaultSpecId;
    return this;
  }

  public TableMetadataUpdateBuilder withSpecs(List<PartitionSpec> inputSpecs) {
    this.specs = inputSpecs;
    return this;
  }

  public TableMetadataUpdateBuilder withLastAssignedPartitionId(int inputLastAssignedPartitionId) {
    this.lastAssignedPartitionId = inputLastAssignedPartitionId;
    return this;
  }

  public TableMetadataUpdateBuilder withDefaultSortOrderId(int inputDefaultSortOrderId) {
    this.defaultSortOrderId = inputDefaultSortOrderId;
    return this;
  }

  public TableMetadataUpdateBuilder withSortOrders(List<SortOrder> inputSortOrders) {
    this.sortOrders = inputSortOrders;
    return this;
  }

  public TableMetadataUpdateBuilder withProperties(Map<String, String> inputProperties) {
    this.properties = inputProperties;
    return this;
  }

  public TableMetadataUpdateBuilder withCurrentSnapshotId(long inputCurrentSnapshotId) {
    this.currentSnapshotId = inputCurrentSnapshotId;
    return this;
  }

  public TableMetadataUpdateBuilder withSnapshots(List<Snapshot> inputSnapshots) {
    this.snapshots = inputSnapshots;
    return this;
  }

  public TableMetadataUpdateBuilder withSnapshotLog(List<HistoryEntry> inputSnapshotLog) {
    this.snapshotLog = inputSnapshotLog;
    return this;
  }

  public TableMetadataUpdateBuilder withPreviousFiles(List<TableMetadata.MetadataLogEntry> inputPreviousFiles) {
    this.previousFiles = inputPreviousFiles;
    return this;
  }

  public TableMetadata build() {
    if (previousFiles == null) {
      previousFiles = TableMetadata.addPreviousFile(baseFile, baseLastUpdateMillis, properties, basePreviousFiles);
    }

    return new TableMetadata(file, formatVersion, uuid, location, lastSequenceNumber, lastUpdatedMillis,
        lastColumnId, currentSchemaId, schemas, defaultSpecId, specs, lastAssignedPartitionId,
        defaultSortOrderId, sortOrders, properties, currentSnapshotId, snapshots, snapshotLog, previousFiles);
  }
}
