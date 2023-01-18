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
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;

public class MetadataLogEntriesTable extends BaseMetadataTable {

  private static final Schema METADATA_LOG_ENTRIES_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "timestamp", Types.TimestampType.withZone()),
          Types.NestedField.required(2, "file", Types.StringType.get()),
          Types.NestedField.optional(3, "latest_snapshot_id", Types.LongType.get()),
          Types.NestedField.optional(4, "latest_schema_id", Types.IntegerType.get()),
          Types.NestedField.optional(5, "latest_sequence_number", Types.LongType.get()));

  MetadataLogEntriesTable(Table table) {
    this(table, table.name() + ".metadata_log_entries");
  }

  MetadataLogEntriesTable(Table table, String name) {
    super(table, name);
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.METADATA_LOG_ENTRIES;
  }

  @Override
  public TableScan newScan() {
    return new MetadataLogScan(table());
  }

  @Override
  public Schema schema() {
    return METADATA_LOG_ENTRIES_SCHEMA;
  }

  private DataTask task(TableScan scan) {
    TableMetadata current = table().operations().current();
    List<TableMetadata.MetadataLogEntry> metadataLogEntries =
        Lists.newArrayList(current.previousFiles().listIterator());
    metadataLogEntries.add(
        new TableMetadata.MetadataLogEntry(
            current.lastUpdatedMillis(), current.metadataFileLocation()));
    return StaticDataTask.of(
        table().io().newInputFile(current.metadataFileLocation()),
        schema(),
        scan.schema(),
        metadataLogEntries,
        metadataLogEntry ->
            MetadataLogEntriesTable.metadataLogEntryToRow(metadataLogEntry, table()));
  }

  private class MetadataLogScan extends StaticTableScan {
    MetadataLogScan(Table table) {
      super(
          table,
          METADATA_LOG_ENTRIES_SCHEMA,
          MetadataTableType.METADATA_LOG_ENTRIES,
          MetadataLogEntriesTable.this::task);
    }

    MetadataLogScan(Table table, TableScanContext context) {
      super(
          table,
          METADATA_LOG_ENTRIES_SCHEMA,
          MetadataTableType.METADATA_LOG_ENTRIES,
          MetadataLogEntriesTable.this::task,
          context);
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
      return new MetadataLogScan(table, context);
    }

    @Override
    public CloseableIterable<FileScanTask> planFiles() {
      return CloseableIterable.withNoopClose(MetadataLogEntriesTable.this.task(this));
    }
  }

  private static StaticDataTask.Row metadataLogEntryToRow(
      TableMetadata.MetadataLogEntry metadataLogEntry, Table table) {
    Long latestSnapshotId = null;
    Snapshot latestSnapshot = null;
    try {
      latestSnapshotId = SnapshotUtil.snapshotIdAsOfTime(table, metadataLogEntry.timestampMillis());
      latestSnapshot = table.snapshot(latestSnapshotId);
    } catch (IllegalArgumentException ignored) {
      // implies this metadata file was created at table creation
    }

    return StaticDataTask.Row.of(
        metadataLogEntry.timestampMillis() * 1000,
        metadataLogEntry.file(),
        // latest snapshot in this file corresponding to the log entry
        latestSnapshotId,
        latestSnapshot != null ? latestSnapshot.schemaId() : null,
        latestSnapshot != null ? latestSnapshot.sequenceNumber() : null);
  }
}
