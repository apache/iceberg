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
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

public class MetadataLogEntriesTable extends BaseMetadataTable {

  private static final Types.NestedField LATEST_SNAPSHOT_ID =
      Types.NestedField.optional(3, "latest_snapshot_id", Types.LongType.get());

  private static final Types.NestedField LATEST_SCHEMA_ID =
      Types.NestedField.optional(4, "latest_schema_id", Types.IntegerType.get());

  private static final Types.NestedField LATEST_SEQUENCE_NUMBER =
      Types.NestedField.optional(5, "latest_sequence_number", Types.LongType.get());

  private static final Schema METADATA_LOG_ENTRIES_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "timestamp", Types.TimestampType.withZone()),
          Types.NestedField.required(2, "file", Types.StringType.get()),
          LATEST_SNAPSHOT_ID,
          LATEST_SCHEMA_ID,
          LATEST_SEQUENCE_NUMBER);

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

    Schema projectedSchema = scan.schema();
    boolean shouldLoadSnapshotDetails =
        projectedSchema.findField(LATEST_SNAPSHOT_ID.fieldId()) != null
            || projectedSchema.findField(LATEST_SCHEMA_ID.fieldId()) != null
            || projectedSchema.findField(LATEST_SEQUENCE_NUMBER.fieldId()) != null;

    return StaticDataTask.of(
        table().io().newInputFile(current.metadataFileLocation()),
        schema(),
        projectedSchema,
        metadataLogEntries,
        metadataLogEntry ->
            metadataLogEntryToRow(metadataLogEntry, current, shouldLoadSnapshotDetails));
  }

  private Snapshot latestSnapshotForEntry(
      TableMetadata.MetadataLogEntry metadataLogEntry, TableMetadata current) {
    // Resolve snapshot details from the metadata file represented by this entry because snapshots
    // may have been removed from the current table history after snapshot expiration.
    TableMetadata metadata =
        metadataLogEntry.file().equals(current.metadataFileLocation())
            ? current
            : TableMetadataParser.read(table().io(), metadataLogEntry.file());

    List<HistoryEntry> snapshotLog = metadata.snapshotLog();
    if (snapshotLog.isEmpty()) {
      // The initial table metadata does not contain a snapshot.
      return null;
    }

    HistoryEntry latestEntry = Iterables.getLast(snapshotLog);
    return metadata.snapshot(latestEntry.snapshotId());
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

  private StaticDataTask.Row metadataLogEntryToRow(
      TableMetadata.MetadataLogEntry metadataLogEntry,
      TableMetadata current,
      boolean shouldLoadSnapshotDetails) {
    Snapshot latestSnapshot =
        shouldLoadSnapshotDetails ? latestSnapshotForEntry(metadataLogEntry, current) : null;

    return StaticDataTask.Row.of(
        metadataLogEntry.timestampMillis() * 1000,
        metadataLogEntry.file(),
        latestSnapshot != null ? latestSnapshot.snapshotId() : null,
        latestSnapshot != null ? latestSnapshot.schemaId() : null,
        latestSnapshot != null ? latestSnapshot.sequenceNumber() : null);
  }
}
