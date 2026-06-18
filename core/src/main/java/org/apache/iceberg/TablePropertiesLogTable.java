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
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

/**
 * A {@link Table} implementation that exposes the table properties recorded in each historical
 * metadata file as rows: one row per metadata version, with the snapshot that was current in that
 * version and the full properties map at that point in time.
 *
 * <p>Each row is built by reading the corresponding {@code metadata.json}. The number of files read
 * is bounded by the metadata-log size ({@code write.metadata.previous-versions-max}).
 */
public class TablePropertiesLogTable extends BaseMetadataTable {

  private static final Schema TABLE_PROPERTIES_LOG_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "timestamp", Types.TimestampType.withZone()),
          Types.NestedField.optional(2, "file", Types.StringType.get()),
          Types.NestedField.optional(3, "latest_snapshot_id", Types.LongType.get()),
          Types.NestedField.optional(
              4,
              "properties",
              Types.MapType.ofRequired(5, 6, Types.StringType.get(), Types.StringType.get())));

  TablePropertiesLogTable(Table table) {
    this(table, table.name() + ".table_properties_log");
  }

  TablePropertiesLogTable(Table table, String name) {
    super(table, name);
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.TABLE_PROPERTIES_LOG;
  }

  @Override
  public TableScan newScan() {
    return new TablePropertiesLogScan(table());
  }

  @Override
  public Schema schema() {
    return TABLE_PROPERTIES_LOG_SCHEMA;
  }

  private DataTask task(TableScan scan) {
    TableMetadata current = table().operations().current();
    List<TableMetadata.MetadataLogEntry> metadataLogEntries =
        Lists.newArrayList(current.previousFiles());
    metadataLogEntries.add(
        new TableMetadata.MetadataLogEntry(
            current.lastUpdatedMillis(), current.metadataFileLocation()));
    FileIO io = table().io();
    return StaticDataTask.of(
        io.newInputFile(current.metadataFileLocation()),
        schema(),
        scan.schema(),
        metadataLogEntries,
        metadataLogEntry ->
            TablePropertiesLogTable.tablePropertiesLogEntryToRow(metadataLogEntry, io));
  }

  private class TablePropertiesLogScan extends StaticTableScan {
    TablePropertiesLogScan(Table table) {
      super(
          table,
          TABLE_PROPERTIES_LOG_SCHEMA,
          MetadataTableType.TABLE_PROPERTIES_LOG,
          TablePropertiesLogTable.this::task);
    }

    TablePropertiesLogScan(Table table, TableScanContext context) {
      super(
          table,
          TABLE_PROPERTIES_LOG_SCHEMA,
          MetadataTableType.TABLE_PROPERTIES_LOG,
          TablePropertiesLogTable.this::task,
          context);
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
      return new TablePropertiesLogScan(table, context);
    }

    @Override
    public CloseableIterable<FileScanTask> planFiles() {
      return CloseableIterable.withNoopClose(TablePropertiesLogTable.this.task(this));
    }
  }

  private static StaticDataTask.Row tablePropertiesLogEntryToRow(
      TableMetadata.MetadataLogEntry metadataLogEntry, FileIO io) {
    TableMetadata tableMetadata = TableMetadataParser.read(io, metadataLogEntry.file());
    Snapshot currentSnapshot = tableMetadata.currentSnapshot();
    return StaticDataTask.Row.of(
        metadataLogEntry.timestampMillis() * 1000,
        metadataLogEntry.file(),
        currentSnapshot != null ? currentSnapshot.snapshotId() : null,
        tableMetadata.properties());
  }
}
