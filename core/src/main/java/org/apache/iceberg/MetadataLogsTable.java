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

public class MetadataLogsTable extends BaseMetadataTable {

  private static final Schema METADATA_LOGS_SCHEMA = new Schema(
      Types.NestedField.required(1, "timestamp_millis", Types.LongType.get()),
      Types.NestedField.required(2, "file", Types.StringType.get()),
      Types.NestedField.optional(3, "latest_snapshot_id", Types.LongType.get()),
      Types.NestedField.optional(4, "latest_schema_id", Types.IntegerType.get()),
      Types.NestedField.optional(5, "latest_sequence_number", Types.LongType.get())
  );

  MetadataLogsTable(TableOperations ops, Table table) {
    this(ops, table, table.name() + ".metadata_logs");
  }

  MetadataLogsTable(TableOperations ops, Table table, String name) {
    super(ops, table, name);
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.METADATA_LOGS;
  }

  @Override
  public TableScan newScan() {
    return new MetadataLogScan(operations(), table());
  }

  @Override
  public Schema schema() {
    return METADATA_LOGS_SCHEMA;
  }

  private DataTask task(TableScan scan) {
    TableOperations ops = operations();
    List<TableMetadata.MetadataLogEntry> metadataLogEntries =
        Lists.newArrayList(ops.current().previousFiles().listIterator());
    metadataLogEntries.add(new TableMetadata.MetadataLogEntry(ops.current().lastUpdatedMillis(),
        ops.current().metadataFileLocation()));
    return StaticDataTask.of(
        ops.io().newInputFile(ops.current().metadataFileLocation()),
        schema(),
        scan.schema(),
        metadataLogEntries,
        metadataLogEntry -> MetadataLogsTable.metadataLogToRow(metadataLogEntry, table())
    );
  }

  private class MetadataLogScan extends StaticTableScan {
    MetadataLogScan(TableOperations ops, Table table) {
      super(ops, table, METADATA_LOGS_SCHEMA, MetadataTableType.METADATA_LOGS, MetadataLogsTable.this::task);
    }

    MetadataLogScan(TableOperations ops, Table table, TableScanContext context) {
      super(ops, table, METADATA_LOGS_SCHEMA, MetadataTableType.METADATA_LOGS, MetadataLogsTable.this::task, context);
    }

    @Override
    protected TableScan newRefinedScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
      return new MetadataLogScan(ops, table, context);
    }

    @Override
    public CloseableIterable<FileScanTask> planFiles() {
      return CloseableIterable.withNoopClose(MetadataLogsTable.this.task(this));
    }
  }

  private static StaticDataTask.Row metadataLogToRow(TableMetadata.MetadataLogEntry metadataLogEntry, Table table) {
    Long latestSnapshotId = null;
    Snapshot latestSnapshot = null;
    try {
      latestSnapshotId = SnapshotUtil.snapshotIdAsOfTime(table, metadataLogEntry.timestampMillis());
      latestSnapshot = table.snapshot(latestSnapshotId);
    } catch (IllegalArgumentException ignored) {
      // implies this metadata file was created at table creation
    }

    return StaticDataTask.Row.of(
        metadataLogEntry.timestampMillis(),
        metadataLogEntry.file(),
        // latest snapshot in this file corresponding to the log entry
        latestSnapshotId,
        latestSnapshot != null ? latestSnapshot.schemaId() : null,
        latestSnapshot != null ? latestSnapshot.sequenceNumber() : null
    );
  }
}
