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
package org.apache.iceberg.flink.maintenance.operator;

import java.io.IOException;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestEntry;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;

/**
 * Writes the new {@link ManifestFile}s based on the incoming manifest entries represented by the
 * {@link RowData} with the help of the {@link StreamingManifestWriter}s.
 */
@Internal
public class WriteManifests extends AbstractStreamOperator<ManifestFile>
    implements OneInputStreamOperator<RowData, ManifestFile> {

  static final String DATA_FILE = "data_file";
  static final String STATUS = ManifestEntry.STATUS.name();
  static final String SNAPSHOT_ID = ManifestEntry.SNAPSHOT_ID.name();
  static final String SEQUENCE_NUMBER = ManifestEntry.SEQUENCE_NUMBER.name();
  static final String FILE_SEQUENCE_NUMBER = ManifestEntry.FILE_SEQUENCE_NUMBER.name();

  private StreamingManifestWriter.Builder writerBuilder;
  private Map<String, Integer> positions;
  private final TableLoader tableLoader;
  private int dataFileFieldNum;
  private Long targetManifestSizeBytes;
  private final String taskName;
  private final int taskIndex;
  private final Integer rowsDivisor;

  private transient Map<ManifestContent, StreamingManifestWriter> writersMap;
  private transient ListState<RowData> pendingEntriesState;
  private transient Counter errorCounter;

  public WriteManifests(
      String taskName,
      int taskIndex,
      TableLoader tableLoader,
      Long targetManifestSizeBytes,
      Integer rowsDivisor) {
    this.taskName = taskName;
    this.taskIndex = taskIndex;
    this.tableLoader = tableLoader;
    this.targetManifestSizeBytes = targetManifestSizeBytes;
    this.rowsDivisor = rowsDivisor;
  }

  @Override
  public void processElement(StreamRecord<RowData> element) throws Exception {
    if (element.isRecord()) {
      RowData rowData = element.getValue();
      if (!isLive(rowData)) {
        // If not live, then skip
        return;
      }

      long timestamp = element.getTimestamp();
      try {
        ManifestContent manifestContent = content(rowData);
        StreamingManifestWriter writer = writerFor(manifestContent);
        ManifestFile newManifest = writer.write(rowData);
        if (newManifest != null) {
          output.collect(new StreamRecord<>(newManifest, timestamp));
        }
      } catch (Exception e) {
        LOG.error("Exception processing element {} at {}", rowData, element.getTimestamp(), e);
        output.collect(TaskResultAggregator.ERROR_STREAM, new StreamRecord<>(e, timestamp));
        errorCounter.inc();
      }
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    try {
      for (StreamingManifestWriter writer : writersMap.values()) {
        closeWriterAndCleanup(mark.getTimestamp(), writer);
      }
    } catch (Exception ex) {
      LOG.error("Exception processing watermark {}", mark, ex);
      output.collect(
          TaskResultAggregator.ERROR_STREAM, new StreamRecord<>(ex, mark.getTimestamp()));
      errorCounter.inc();
    }

    pendingEntriesState.clear();
    writersMap.clear();

    super.processWatermark(mark);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    pendingEntriesState.clear();
    for (Map.Entry<ManifestContent, StreamingManifestWriter> entry : writersMap.entrySet()) {
      pendingEntriesState.addAll(entry.getValue().pending());
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    tableLoader.open();
    Table table = tableLoader.loadTable();
    TableOperations ops = ((HasTableOperations) table).operations();
    Path metadataFilePath = new Path(ops.metadataFileLocation("file"));
    String outputLocation = metadataFilePath.getParent().toString();
    int formatVersion = ops.current().formatVersion();

    Table entriesTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.ENTRIES);
    RowType entriesTableType = FlinkSchemaUtil.convert(entriesTable.schema());
    RowType dataFileRowType =
        (RowType) entriesTableType.getTypeAt(entriesTableType.getFieldIndex(DATA_FILE));
    this.positions =
        ImmutableMap.of(
            DATA_FILE,
            entriesTableType.getFieldIndex(DATA_FILE),
            STATUS,
            entriesTableType.getFieldIndex(STATUS),
            SNAPSHOT_ID,
            entriesTableType.getFieldIndex(SNAPSHOT_ID),
            SEQUENCE_NUMBER,
            entriesTableType.getFieldIndex(SEQUENCE_NUMBER),
            FILE_SEQUENCE_NUMBER,
            entriesTableType.getFieldIndex(FILE_SEQUENCE_NUMBER),
            // This one is one level deeper, but key does not clash
            DataFile.CONTENT.name(),
            dataFileRowType.getFieldIndex(DataFile.CONTENT.name()));

    if (targetManifestSizeBytes == null) {
      targetManifestSizeBytes =
          PropertyUtil.propertyAsLong(
              table.properties(),
              TableProperties.MANIFEST_TARGET_SIZE_BYTES,
              TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
    }

    this.writersMap = Maps.newHashMap();
    this.dataFileFieldNum = dataFileRowType.getFieldCount();
    this.writerBuilder =
        new StreamingManifestWriter.Builder(
            table,
            outputLocation,
            formatVersion,
            entriesTableType,
            positions,
            targetManifestSizeBytes,
            rowsDivisor);

    this.errorCounter =
        TableMaintenanceMetrics.groupFor(getRuntimeContext(), table.name(), taskName, taskIndex)
            .counter(TableMaintenanceMetrics.ERROR_COUNTER);

    this.pendingEntriesState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "writeManifestPendingEntries", InternalTypeInfo.of(entriesTableType)));

    if (context.isRestored()) {
      pendingEntriesState.get().forEach(entry -> writerFor(content(entry)).write(entry));
    }
  }

  private void closeWriterAndCleanup(long timestamp, StreamingManifestWriter writer)
      throws IOException {
    ManifestFile manifestFile = writer.close();
    output.collect(new StreamRecord<>(manifestFile, timestamp));
  }

  /** See {@link org.apache.iceberg.ManifestEntry#isLive()} */
  private boolean isLive(RowData data) {
    return data.getInt(positions.get(STATUS)) < 2;
  }

  private ManifestContent content(RowData data) {
    return data.getRow(positions.get(DATA_FILE), dataFileFieldNum)
                .getInt(positions.get(DataFile.CONTENT.name()))
            < 1
        ? ManifestContent.DATA
        : ManifestContent.DELETES;
  }

  private StreamingManifestWriter writerFor(ManifestContent manifestContent) {
    return writersMap.computeIfAbsent(
        manifestContent, newKey -> writerBuilder.build(manifestContent));
  }
}
