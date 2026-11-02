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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.reader.MetaDataReaderFunction;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitSerializer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reads the records from the metadata table splits. */
abstract class TableReader<R> extends ProcessFunction<MetadataTablePlanner.SplitInfo, R> {
  private static final Logger LOG = LoggerFactory.getLogger(TableReader.class);

  private final TableLoader tableLoader;
  private final String taskName;
  private final int taskIndex;
  private final Schema projectedSchema;
  private IcebergSourceSplitSerializer splitSerializer;
  private final ScanContext scanContext;
  private final MetadataTableType metadataTableType;

  private transient MetaDataReaderFunction rowDataReaderFunction;
  private transient Counter errorCounter;

  TableReader(
      String taskName,
      int taskIndex,
      TableLoader tableLoader,
      Schema projectedSchema,
      ScanContext scanContext,
      MetadataTableType metadataTableType) {
    Preconditions.checkNotNull(taskName, "Task name should no be null");
    Preconditions.checkNotNull(tableLoader, "Table should no be null");
    Preconditions.checkNotNull(projectedSchema, "The projected schema should no be null");

    this.tableLoader = tableLoader;
    this.taskName = taskName;
    this.taskIndex = taskIndex;
    this.projectedSchema = projectedSchema;
    this.scanContext = scanContext;
    this.metadataTableType = metadataTableType;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    tableLoader.open();
    Table table = tableLoader.loadTable();
    Table metaTable = MetadataTableUtils.createMetadataTableInstance(table, metadataTableType);
    this.errorCounter =
        TableMaintenanceMetrics.groupFor(getRuntimeContext(), table.name(), taskName, taskIndex)
            .counter(TableMaintenanceMetrics.ERROR_COUNTER);
    this.rowDataReaderFunction =
        new MetaDataReaderFunction(
            new Configuration(),
            metaTable.schema(),
            projectedSchema,
            metaTable.io(),
            metaTable.encryption());
    this.splitSerializer = new IcebergSourceSplitSerializer(scanContext.caseSensitive());
  }

  @Override
  public void processElement(
      MetadataTablePlanner.SplitInfo splitInfo, Context ctx, Collector<R> out) throws Exception {
    IcebergSourceSplit split = splitSerializer.deserialize(splitInfo.version(), splitInfo.split());
    try (DataIterator<RowData> iterator = rowDataReaderFunction.createDataIterator(split)) {
      iterator.forEachRemaining(rowData -> extract(rowData, out));
    } catch (Exception e) {
      LOG.warn("Exception processing split {} at {}", split, ctx.timestamp(), e);
      ctx.output(DeleteOrphanFiles.ERROR_STREAM, e);
      errorCounter.inc();
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    tableLoader.close();
  }

  /**
   * Extracts the desired data from the given RowData.
   *
   * @param rowData the RowData from which to extract
   * @param out the Collector to which to output the extracted data
   */
  abstract void extract(RowData rowData, Collector<R> out);
}
