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
package org.apache.iceberg.flink.sink;

import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.TableSupplier;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.ArrayUtil;

public class RowDataTaskWriterFactory implements TaskWriterFactory<RowData> {
  private final TableSupplier tableSupplier;
  private final Table initTable;
  private final Schema schema;
  private final RowType flinkSchema;
  private final PartitionSpec spec;
  private final long targetFileSizeBytes;
  private final FileFormat format;
  private final List<Integer> equalityFieldIds;
  private final boolean upsert;
  private final FileAppenderFactory<RowData> appenderFactory;

  private transient OutputFileFactory outputFileFactory;

  /**
   * @deprecated since 1.4.0, will be removed in 1.5.0. Use the constructor that accepts a table
   *     supplier instead.
   */
  @Deprecated
  public RowDataTaskWriterFactory(
      Table table,
      RowType flinkSchema,
      long targetFileSizeBytes,
      FileFormat format,
      Map<String, String> writeProperties,
      List<Integer> equalityFieldIds,
      boolean upsert) {
    this(
        () -> table,
        flinkSchema,
        targetFileSizeBytes,
        format,
        writeProperties,
        equalityFieldIds,
        upsert);
  }

  public RowDataTaskWriterFactory(
      TableSupplier tableSupplier,
      RowType flinkSchema,
      long targetFileSizeBytes,
      FileFormat format,
      Map<String, String> writeProperties,
      List<Integer> equalityFieldIds,
      boolean upsert) {
    this.tableSupplier = tableSupplier;
    // rely on the initial table metadata for schema, etc., until schema evolution is supported
    this.initTable = tableSupplier.get();
    this.schema = initTable.schema();
    this.flinkSchema = flinkSchema;
    this.spec = initTable.spec();
    this.targetFileSizeBytes = targetFileSizeBytes;
    this.format = format;
    this.equalityFieldIds = equalityFieldIds;
    this.upsert = upsert;

    if (equalityFieldIds == null || equalityFieldIds.isEmpty()) {
      this.appenderFactory =
          new FlinkAppenderFactory(
              initTable, schema, flinkSchema, writeProperties, spec, null, null, null);
    } else if (upsert) {
      // In upsert mode, only the new row is emitted using INSERT row kind. Therefore, any column of
      // the inserted row
      // may differ from the deleted row other than the primary key fields, and the delete file must
      // contain values
      // that are correct for the deleted row. Therefore, only write the equality delete fields.
      this.appenderFactory =
          new FlinkAppenderFactory(
              initTable,
              schema,
              flinkSchema,
              writeProperties,
              spec,
              ArrayUtil.toIntArray(equalityFieldIds),
              TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds)),
              null);
    } else {
      this.appenderFactory =
          new FlinkAppenderFactory(
              initTable,
              schema,
              flinkSchema,
              writeProperties,
              spec,
              ArrayUtil.toIntArray(equalityFieldIds),
              schema,
              null);
    }
  }

  public void refreshTable() {
    tableSupplier.refreshTable();
  }

  @Override
  public void initialize(int taskId, int attemptId) {
    this.outputFileFactory =
        OutputFileFactory.builderFor(initTable, taskId, attemptId)
            .format(format)
            .ioSupplier(() -> tableSupplier.get().io())
            .build();
  }

  @Override
  public TaskWriter<RowData> create() {
    Preconditions.checkNotNull(
        outputFileFactory,
        "The outputFileFactory shouldn't be null if we have invoked the initialize().");

    FileIO io = tableSupplier.get().io();

    if (equalityFieldIds == null || equalityFieldIds.isEmpty()) {
      // Initialize a task writer to write INSERT only.
      if (spec.isUnpartitioned()) {
        return new UnpartitionedWriter<>(
            spec, format, appenderFactory, outputFileFactory, io, targetFileSizeBytes);
      } else {
        return new RowDataPartitionedFanoutWriter(
            spec,
            format,
            appenderFactory,
            outputFileFactory,
            io,
            targetFileSizeBytes,
            schema,
            flinkSchema);
      }
    } else {
      // Initialize a task writer to write both INSERT and equality DELETE.
      if (spec.isUnpartitioned()) {
        return new UnpartitionedDeltaWriter(
            spec,
            format,
            appenderFactory,
            outputFileFactory,
            io,
            targetFileSizeBytes,
            schema,
            flinkSchema,
            equalityFieldIds,
            upsert);
      } else {
        return new PartitionedDeltaWriter(
            spec,
            format,
            appenderFactory,
            outputFileFactory,
            io,
            targetFileSizeBytes,
            schema,
            flinkSchema,
            equalityFieldIds,
            upsert);
      }
    }
  }

  private static class RowDataPartitionedFanoutWriter extends PartitionedFanoutWriter<RowData> {

    private final PartitionKey partitionKey;
    private final RowDataWrapper rowDataWrapper;

    RowDataPartitionedFanoutWriter(
        PartitionSpec spec,
        FileFormat format,
        FileAppenderFactory<RowData> appenderFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        long targetFileSize,
        Schema schema,
        RowType flinkSchema) {
      super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
      this.partitionKey = new PartitionKey(spec, schema);
      this.rowDataWrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
    }

    @Override
    protected PartitionKey partition(RowData row) {
      partitionKey.partition(rowDataWrapper.wrap(row));
      return partitionKey;
    }
  }
}
