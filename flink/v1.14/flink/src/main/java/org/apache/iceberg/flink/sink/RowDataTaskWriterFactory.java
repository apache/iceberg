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

import java.io.IOException;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.io.BaseEqualityDeltaWriter;
import org.apache.iceberg.io.ClusteredDataWriter;
import org.apache.iceberg.io.ClusteredEqualityDeleteWriter;
import org.apache.iceberg.io.ClusteredPositionDeleteWriter;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.EqualityDeltaWriter;
import org.apache.iceberg.io.FanoutDataWriter;
import org.apache.iceberg.io.FanoutEqualityDeleteWriter;
import org.apache.iceberg.io.FanoutPositionDeleteWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitioningWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.Tasks;

public class RowDataTaskWriterFactory implements TaskWriterFactory<RowData> {
  private final Table table;
  private final RowType flinkSchema;
  private final Schema schema;
  private final PartitionSpec spec;
  private final FileIO io;

  private final Schema deleteSchema;

  private final long targetFileSizeBytes;
  private final FileFormat format;
  private final boolean upsert;

  private final FlinkFileWriterFactory fileWriterFactory;
  private transient OutputFileFactory outputFileFactory;

  public RowDataTaskWriterFactory(
      Table table,
      RowType flinkSchema,
      long targetFileSizeBytes,
      FileFormat format,
      List<Integer> equalityFieldIds,
      boolean upsert) {
    this.table = table;
    this.flinkSchema = flinkSchema;
    this.schema = table.schema();
    this.spec = table.spec();
    this.io = table.io();

    this.targetFileSizeBytes = targetFileSizeBytes;
    this.format = format;
    this.upsert = upsert;

    if (equalityFieldIds != null && !equalityFieldIds.isEmpty()) {
      this.fileWriterFactory = FlinkFileWriterFactory.builderFor(table)
          .dataSchema(schema)
          .dataFlinkType(flinkSchema)
          .equalityFieldIds(ArrayUtil.toIntArray(equalityFieldIds))
          .equalityDeleteRowSchema(schema)
          .build();

      this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
    } else {
      this.fileWriterFactory = FlinkFileWriterFactory.builderFor(table)
          .dataSchema(schema)
          .dataFlinkType(flinkSchema)
          .build();

      this.deleteSchema = null;
    }
  }

  @Override
  public void initialize(int taskId, int attemptId) {
    this.outputFileFactory = OutputFileFactory.builderFor(table, taskId, attemptId).build();
  }

  @Override
  public TaskWriter<RowData> create() {
    Preconditions.checkNotNull(
        outputFileFactory,
        "The outputFileFactory shouldn't be null if we have invoked the initialize().");

    if (deleteSchema != null) {
      return new DeltaWriter(spec.isPartitioned());
    } else {
      return new InsertOnlyWriter(spec.isPartitioned());
    }
  }

  private static <T extends ContentFile<T>> void cleanFiles(FileIO io, T[] files) {
    Tasks.foreach(files)
        .throwFailureWhenFinished()
        .noRetry()
        .run(file -> io.deleteFile(file.path().toString()));
  }

  private abstract class FlinkBaseWriter implements TaskWriter<RowData> {
    private final WriteResult.Builder writeResultBuilder = WriteResult.builder();

    @Override
    public void write(RowData row) throws IOException {
      switch (row.getRowKind()) {
        case INSERT:
        case UPDATE_AFTER:
          if (upsert) {
            delete(row);
          }
          insert(row);
          break;

        case UPDATE_BEFORE:
          if (upsert) {
            // Skip to write delete in upsert mode because UPDATE_AFTER will insert after delete.
            break;
          }
          delete(row);
          break;

        case DELETE:
          delete(row);
          break;

        default:
          throw new UnsupportedOperationException("Unknown row kind: " + row.getRowKind());
      }
    }

    protected void delete(RowData row) {
      throw new UnsupportedOperationException(this.getClass().getName() + " does not implement delete.");
    }

    protected void insert(RowData row) {
      throw new UnsupportedOperationException(this.getClass().getName() + " does not implement insert.");
    }

    protected void aggregateResult(DataWriteResult result) {
      writeResultBuilder.addDataFiles(result.dataFiles());
    }

    protected void aggregateResult(WriteResult result) {
      writeResultBuilder.add(result);
    }

    @Override
    public void abort() throws IOException {
      close();

      WriteResult result = writeResultBuilder.build();
      cleanFiles(io, result.dataFiles());
      cleanFiles(io, result.deleteFiles());
    }

    @Override
    public WriteResult complete() throws IOException {
      close();

      return writeResultBuilder.build();
    }
  }

  private PartitioningWriter<RowData, DataWriteResult> newDataWriter(boolean fanoutEnabled) {
    if (fanoutEnabled) {
      return new FanoutDataWriter<>(fileWriterFactory, outputFileFactory, io, format, targetFileSizeBytes);
    } else {
      return new ClusteredDataWriter<>(fileWriterFactory, outputFileFactory, io, format, targetFileSizeBytes);
    }
  }

  private PartitioningWriter<RowData, DeleteWriteResult> newEqualityWriter(boolean fanoutEnabled) {
    if (fanoutEnabled) {
      return new FanoutEqualityDeleteWriter<>(fileWriterFactory, outputFileFactory, io, format, targetFileSizeBytes);
    } else {
      return new ClusteredEqualityDeleteWriter<>(fileWriterFactory, outputFileFactory, io, format, targetFileSizeBytes);
    }
  }

  private PartitioningWriter<PositionDelete<RowData>, DeleteWriteResult> newPositionWriter(boolean fanoutEnabled) {
    if (fanoutEnabled) {
      return new FanoutPositionDeleteWriter<>(fileWriterFactory, outputFileFactory, io, format, targetFileSizeBytes);
    } else {
      return new ClusteredPositionDeleteWriter<>(fileWriterFactory, outputFileFactory, io, format, targetFileSizeBytes);
    }
  }

  private class InsertOnlyWriter extends FlinkBaseWriter {

    private final PartitionKey partitionKey;
    private final RowDataWrapper rowDataWrapper;

    private final PartitioningWriter<RowData, DataWriteResult> delegate;
    private boolean closed = false;

    private InsertOnlyWriter(boolean fanoutEnabled) {
      this.partitionKey = new PartitionKey(spec, schema);
      this.rowDataWrapper = new RowDataWrapper(flinkSchema, schema.asStruct());

      this.delegate = newDataWriter(fanoutEnabled);
    }

    @Override
    protected void insert(RowData row) {
      partitionKey.partition(rowDataWrapper.wrap(row));
      delegate.write(row, spec, partitionKey);
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        delegate.close();
        aggregateResult(delegate.result());

        closed = true;
      }
    }
  }

  private class DeltaWriter extends FlinkBaseWriter {

    private final PartitionKey partitionKey;
    private final RowDataWrapper rowDataWrapper;

    private final EqualityDeltaWriter<RowData> delegate;
    private boolean closed = false;

    private DeltaWriter(boolean fanoutEnabled) {
      this.partitionKey = new PartitionKey(spec, schema);
      this.rowDataWrapper = new RowDataWrapper(flinkSchema, schema.asStruct());

      this.delegate = new BaseEqualityDeltaWriter<>(
          newDataWriter(fanoutEnabled),
          newEqualityWriter(fanoutEnabled),
          newPositionWriter(fanoutEnabled),
          schema, deleteSchema,
          table.properties(),
          rowDataWrapper::wrap);
    }

    protected void delete(RowData row) {
      partitionKey.partition(rowDataWrapper.wrap(row));
      delegate.delete(row, spec, partitionKey);
    }

    protected void insert(RowData row) {
      partitionKey.partition(rowDataWrapper.wrap(row));
      delegate.insert(row, spec, partitionKey);
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        delegate.close();
        aggregateResult(delegate.result());

        this.closed = true;
      }
    }
  }
}
