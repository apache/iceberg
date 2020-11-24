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
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;

public class RowDataTaskWriter extends BaseTaskWriter<RowData> {

  private static final PartitionKey UNPARTITIONED_KEY = new PartitionKey(PartitionSpec.unpartitioned(), null);

  private final Schema schema;
  private final PartitionKey partitionKey;
  private final RowDataWrapper rowDataWrapper;
  private final List<Integer> equalityFieldIds;

  private final Map<PartitionKey, RowDataDeltaWriter> deltaWriterMap = Maps.newHashMap();

  RowDataTaskWriter(PartitionSpec spec,
                    FileFormat format,
                    FileAppenderFactory<RowData> appenderFactory,
                    OutputFileFactory fileFactory,
                    FileIO io,
                    long targetFileSize,
                    Schema schema,
                    RowType flinkSchema,
                    List<Integer> equalityFieldIds) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);

    this.schema = schema;
    this.equalityFieldIds = equalityFieldIds;

    this.partitionKey = new PartitionKey(spec, schema);
    this.rowDataWrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
  }

  @Override
  public void write(RowData row) throws IOException {
    RowDataDeltaWriter deltaWriter;

    if (spec().fields().size() <= 0) {
      // Create and cache the delta writer if absent for unpartitioned table.
      deltaWriter = deltaWriterMap.get(UNPARTITIONED_KEY);
      if (deltaWriter == null) {
        deltaWriter = new RowDataDeltaWriter(null, equalityFieldIds);
        deltaWriterMap.put(UNPARTITIONED_KEY, deltaWriter);
      }
    } else {
      // Refresh the current partition key.
      partitionKey.partition(rowDataWrapper.wrap(row));

      // Create and cache the delta writer if absent for partitioned table.
      deltaWriter = deltaWriterMap.get(partitionKey);
      if (deltaWriter == null) {
        // NOTICE: we need to copy a new partition key here, in case of messing up the keys in writers.
        PartitionKey copiedKey = partitionKey.copy();
        deltaWriter = new RowDataDeltaWriter(copiedKey, equalityFieldIds);
        deltaWriterMap.put(copiedKey, deltaWriter);
      }
    }

    switch (row.getRowKind()) {
      case INSERT:
      case UPDATE_AFTER:
        deltaWriter.write(row);
        break;

      case DELETE:
      case UPDATE_BEFORE:
        deltaWriter.delete(row);
        break;

      default:
        throw new UnsupportedOperationException("Unknown row kind: " + row.getRowKind());
    }
  }

  @Override
  public void close() {
    Tasks.foreach(deltaWriterMap.values())
        .throwFailureWhenFinished()
        .noRetry()
        .run(deltaWriter -> {
          try {
            deltaWriter.close();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }

  private class RowDataDeltaWriter extends BaseDeltaWriter {

    private RowDataDeltaWriter(PartitionKey partition, List<Integer> equalityFieldIds) {
      super(partition, equalityFieldIds, schema);
    }

    @Override
    protected StructLike asKey(RowData row) {
      return rowDataWrapper.wrap(row);
    }

    @Override
    protected StructLike asCopiedKey(RowData row) {
      return rowDataWrapper.wrap(row).copy();
    }
  }
}
