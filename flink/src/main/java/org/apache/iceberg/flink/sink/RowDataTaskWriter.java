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
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.io.DeltaWriter;
import org.apache.iceberg.io.DeltaWriterFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriterResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.Tasks;

public class RowDataTaskWriter implements TaskWriter<RowData> {

  private final PartitionSpec spec;
  private final FlinkDeltaWriterFactory deltaWriterFactory;
  private final PartitionKey partitionKey;
  private final RowDataWrapper rowDataWrapper;

  private final Map<PartitionKey, DeltaWriter<RowData>> deltaWriterMap;
  private final DeltaWriterFactory.Context ctxt;

  RowDataTaskWriter(Schema schema,
                    RowType flinkSchema,
                    PartitionSpec spec,
                    FileFormat format,
                    OutputFileFactory fileFactory,
                    FileIO io,
                    long targetFileSize,
                    Map<String, String> tableProperties) {
    this.spec = spec;
    this.deltaWriterFactory = new FlinkDeltaWriterFactory(schema, flinkSchema, spec,
        format, fileFactory, io, targetFileSize, tableProperties);
    this.partitionKey = new PartitionKey(spec, schema);
    this.rowDataWrapper = new RowDataWrapper(flinkSchema, schema.asStruct());

    this.deltaWriterMap = Maps.newHashMap();

    // TODO make it to be a valuable equality field ids.
    List<Integer> equalityFieldIds = Lists.newArrayList();
    this.ctxt = DeltaWriterFactory.Context.builder()
        //.allowEqualityDelete(true)  TODO enable this switch???
        //.equalityFieldIds(equalityFieldIds)
        .rowSchema(TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds)))
        .build();
  }

  @Override
  public void write(RowData row) throws IOException {
    DeltaWriter<RowData> deltaWriter;

    if (spec.fields().size() <= 0) {
      // Create and cache the delta writer if absent for unpartitioned table.
      deltaWriter = deltaWriterMap.get(partitionKey);
      if (deltaWriter == null) {
        deltaWriter = deltaWriterFactory.createDeltaWriter(null, ctxt);
        deltaWriterMap.put(partitionKey, deltaWriter);
      }
    } else {
      // Refresh the current partition key.
      partitionKey.partition(rowDataWrapper.wrap(row));

      // Create and cache the delta writer if absent for partitioned table.
      deltaWriter = deltaWriterMap.get(partitionKey);
      if (deltaWriter == null) {
        // NOTICE: we need to copy a new partition key here, in case of messing up the keys in writers.
        PartitionKey copiedKey = partitionKey.copy();
        deltaWriter = deltaWriterFactory.createDeltaWriter(copiedKey, ctxt);
        deltaWriterMap.put(copiedKey, deltaWriter);
      }
    }

    switch (row.getRowKind()) {
      case INSERT:
      case UPDATE_AFTER:
        deltaWriter.writeRow(row);
        break;

      case DELETE:
      case UPDATE_BEFORE:
        deltaWriter.writeEqualityDelete(row);
        break;

      default:
        throw new UnsupportedOperationException("Unknown row kind: " + row.getRowKind());
    }
  }

  @Override
  public void abort() {
    close();

    Tasks.foreach(deltaWriterMap.values())
        .throwFailureWhenFinished()
        .noRetry()
        .run(deltaWriter -> {
          try {
            deltaWriter.abort();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }

  @Override
  public WriterResult complete() throws IOException {
    close();

    WriterResult.Builder builder = WriterResult.builder();
    for (DeltaWriter<RowData> writer : deltaWriterMap.values()) {
      builder.add(writer.complete());
    }

    return builder.build();
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
}
