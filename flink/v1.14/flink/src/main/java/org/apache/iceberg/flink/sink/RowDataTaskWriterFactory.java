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
import java.util.function.Function;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.DirectTaskWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitioningWriterFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ArrayUtil;

public class RowDataTaskWriterFactory implements TaskWriterFactory<RowData> {
  private final Table table;
  private final Schema schema;
  private final RowType flinkSchema;
  private final PartitionSpec spec;
  private final FileIO io;
  private final long targetFileSizeBytes;
  private final FileFormat format;
  private final List<Integer> equalityFieldIds;
  private final boolean upsert;
  private final FileWriterFactory<RowData> writerFactory;

  private transient OutputFileFactory outputFileFactory;

  public RowDataTaskWriterFactory(
      Table table,
      RowType flinkSchema,
      long targetFileSizeBytes,
      FileFormat format,
      List<Integer> equalityFieldIds,
      boolean upsert) {
    this.table = table;
    this.schema = table.schema();
    this.flinkSchema = flinkSchema;
    this.spec = table.spec();
    this.io = table.io();
    this.targetFileSizeBytes = targetFileSizeBytes;
    this.format = format;
    this.equalityFieldIds = equalityFieldIds;
    this.upsert = upsert;

    FlinkFileWriterFactory.Builder writerFactoryBuilder = FlinkFileWriterFactory.builderFor(table)
        .dataSchema(schema)
        .dataFlinkType(flinkSchema)
        .dataFileFormat(format)
        .deleteFileFormat(format);

    if (equalityFieldIds != null && !equalityFieldIds.isEmpty()) {
      writerFactoryBuilder
          .equalityFieldIds(ArrayUtil.toIntArray(equalityFieldIds))
          .equalityDeleteRowSchema(schema);
    }

    writerFactory = writerFactoryBuilder.build();
  }

  @Override
  public void initialize(int taskId, int attemptId) {
    this.outputFileFactory = OutputFileFactory.builderFor(table, taskId, attemptId).build();
  }

  @Override
  public TaskWriter<RowData> create() {
    Preconditions.checkNotNull(outputFileFactory,
        "The outputFileFactory shouldn't be null if we have invoked the initialize().");

    Function<RowData, StructLike> partitioner = spec.isPartitioned() ?
        FlinkTaskWriter.partitionerFor(spec, schema, flinkSchema) :
        DirectTaskWriter.unpartition();

    PartitioningWriterFactory<RowData> partitioningWriterFactory =
        PartitioningWriterFactory.builder(writerFactory)
            .fileFactory(outputFileFactory)
            .io(io)
            .fileFormat(format)
            .targetFileSizeInBytes(targetFileSizeBytes)
            .buildForFanoutPartition();

    if (equalityFieldIds == null || equalityFieldIds.isEmpty()) {
      return new DirectTaskWriter<>(partitioningWriterFactory, partitioner, spec, io);
    } else {
      return new FlinkTaskWriter(partitioningWriterFactory, partitioner,
          schema, spec, Sets.newHashSet(equalityFieldIds), io, flinkSchema, upsert);
    }
  }
}
