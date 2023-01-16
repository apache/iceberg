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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.ClusteredDataWriter;
import org.apache.iceberg.io.ClusteredEqualityDeleteWriter;
import org.apache.iceberg.io.ClusteredPositionDeleteWriter;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FanoutDataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;

public abstract class WritersBenchmark extends IcebergSourceBenchmark {

  private static final int NUM_ROWS = 2500000;
  private static final long TARGET_FILE_SIZE_IN_BYTES = 50L * 1024 * 1024;

  private static final Schema SCHEMA =
      new Schema(
          required(1, "longCol", Types.LongType.get()),
          required(2, "intCol", Types.IntegerType.get()),
          required(3, "floatCol", Types.FloatType.get()),
          optional(4, "doubleCol", Types.DoubleType.get()),
          optional(5, "decimalCol", Types.DecimalType.of(20, 5)),
          optional(6, "timestampCol", Types.TimestampType.withZone()),
          optional(7, "stringCol", Types.StringType.get()));

  private Iterable<InternalRow> rows;
  private Iterable<InternalRow> positionDeleteRows;
  private PartitionSpec unpartitionedSpec;
  private PartitionSpec partitionedSpec;

  @Override
  protected abstract FileFormat fileFormat();

  @Setup
  public void setupBenchmark() {
    setupSpark();

    List<InternalRow> data = Lists.newArrayList(RandomData.generateSpark(SCHEMA, NUM_ROWS, 0L));
    Transform<Integer, Integer> transform = Transforms.bucket(Types.IntegerType.get(), 32);
    data.sort(Comparator.comparingInt(row -> transform.apply(row.getInt(1))));
    this.rows = data;

    this.positionDeleteRows =
        RandomData.generateSpark(DeleteSchemaUtil.pathPosSchema(), NUM_ROWS, 0L);

    this.unpartitionedSpec = table().specs().get(0);
    Preconditions.checkArgument(unpartitionedSpec.isUnpartitioned());
    this.partitionedSpec = table().specs().get(1);
  }

  @TearDown
  public void tearDownBenchmark() throws IOException {
    tearDownSpark();
    cleanupFiles();
  }

  @Override
  protected Configuration initHadoopConf() {
    return new Configuration();
  }

  @Override
  protected final Table initTable() {
    HadoopTables tables = new HadoopTables(hadoopConf());
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> properties = Maps.newHashMap();
    Table table = tables.create(SCHEMA, spec, properties, newTableLocation());

    // add a partitioned spec to the table
    table.updateSpec().addField(Expressions.bucket("intCol", 32)).commit();

    return table;
  }

  @Benchmark
  @Threads(1)
  public void writeUnpartitionedClusteredDataWriter(Blackhole blackhole) throws IOException {
    FileIO io = table().io();

    OutputFileFactory fileFactory = newFileFactory();
    SparkFileWriterFactory writerFactory =
        SparkFileWriterFactory.builderFor(table())
            .dataFileFormat(fileFormat())
            .dataSchema(table().schema())
            .build();

    ClusteredDataWriter<InternalRow> writer =
        new ClusteredDataWriter<>(writerFactory, fileFactory, io, TARGET_FILE_SIZE_IN_BYTES);

    try (ClusteredDataWriter<InternalRow> closeableWriter = writer) {
      for (InternalRow row : rows) {
        closeableWriter.write(row, unpartitionedSpec, null);
      }
    }

    blackhole.consume(writer);
  }

  @Benchmark
  @Threads(1)
  public void writeUnpartitionedLegacyDataWriter(Blackhole blackhole) throws IOException {
    FileIO io = table().io();

    OutputFileFactory fileFactory = newFileFactory();

    Schema writeSchema = table().schema();
    StructType sparkWriteType = SparkSchemaUtil.convert(writeSchema);
    SparkAppenderFactory appenders =
        SparkAppenderFactory.builderFor(table(), writeSchema, sparkWriteType)
            .spec(unpartitionedSpec)
            .build();

    TaskWriter<InternalRow> writer =
        new UnpartitionedWriter<>(
            unpartitionedSpec, fileFormat(), appenders, fileFactory, io, TARGET_FILE_SIZE_IN_BYTES);

    try (TaskWriter<InternalRow> closableWriter = writer) {
      for (InternalRow row : rows) {
        closableWriter.write(row);
      }
    }

    blackhole.consume(writer.complete());
  }

  @Benchmark
  @Threads(1)
  public void writePartitionedClusteredDataWriter(Blackhole blackhole) throws IOException {
    FileIO io = table().io();

    OutputFileFactory fileFactory = newFileFactory();
    SparkFileWriterFactory writerFactory =
        SparkFileWriterFactory.builderFor(table())
            .dataFileFormat(fileFormat())
            .dataSchema(table().schema())
            .build();

    ClusteredDataWriter<InternalRow> writer =
        new ClusteredDataWriter<>(writerFactory, fileFactory, io, TARGET_FILE_SIZE_IN_BYTES);

    PartitionKey partitionKey = new PartitionKey(partitionedSpec, table().schema());
    StructType dataSparkType = SparkSchemaUtil.convert(table().schema());
    InternalRowWrapper internalRowWrapper = new InternalRowWrapper(dataSparkType);

    try (ClusteredDataWriter<InternalRow> closeableWriter = writer) {
      for (InternalRow row : rows) {
        partitionKey.partition(internalRowWrapper.wrap(row));
        closeableWriter.write(row, partitionedSpec, partitionKey);
      }
    }

    blackhole.consume(writer);
  }

  @Benchmark
  @Threads(1)
  public void writePartitionedLegacyDataWriter(Blackhole blackhole) throws IOException {
    FileIO io = table().io();

    OutputFileFactory fileFactory = newFileFactory();

    Schema writeSchema = table().schema();
    StructType sparkWriteType = SparkSchemaUtil.convert(writeSchema);
    SparkAppenderFactory appenders =
        SparkAppenderFactory.builderFor(table(), writeSchema, sparkWriteType)
            .spec(partitionedSpec)
            .build();

    TaskWriter<InternalRow> writer =
        new SparkPartitionedWriter(
            partitionedSpec,
            fileFormat(),
            appenders,
            fileFactory,
            io,
            TARGET_FILE_SIZE_IN_BYTES,
            writeSchema,
            sparkWriteType);

    try (TaskWriter<InternalRow> closableWriter = writer) {
      for (InternalRow row : rows) {
        closableWriter.write(row);
      }
    }

    blackhole.consume(writer.complete());
  }

  @Benchmark
  @Threads(1)
  public void writePartitionedFanoutDataWriter(Blackhole blackhole) throws IOException {
    FileIO io = table().io();

    OutputFileFactory fileFactory = newFileFactory();
    SparkFileWriterFactory writerFactory =
        SparkFileWriterFactory.builderFor(table())
            .dataFileFormat(fileFormat())
            .dataSchema(table().schema())
            .build();

    FanoutDataWriter<InternalRow> writer =
        new FanoutDataWriter<>(writerFactory, fileFactory, io, TARGET_FILE_SIZE_IN_BYTES);

    PartitionKey partitionKey = new PartitionKey(partitionedSpec, table().schema());
    StructType dataSparkType = SparkSchemaUtil.convert(table().schema());
    InternalRowWrapper internalRowWrapper = new InternalRowWrapper(dataSparkType);

    try (FanoutDataWriter<InternalRow> closeableWriter = writer) {
      for (InternalRow row : rows) {
        partitionKey.partition(internalRowWrapper.wrap(row));
        closeableWriter.write(row, partitionedSpec, partitionKey);
      }
    }

    blackhole.consume(writer);
  }

  @Benchmark
  @Threads(1)
  public void writePartitionedLegacyFanoutDataWriter(Blackhole blackhole) throws IOException {
    FileIO io = table().io();

    OutputFileFactory fileFactory = newFileFactory();

    Schema writeSchema = table().schema();
    StructType sparkWriteType = SparkSchemaUtil.convert(writeSchema);
    SparkAppenderFactory appenders =
        SparkAppenderFactory.builderFor(table(), writeSchema, sparkWriteType)
            .spec(partitionedSpec)
            .build();

    TaskWriter<InternalRow> writer =
        new SparkPartitionedFanoutWriter(
            partitionedSpec,
            fileFormat(),
            appenders,
            fileFactory,
            io,
            TARGET_FILE_SIZE_IN_BYTES,
            writeSchema,
            sparkWriteType);

    try (TaskWriter<InternalRow> closableWriter = writer) {
      for (InternalRow row : rows) {
        closableWriter.write(row);
      }
    }

    blackhole.consume(writer.complete());
  }

  @Benchmark
  @Threads(1)
  public void writePartitionedClusteredEqualityDeleteWriter(Blackhole blackhole)
      throws IOException {
    FileIO io = table().io();

    int equalityFieldId = table().schema().findField("longCol").fieldId();

    OutputFileFactory fileFactory = newFileFactory();
    SparkFileWriterFactory writerFactory =
        SparkFileWriterFactory.builderFor(table())
            .dataFileFormat(fileFormat())
            .equalityDeleteRowSchema(table().schema())
            .equalityFieldIds(new int[] {equalityFieldId})
            .build();

    ClusteredEqualityDeleteWriter<InternalRow> writer =
        new ClusteredEqualityDeleteWriter<>(
            writerFactory, fileFactory, io, TARGET_FILE_SIZE_IN_BYTES);

    PartitionKey partitionKey = new PartitionKey(partitionedSpec, table().schema());
    StructType deleteSparkType = SparkSchemaUtil.convert(table().schema());
    InternalRowWrapper internalRowWrapper = new InternalRowWrapper(deleteSparkType);

    try (ClusteredEqualityDeleteWriter<InternalRow> closeableWriter = writer) {
      for (InternalRow row : rows) {
        partitionKey.partition(internalRowWrapper.wrap(row));
        closeableWriter.write(row, partitionedSpec, partitionKey);
      }
    }

    blackhole.consume(writer);
  }

  @Benchmark
  @Threads(1)
  public void writeUnpartitionedClusteredPositionDeleteWriter(Blackhole blackhole)
      throws IOException {
    FileIO io = table().io();

    OutputFileFactory fileFactory = newFileFactory();
    SparkFileWriterFactory writerFactory =
        SparkFileWriterFactory.builderFor(table()).dataFileFormat(fileFormat()).build();

    ClusteredPositionDeleteWriter<InternalRow> writer =
        new ClusteredPositionDeleteWriter<>(
            writerFactory, fileFactory, io, TARGET_FILE_SIZE_IN_BYTES);

    PositionDelete<InternalRow> positionDelete = PositionDelete.create();
    try (ClusteredPositionDeleteWriter<InternalRow> closeableWriter = writer) {
      for (InternalRow row : positionDeleteRows) {
        String path = row.getString(0);
        long pos = row.getLong(1);
        positionDelete.set(path, pos, null);
        closeableWriter.write(positionDelete, unpartitionedSpec, null);
      }
    }

    blackhole.consume(writer);
  }

  private OutputFileFactory newFileFactory() {
    return OutputFileFactory.builderFor(table(), 1, 1).format(fileFormat()).build();
  }
}
