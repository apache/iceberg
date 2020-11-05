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
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.ContentFileWriterFactory;
import org.apache.iceberg.DataFileWriterFactory;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.data.FlinkAvroWriter;
import org.apache.iceberg.flink.data.FlinkOrcWriter;
import org.apache.iceberg.flink.data.FlinkParquetWriters;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.FileGroupWriter;
import org.apache.iceberg.io.WriterResult;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class RowDataTaskWriterFactory implements TaskWriterFactory<RowData> {
  private final Schema schema;
  private final RowType flinkSchema;
  private final PartitionSpec spec;
  private final LocationProvider locations;
  private final FileIO io;
  private final EncryptionManager encryptionManager;
  private final long targetFileSizeBytes;
  private final FileFormat format;
  private final Map<String, String> tableProperties;
  private final FileAppenderFactory<RowData> appenderFactory;

  private transient OutputFileFactory outputFileFactory;

  public RowDataTaskWriterFactory(Schema schema,
                                  RowType flinkSchema,
                                  PartitionSpec spec,
                                  LocationProvider locations,
                                  FileIO io,
                                  EncryptionManager encryptionManager,
                                  long targetFileSizeBytes,
                                  FileFormat format,
                                  Map<String, String> tableProperties) {
    this.schema = schema;
    this.flinkSchema = flinkSchema;
    this.spec = spec;
    this.locations = locations;
    this.io = io;
    this.encryptionManager = encryptionManager;
    this.targetFileSizeBytes = targetFileSizeBytes;
    this.format = format;
    this.tableProperties = tableProperties;
    this.appenderFactory = new FlinkFileAppenderFactory(schema, flinkSchema, tableProperties);
  }

  @Override
  public void initialize(int taskId, int attemptId) {
    this.outputFileFactory = new OutputFileFactory(spec, format, locations, io, encryptionManager, taskId, attemptId);
  }

  @Override
  public FileGroupWriter<RowData> create() {
    Preconditions.checkNotNull(outputFileFactory,
        "The outputFileFactory shouldn't be null if we have invoked the initialize().");

    if (spec.fields().isEmpty()) {
      return new MixedUnpartitionedFileGroupWriter(format, appenderFactory, outputFileFactory, io, targetFileSizeBytes,
          schema, flinkSchema, tableProperties);
    } else {
      return new MixedPartitionedFileGroupWriter(spec, format, appenderFactory, outputFileFactory,
          io, targetFileSizeBytes, schema, flinkSchema, tableProperties);
    }
  }

  private abstract static class BaseMixedFileGroupWriter implements FileGroupWriter<RowData> {

    abstract boolean supportDeletion();

    abstract FileGroupWriter<RowData> dataTaskWriter();

    abstract FileGroupWriter<RowData> deleteTaskWriter();

    @Override
    public void write(RowData row) throws IOException {
      switch (row.getRowKind()) {

        case INSERT:
        case UPDATE_AFTER:
          dataTaskWriter().write(row);
          break;

        case UPDATE_BEFORE:
        case DELETE:
          if (supportDeletion()) {
            deleteTaskWriter().write(row);
          } else {
            throw new UnsupportedOperationException("Couldn't accept deletion in task writer.");
          }
          break;

        default:
          throw new UnsupportedOperationException("Unrecognized row kind: " + row.getRowKind());
      }
    }

    @Override
    public void abort() throws IOException {
      dataTaskWriter().abort();
      if (supportDeletion()) {
        deleteTaskWriter().abort();
      }
    }

    @Override
    public WriterResult complete() throws IOException {
      if (supportDeletion()) {
        return WriterResult.concat(dataTaskWriter().complete(), deleteTaskWriter().complete());
      } else {
        return dataTaskWriter().complete();
      }
    }

    @Override
    public void close() throws IOException {
      dataTaskWriter().close();
      if (supportDeletion()) {
        deleteTaskWriter().close();
      }
    }
  }

  private static class MixedUnpartitionedFileGroupWriter extends BaseMixedFileGroupWriter {
    private final FileFormat format;
    private final FileGroupWriter<RowData> dataFileGroupWriter;
    private final FileGroupWriter<RowData> deleteFileGroupWriter;

    MixedUnpartitionedFileGroupWriter(FileFormat format, FileAppenderFactory<RowData> appenderFactory,
                                      OutputFileFactory fileFactory, FileIO io, long targetFileSize, Schema schema,
                                      RowType flinkSchema, Map<String, String> tableProperties) {
      this.format = format;

      this.dataFileGroupWriter = new UnpartitionedWriter<>(format, fileFactory, io,
          targetFileSize, new DataFileWriterFactory<>(appenderFactory, PartitionSpec.unpartitioned()));

      // TODO: set the correct equality field ids.
      List<Integer> equalityIds = ImmutableList.of();

      if (supportDeletion()) {
        this.deleteFileGroupWriter = new UnpartitionedWriter<>(format, fileFactory, io, targetFileSize,
            new FlinkEqualityDeleterFactory(schema, flinkSchema, PartitionSpec.unpartitioned(), equalityIds,
                tableProperties));
      } else {
        this.deleteFileGroupWriter = null;
      }
    }

    @Override
    boolean supportDeletion() {
      return FileFormat.PARQUET.equals(format) || FileFormat.AVRO.equals(format);
    }

    @Override
    FileGroupWriter<RowData> dataTaskWriter() {
      return dataFileGroupWriter;
    }

    @Override
    FileGroupWriter<RowData> deleteTaskWriter() {
      return deleteFileGroupWriter;
    }
  }

  private static class MixedPartitionedFileGroupWriter extends BaseMixedFileGroupWriter {
    private final FileFormat format;
    private final FileGroupWriter<RowData> dataFileGroupWriter;
    private final FileGroupWriter<RowData> deleteFileGroupWriter;
    private final FileGroupWriter<PositionDelete<RowData>> posDelFileGroupWriter;

    MixedPartitionedFileGroupWriter(PartitionSpec spec, FileFormat format, FileAppenderFactory<RowData> appenderFactory,
                                    OutputFileFactory fileFactory, FileIO io, long targetFileSize, Schema schema,
                                    RowType flinkSchema, Map<String, String> tableProperties) {
      this.format = format;

      this.dataFileGroupWriter =
          new RowDataPartitionedFanoutWriter<>(spec, format, fileFactory, io, targetFileSize, schema,
              flinkSchema, new DataFileWriterFactory<>(appenderFactory, spec));

      // TODO: set the correct equality field ids.
      List<Integer> equalityIds = ImmutableList.of();

      if (supportDeletion()) {
        this.deleteFileGroupWriter =
            new RowDataPartitionedFanoutWriter<>(spec, format, fileFactory, io, targetFileSize, schema,
                flinkSchema, new FlinkEqualityDeleterFactory(schema, flinkSchema, spec, equalityIds, tableProperties));
        this.posDelFileGroupWriter =
            new PosPartitionedFanoutWriter<>(spec, format, fileFactory, io, targetFileSize, schema,
                flinkSchema, new FlinkPositionDeleteWriterFactory(schema, spec, tableProperties));
      } else {
        this.deleteFileGroupWriter = null;
        this.posDelFileGroupWriter = null;
      }
    }

    @Override
    boolean supportDeletion() {
      return FileFormat.PARQUET.equals(format) || FileFormat.AVRO.equals(format);
    }

    @Override
    public void write(RowData row) throws IOException {
      switch (row.getRowKind()) {

        case INSERT:
        case UPDATE_AFTER:
          dataTaskWriter().write(row);
          break;

        case UPDATE_BEFORE:
        case DELETE:
          if (supportDeletion()) {
            deleteTaskWriter().write(row);
          } else {
            throw new UnsupportedOperationException("Couldn't accept deletion in task writer.");
          }
          break;

        default:
          throw new UnsupportedOperationException("Unrecognized row kind: " + row.getRowKind());
      }
    }

    @Override
    FileGroupWriter<RowData> dataTaskWriter() {
      return dataFileGroupWriter;
    }

    @Override
    FileGroupWriter<RowData> deleteTaskWriter() {
      return deleteFileGroupWriter;
    }
  }

  private static class RowDataPartitionedFanoutWriter<ContentFileT>
      extends PartitionedFanoutWriter<ContentFileT, RowData> {

    private final PartitionKey partitionKey;
    private final RowDataWrapper rowDataWrapper;

    RowDataPartitionedFanoutWriter(PartitionSpec spec, FileFormat format, OutputFileFactory fileFactory, FileIO io,
                                   long targetFileSize, Schema schema, RowType flinkSchema,
                                   ContentFileWriterFactory<ContentFileT, RowData> contentFileWriter) {
      super(format, fileFactory, io, targetFileSize, contentFileWriter);
      this.partitionKey = new PartitionKey(spec, schema);
      this.rowDataWrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
    }

    @Override
    protected PartitionKey partition(RowData row) {
      partitionKey.partition(rowDataWrapper.wrap(row));
      return partitionKey;
    }
  }

  private static class PosPartitionedFanoutWriter<ContentFileT>
      extends PartitionedFanoutWriter<ContentFileT, PositionDelete<RowData>> {

    private final PartitionKey partitionKey;
    private final RowDataWrapper rowDataWrapper;

    PosPartitionedFanoutWriter(PartitionSpec spec, FileFormat format, OutputFileFactory fileFactory, FileIO io,
                               long targetFileSize, Schema schema, RowType flinkSchema,
                               ContentFileWriterFactory<ContentFileT, PositionDelete<RowData>> contentFileWriter) {
      super(format, fileFactory, io, targetFileSize, contentFileWriter);
      this.partitionKey = new PartitionKey(spec, schema);
      this.rowDataWrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
    }

    @Override
    protected PartitionKey partition(PositionDelete<RowData> row) {
      partitionKey.partition(rowDataWrapper.wrap(row.row()));
      return partitionKey;
    }
  }

  public static class FlinkFileAppenderFactory implements FileAppenderFactory<RowData>, Serializable {
    private final Schema schema;
    private final RowType flinkSchema;
    private final Map<String, String> props;

    public FlinkFileAppenderFactory(Schema schema, RowType flinkSchema, Map<String, String> props) {
      this.schema = schema;
      this.flinkSchema = flinkSchema;
      this.props = props;
    }

    @Override
    public FileAppender<RowData> newAppender(OutputFile outputFile, FileFormat format) {
      MetricsConfig metricsConfig = MetricsConfig.fromProperties(props);
      try {
        switch (format) {
          case AVRO:
            return Avro.write(outputFile)
                .createWriterFunc(ignore -> new FlinkAvroWriter(flinkSchema))
                .setAll(props)
                .schema(schema)
                .overwrite()
                .build();

          case ORC:
            return ORC.write(outputFile)
                .createWriterFunc((iSchema, typDesc) -> FlinkOrcWriter.buildWriter(flinkSchema, iSchema))
                .setAll(props)
                .schema(schema)
                .overwrite()
                .build();

          case PARQUET:
            return Parquet.write(outputFile)
                .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(flinkSchema, msgType))
                .setAll(props)
                .metricsConfig(metricsConfig)
                .schema(schema)
                .overwrite()
                .build();

          default:
            throw new UnsupportedOperationException("Cannot write unknown file format: " + format);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
