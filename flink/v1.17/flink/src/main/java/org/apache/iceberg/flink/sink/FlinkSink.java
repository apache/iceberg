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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.sink.committer.IcebergFilesCommitter;
import org.apache.iceberg.flink.sink.writer.IcebergStreamWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.util.SerializableSupplier;

public class FlinkSink extends SinkBase {
  private static final String ICEBERG_STREAM_WRITER_NAME =
      IcebergStreamWriter.class.getSimpleName();
  private static final String ICEBERG_FILES_COMMITTER_NAME =
      IcebergFilesCommitter.class.getSimpleName();

  private FlinkSink(SinkBuilder builder) {
    super(builder);
  }

  /**
   * Initialize a {@link SinkBuilder} to export the data from generic input data stream into iceberg
   * table. We use {@link RowData} inside the sink connector, so users need to provide a mapper
   * function and a {@link TypeInformation} to convert those generic records to a RowData
   * DataStream.
   *
   * @param input the generic source input data stream.
   * @param mapper function to convert the generic data to {@link RowData}
   * @param outputType to define the {@link TypeInformation} for the input data.
   * @param <T> the data type of records.
   * @return {@link SinkBuilder} to connect the iceberg table.
   */
  public static <T> SinkBuilder builderFor(
      DataStream<T> input, MapFunction<T, RowData> mapper, TypeInformation<RowData> outputType) {
    return new Builder().forMapperOutputType(input, mapper, outputType);
  }

  /**
   * Initialize a {@link SinkBuilder} to export the data from input data stream with {@link Row}s
   * into iceberg table. We use {@link RowData} inside the sink connector, so users need to provide
   * a {@link TableSchema} for builder to convert those {@link Row}s to a {@link RowData}
   * DataStream.
   *
   * @param input the source input data stream with {@link Row}s.
   * @param tableSchema defines the {@link TypeInformation} for input data.
   * @return {@link SinkBuilder} to connect the iceberg table.
   */
  public static SinkBuilder forRow(DataStream<Row> input, TableSchema tableSchema) {
    return new Builder().forRow(input, tableSchema);
  }

  /**
   * Initialize a {@link SinkBuilder} to export the data from input data stream with {@link
   * RowData}s into iceberg table.
   *
   * @param input the source input data stream with {@link RowData}s.
   * @return {@link SinkBuilder} to connect the iceberg table.
   */
  public static SinkBuilder forRowData(DataStream<RowData> input) {
    return new Builder().forRowData(input);
  }

  public static class Builder extends SinkBuilder {
    private Builder() {}

    @Override
    public DataStreamSink<Void> append() {
      return chainIcebergOperators();
    }

    @Override
    @VisibleForTesting
    FlinkSink build() {
      FlinkSink sink = new FlinkSink(this);
      sink.validate();
      sink.init();
      return sink;
    }

    private <T> DataStreamSink<T> chainIcebergOperators() {
      FlinkSink sink = build();

      DataStream<RowData> rowDataInput = inputCreator().apply(uidPrefix());

      // Distribute the records from input data stream based on the write.distribution-mode and
      // equality fields.
      DataStream<RowData> distributeStream = sink.distributeDataStream(rowDataInput);

      // Add parallel writers that append rows to files
      SingleOutputStreamOperator<WriteResult> writerStream =
          appendWriter(
              distributeStream,
              sink.tableSupplier(),
              sink.flinkWriteConf(),
              sink.writeProperties(),
              sink.flinkRowType(),
              sink.equalityFieldIds());

      // Add single-parallelism committer that commits files
      // after successful checkpoint or end of input
      SingleOutputStreamOperator<Void> committerStream =
          appendCommitter(writerStream, sink.flinkWriteConf());

      // Add dummy discard sink
      return appendDummySink(committerStream);
    }

    @SuppressWarnings("unchecked")
    private <T> DataStreamSink<T> appendDummySink(
        SingleOutputStreamOperator<Void> committerStream) {
      DataStreamSink<T> resultStream =
          committerStream
              .addSink(new DiscardingSink())
              .name(operatorName(String.format("IcebergSink %s", this.table().name())))
              .setParallelism(1);
      if (uidPrefix() != null) {
        resultStream = resultStream.uid(uidPrefix() + "-dummysink");
      }
      return resultStream;
    }

    private SingleOutputStreamOperator<Void> appendCommitter(
        SingleOutputStreamOperator<WriteResult> writerStream, FlinkWriteConf flinkWriteConf) {
      IcebergFilesCommitter filesCommitter =
          new IcebergFilesCommitter(
              tableLoader(),
              flinkWriteConf.overwriteMode(),
              snapshotProperties(),
              flinkWriteConf.workerPoolSize(),
              flinkWriteConf.branch());
      SingleOutputStreamOperator<Void> committerStream =
          writerStream
              .transform(operatorName(ICEBERG_FILES_COMMITTER_NAME), Types.VOID, filesCommitter)
              .setParallelism(1)
              .setMaxParallelism(1);
      if (uidPrefix() != null) {
        committerStream = committerStream.uid(uidPrefix() + "-committer");
      }
      return committerStream;
    }

    private SingleOutputStreamOperator<WriteResult> appendWriter(
        DataStream<RowData> input,
        SerializableSupplier<Table> tableSupplier,
        FlinkWriteConf flinkWriteConf,
        Map<String, String> writeProperties,
        RowType flinkRowType,
        List<Integer> equalityFieldIds) {

      IcebergStreamWriter<RowData> streamWriter =
          IcebergStreamWriter.createStreamWriter(
              tableSupplier, flinkWriteConf, writeProperties, flinkRowType, equalityFieldIds);

      int parallelism =
          flinkWriteConf.writeParallelism() == null
              ? input.getParallelism()
              : flinkWriteConf.writeParallelism();
      SingleOutputStreamOperator<WriteResult> writerStream =
          input
              .transform(
                  operatorName(ICEBERG_STREAM_WRITER_NAME),
                  TypeInformation.of(WriteResult.class),
                  streamWriter)
              .setParallelism(parallelism);
      if (uidPrefix() != null) {
        writerStream = writerStream.uid(uidPrefix() + "-writer");
      }
      return writerStream;
    }
  }
}
