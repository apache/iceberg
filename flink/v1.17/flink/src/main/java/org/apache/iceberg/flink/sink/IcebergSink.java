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

import java.util.Map;
import java.util.UUID;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.committer.CommonCommitter;
import org.apache.iceberg.flink.sink.committer.SinkV2Aggregator;
import org.apache.iceberg.flink.sink.committer.SinkV2Committable;
import org.apache.iceberg.flink.sink.committer.SinkV2CommittableSerializer;
import org.apache.iceberg.flink.sink.committer.SinkV2Committer;
import org.apache.iceberg.flink.sink.writer.IcebergStreamWriterMetrics;
import org.apache.iceberg.flink.sink.writer.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.writer.SinkV2Writer;

/**
 * Flink v2 sink offer different hooks to insert custom topologies into the sink. We will use the
 * following:
 *
 * <ul>
 *   <li>{@link WithPreWriteTopology} which redistributes the data to the writers based on the
 *       {@link DistributionMode}
 *   <li>{@link org.apache.flink.api.connector.sink2.SinkWriter} which writes data files, and
 *       generates a {@link SinkV2Committable} to store the {@link
 *       org.apache.iceberg.io.WriteResult}
 *   <li>{@link WithPreCommitTopology} which we use to to place the {@link SinkV2Aggregator} which
 *       merges the individual {@link org.apache.flink.api.connector.sink2.SinkWriter}'s {@link
 *       org.apache.iceberg.io.WriteResult}s to a single {@link
 *       org.apache.iceberg.flink.sink.committer.DeltaManifests}
 *   <li>{@link Committer} which stores the incoming {@link
 *       org.apache.iceberg.flink.sink.committer.DeltaManifests}s in state for recovery, and commits
 *       them to the Iceberg table using the {@link CommonCommitter}
 *   <li>{@link WithPostCommitTopology} we could use for incremental compaction later. This is not
 *       implemented yet.
 * </ul>
 *
 * The job graph looks like below:
 *
 * <pre>{@code
 *                            Flink sink
 *               +-----------------------------------------------------------------------------------+
 *               |                                                                                   |
 * +-------+     | +----------+                               +-------------+      +---------------+ |
 * | Map 1 | ==> | | writer 1 |                               | committer 1 | ---> | post commit 1 | |
 * +-------+     | +----------+                               +-------------+      +---------------+ |
 *               |             \                             /                \                      |
 *               |              \                           /                  \                     |
 *               |               \                         /                    \                    |
 * +-------+     | +----------+   \ +-------------------+ /   +-------------+    \ +---------------+ |
 * | Map 2 | ==> | | writer 2 | --->| commit aggregator |     | committer 2 |      | post commit 2 | |
 * +-------+     | +----------+     +-------------------+     +-------------+      +---------------+ |
 *               |                                             Commit only on                        |
 *               |                                             committer 1                           |
 *               +-----------------------------------------------------------------------------------+
 * }</pre>
 */
public class IcebergSink extends SinkBase
    implements WithPreWriteTopology<RowData>,
        WithPreCommitTopology<RowData, SinkV2Committable>,
        WithPostCommitTopology<RowData, SinkV2Committable> {
  private Table initTable;
  private final TableLoader tableLoader;
  private final Map<String, String> snapshotProperties;
  private String uidPrefix;
  private Committer<SinkV2Committable> sinkV2Committer;
  private final String sinkId;
  private CommonCommitter commonCommitter;

  // FlinkWriteConf properties stored here for serialization
  private boolean upsertMode;
  private FileFormat dataFileFormat;
  private long targetDataFileSize;

  private IcebergSink(SinkBuilder builder) {
    super(builder);

    this.tableLoader = builder.tableLoader();
    this.snapshotProperties = builder.snapshotProperties();

    // We generate a random UUID every time when a sink is created.
    // This is used to separate files generated by different sinks writing the same table.
    // Also used to generate the aggregator operator name
    this.sinkId = UUID.randomUUID().toString();
  }

  @Override
  void init() {
    super.init();
    this.initTable = tableSupplier().get();
    this.uidPrefix = builder().uidPrefix() == null ? "" : builder().uidPrefix();

    // FlinkWriteConf properties needed to be set separately, so we do not have to serialize the
    // full conf
    this.upsertMode = flinkWriteConf().upsertMode();
    this.dataFileFormat = flinkWriteConf().dataFileFormat();
    this.targetDataFileSize = flinkWriteConf().targetDataFileSize();

    this.commonCommitter =
        new CommonCommitter(
            tableLoader,
            flinkWriteConf().branch(),
            snapshotProperties,
            flinkWriteConf().overwriteMode(),
            flinkWriteConf().workerPoolSize(),
            sinkId);
  }

  @Override
  public DataStream<RowData> addPreWriteTopology(DataStream<RowData> inputDataStream) {
    return distributeDataStream(inputDataStream);
  }

  @Override
  public SinkV2Writer createWriter(InitContext context) {
    RowDataTaskWriterFactory taskWriterFactory =
        new RowDataTaskWriterFactory(
            tableSupplier(),
            flinkRowType(),
            targetDataFileSize,
            dataFileFormat,
            writeProperties(),
            equalityFieldIds(),
            upsertMode);
    IcebergStreamWriterMetrics metrics =
        new IcebergStreamWriterMetrics(context.metricGroup(), initTable.name());
    return new SinkV2Writer(
        tableSupplier(),
        taskWriterFactory,
        metrics,
        context.getSubtaskId(),
        context.getAttemptNumber());
  }

  @Override
  public DataStream<CommittableMessage<SinkV2Committable>> addPreCommitTopology(
      DataStream<CommittableMessage<SinkV2Committable>> writeResults) {
    TypeInformation<CommittableMessage<SinkV2Committable>> typeInformation =
        CommittableMessageTypeInfo.of(this::getCommittableSerializer);
    return writeResults
        .global()
        .transform(
            prefixIfNotNull(uidPrefix, initTable.name() + "-" + sinkId + "-pre-commit-topology"),
            typeInformation,
            new SinkV2Aggregator(commonCommitter))
        .uid(prefixIfNotNull(uidPrefix, "pre-commit-topology"))
        .setParallelism(1)
        .setMaxParallelism(1)
        .global();
  }

  @Override
  public Committer<SinkV2Committable> createCommitter() {
    return sinkV2Committer != null ? sinkV2Committer : new SinkV2Committer(commonCommitter);
  }

  /**
   * Sets the currently used committer. Should be used for testing purposes only. Needs to be public
   * as {@link IcebergSink} is used to test the {@link SinkV2Committer} functionality.
   *
   * @param newCommitter The committer used for testing
   */
  public void setCommitter(Committer<SinkV2Committable> newCommitter) {
    this.sinkV2Committer = newCommitter;
  }

  @Override
  public SimpleVersionedSerializer<SinkV2Committable> getCommittableSerializer() {
    return new SinkV2CommittableSerializer();
  }

  @Override
  public void addPostCommitTopology(
      DataStream<CommittableMessage<SinkV2Committable>> committables) {
    // TODO Support small file compaction
  }

  /**
   * Initialize a {@link Builder} to export the data from generic input data stream into iceberg
   * table. We use {@link RowData} inside the sink connector, so users need to provide a mapper
   * function and a {@link TypeInformation} to convert those generic records to a RowData
   * DataStream.
   *
   * @param input the generic source input data stream.
   * @param mapper function to convert the generic data to {@link RowData}
   * @param outputType to define the {@link TypeInformation} for the input data.
   * @param <T> the data type of records.
   * @return {@link Builder} to connect the iceberg table.
   */
  public static <T> SinkBuilder builderFor(
      DataStream<T> input, MapFunction<T, RowData> mapper, TypeInformation<RowData> outputType) {
    return new Builder().forMapperOutputType(input, mapper, outputType);
  }

  /**
   * Initialize a {@link Builder} to export the data from input data stream with {@link Row}s into
   * iceberg table. We use {@link RowData} inside the sink connector, so users need to provide a
   * {@link TableSchema} for builder to convert those {@link Row}s to a {@link RowData} DataStream.
   *
   * @param input the source input data stream with {@link Row}s.
   * @param tableSchema defines the {@link TypeInformation} for input data.
   * @return {@link Builder} to connect the iceberg table.
   */
  public static SinkBuilder forRow(DataStream<Row> input, TableSchema tableSchema) {
    return new Builder().forRow(input, tableSchema);
  }

  /**
   * Initialize a {@link Builder} to export the data from input data stream with {@link RowData}s
   * into iceberg table.
   *
   * @param input the source input data stream with {@link RowData}s.
   * @return {@link Builder} to connect the iceberg table.
   */
  public static SinkBuilder forRowData(DataStream<RowData> input) {
    return new Builder().forRowData(input);
  }

  public static class Builder extends SinkBuilder {
    private Builder() {}

    @Override
    public DataStreamSink<RowData> append() {
      IcebergSink sink = build();

      DataStream<RowData> rowDataInput = inputCreator().apply(uidPrefix());
      DataStreamSink<RowData> rowDataDataStreamSink =
          rowDataInput.sinkTo(sink).uid(uidPrefix() + "-sink");
      if (sink.flinkWriteConf().writeParallelism() != null) {
        rowDataDataStreamSink.setParallelism(sink.flinkWriteConf().writeParallelism());
      }
      return rowDataDataStreamSink;
    }

    @Override
    public IcebergSink build() {
      IcebergSink sink = new IcebergSink(this);
      sink.validate();
      sink.init();
      return sink;
    }
  }
}
