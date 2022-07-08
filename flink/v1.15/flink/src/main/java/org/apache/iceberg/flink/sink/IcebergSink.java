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

import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.committer.FilesCommittable;
import org.apache.iceberg.flink.sink.committer.FilesCommittableSerializer;
import org.apache.iceberg.flink.sink.committer.FilesCommitter;
import org.apache.iceberg.flink.sink.writer.StreamWriter;
import org.apache.iceberg.flink.sink.writer.StreamWriterState;
import org.apache.iceberg.flink.sink.writer.StreamWriterStateSerializer;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink v2 sink offer different hooks to insert custom topologies into the sink. It includes
 * writers that can write data to files in parallel and route commit info globally to one Committer.
 * Post commit topology will take of compacting the already written files and updating the file log
 * after the compaction.
 *
 * <pre>{@code
 *                            Flink sink
 *               +------------------------------------------------------------------+
 *               |                                                                  |
 * +-------+     | +-------------+       +---------------+                          |
 * | Map 1 | ==> | | writer 1 | =|       | committer 1   |                          |
 * +-------+     | +-------------+       +---------------+                          |
 *               |                 \                                                |
 *               |                  \                                               |
 *               |                   \                                              |
 * +-------+     | +-------------+    \  +---------------+        +---------------+ |
 * | Map 2 | ==> | | writer 2 | =| ---  >| committer 2   |  --->  | post commit   | |
 * +-------+     | +-------------+       +---------------+        +---------------+ |
 *               |                                                                  |
 *               +------------------------------------------------------------------+
 * }</pre>
 */
public class IcebergSink
    implements StatefulSink<RowData, StreamWriterState>,
        WithPreWriteTopology<RowData>,
        WithPreCommitTopology<RowData, FilesCommittable>,
        WithPostCommitTopology<RowData, FilesCommittable> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSink.class);

  private final TableLoader tableLoader;
  private final Table table;
  private final boolean overwrite;
  private final boolean upsertMode;
  private final DistributionMode distributionMode;
  private final List<String> equalityFieldColumns;
  private final List<Integer> equalityFieldIds;
  private final FileFormat dataFileFormat;
  private final int workerPoolSize;
  private final long targetDataFileSize;
  private final String uidPrefix;
  private final Map<String, String> snapshotProperties;
  private final RowType flinkRowType;
  private Committer<FilesCommittable> committer;

  private IcebergSink(
      TableLoader tableLoader,
      @Nullable Table table,
      List<Integer> equalityFieldIds,
      List<String> equalityFieldColumns,
      @Nullable String uidPrefix,
      Map<String, String> snapshotProperties,
      boolean upsertMode,
      boolean overwrite,
      TableSchema tableSchema,
      DistributionMode distributionMode,
      int workerPoolSize,
      long targetDataFileSize,
      FileFormat dataFileFormat) {
    Preconditions.checkNotNull(tableLoader, "Table loader shouldn't be null");
    Preconditions.checkNotNull(table, "Table shouldn't be null");

    this.tableLoader = tableLoader;
    this.table = table;
    this.equalityFieldIds = equalityFieldIds;
    this.equalityFieldColumns = equalityFieldColumns;
    this.uidPrefix = uidPrefix == null ? "" : uidPrefix;
    this.snapshotProperties = snapshotProperties;
    this.flinkRowType = toFlinkRowType(table.schema(), tableSchema);
    this.upsertMode = upsertMode;
    this.overwrite = overwrite;
    this.distributionMode = distributionMode;
    this.workerPoolSize = workerPoolSize;
    this.targetDataFileSize = targetDataFileSize;
    this.dataFileFormat = dataFileFormat;
  }

  @Override
  public DataStream<RowData> addPreWriteTopology(DataStream<RowData> inputDataStream) {
    return distributeDataStream(
        inputDataStream,
        equalityFieldIds,
        table.spec(),
        table.schema(),
        flinkRowType,
        distributionMode,
        equalityFieldColumns);
  }

  @Override
  public StreamWriter createWriter(InitContext context) {
    return restoreWriter(context, Lists.newArrayList());
  }

  @Override
  public StreamWriter restoreWriter(
      InitContext context, Collection<StreamWriterState> recoveredState) {
    RowDataTaskWriterFactory taskWriterFactory =
        new RowDataTaskWriterFactory(
            SerializableTable.copyOf(table),
            flinkRowType,
            targetDataFileSize,
            dataFileFormat,
            equalityFieldIds,
            upsertMode);
    StreamWriter streamWriter =
        new StreamWriter(
            table.name(),
            taskWriterFactory,
            context.getSubtaskId(),
            context.getNumberOfParallelSubtasks());

    return streamWriter.initializeState(recoveredState);
  }

  @Override
  public SimpleVersionedSerializer<StreamWriterState> getWriterStateSerializer() {
    return new StreamWriterStateSerializer();
  }

  @Override
  public DataStream<CommittableMessage<FilesCommittable>> addPreCommitTopology(
      DataStream<CommittableMessage<FilesCommittable>> writeResults) {
    return writeResults
        .map(
            new RichMapFunction<
                CommittableMessage<FilesCommittable>, CommittableMessage<FilesCommittable>>() {
              @Override
              public CommittableMessage<FilesCommittable> map(
                  CommittableMessage<FilesCommittable> message) {
                if (message instanceof CommittableWithLineage) {
                  CommittableWithLineage<FilesCommittable> committableWithLineage =
                      (CommittableWithLineage<FilesCommittable>) message;
                  FilesCommittable committable = committableWithLineage.getCommittable();
                  committable.checkpointId(committableWithLineage.getCheckpointId().orElse(-1));
                  committable.subtaskId(committableWithLineage.getSubtaskId());
                  committable.jobID(getRuntimeContext().getJobId().toString());
                }
                return message;
              }
            })
        .uid(uidPrefix + "-pre-commit-topology")
        .global();
  }

  @Override
  public Committer<FilesCommittable> createCommitter() {
    return committer != null
        ? committer
        : new FilesCommitter(tableLoader, overwrite, snapshotProperties, workerPoolSize);
  }

  @VisibleForTesting
  void setCommitter(Committer<FilesCommittable> committer) {
    this.committer = committer;
  }

  @Override
  public SimpleVersionedSerializer<FilesCommittable> getCommittableSerializer() {
    return new FilesCommittableSerializer();
  }

  @Override
  public void addPostCommitTopology(DataStream<CommittableMessage<FilesCommittable>> committables) {
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
  public static <T> Builder builderFor(
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
  public static Builder forRow(DataStream<Row> input, TableSchema tableSchema) {
    RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();
    DataType[] fieldDataTypes = tableSchema.getFieldDataTypes();

    DataFormatConverters.RowConverter rowConverter =
        new DataFormatConverters.RowConverter(fieldDataTypes);
    return builderFor(input, rowConverter::toInternal, FlinkCompatibilityUtil.toTypeInfo(rowType))
        .tableSchema(tableSchema);
  }

  /**
   * Initialize a {@link Builder} to export the data from input data stream with {@link RowData}s
   * into iceberg table.
   *
   * @param input the source input data stream with {@link RowData}s.
   * @return {@link Builder} to connect the iceberg table.
   */
  public static Builder forRowData(DataStream<RowData> input) {
    return new Builder().forRowData(input);
  }

  /**
   * Initialize a {@link Builder} to export the data from input data stream with {@link RowData}s
   * into iceberg table.
   *
   * @return {@link Builder} to connect the iceberg table.
   */
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Function<String, DataStream<RowData>> inputCreator = null;
    private TableLoader tableLoader;
    private Table table;
    private FlinkWriteConf flinkWriteConf;
    private TableSchema tableSchema;
    private Integer writeParallelism = null;
    private List<String> equalityFieldColumns = null;
    private String uidPrefix = "iceberg-flink-job";
    private final Map<String, String> snapshotProperties = Maps.newHashMap();
    private ReadableConfig readableConfig = new Configuration();
    private final Map<String, String> writeOptions = Maps.newHashMap();

    private Builder() {}

    private Builder forRowData(DataStream<RowData> newRowDataInput) {
      this.inputCreator = ignored -> newRowDataInput;
      return this;
    }

    private <T> Builder forMapperOutputType(
        DataStream<T> input, MapFunction<T, RowData> mapper, TypeInformation<RowData> outputType) {
      this.inputCreator =
          newUidPrefix -> {
            // Input stream order is crucial for some situation (e.g. in cdc case).
            // Therefore, we need to set the parallelismof map operator same as its input to keep
            // map operator chaining its input, and avoid rebalanced by default.
            SingleOutputStreamOperator<RowData> inputStream =
                input.map(mapper, outputType).setParallelism(input.getParallelism());
            if (newUidPrefix != null) {
              inputStream
                  .name(Builder.this.operatorName(newUidPrefix))
                  .uid(newUidPrefix + "-mapper");
            }
            return inputStream;
          };
      return this;
    }

    /**
     * This iceberg {@link Table} instance is used for initializing {@link StreamWriter} which will
     * write all the records into {@link DataFile}s and emit them to downstream operator. Providing
     * a table would avoid so many table loading from each separate task.
     *
     * @param newTable the loaded iceberg table instance.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder table(Table newTable) {
      this.table = newTable;
      return this;
    }

    /**
     * The table loader is used for loading tables in {@link FilesCommitter} lazily, we need this
     * loader because {@link Table} is not serializable and could not just use the loaded table from
     * Builder#table in the remote task manager.
     *
     * @param newTableLoader to load iceberg table inside tasks.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder tableLoader(TableLoader newTableLoader) {
      this.tableLoader = newTableLoader;
      return this;
    }

    /**
     * Set the write properties for Flink sink. View the supported properties in {@link
     * FlinkWriteOptions}
     */
    public Builder set(String property, String value) {
      writeOptions.put(property, value);
      return this;
    }

    /**
     * Set the write properties for Flink sink. View the supported properties in {@link
     * FlinkWriteOptions}
     */
    public Builder setAll(Map<String, String> properties) {
      writeOptions.putAll(properties);
      return this;
    }

    public Builder tableSchema(TableSchema newTableSchema) {
      this.tableSchema = newTableSchema;
      return this;
    }

    public Builder overwrite(boolean newOverwrite) {
      writeOptions.put(FlinkWriteOptions.OVERWRITE_MODE.key(), Boolean.toString(newOverwrite));
      return this;
    }

    public Builder flinkConf(ReadableConfig config) {
      this.readableConfig = config;
      return this;
    }

    /**
     * Configure the write {@link DistributionMode} that the flink sink will use. Currently, flink
     * support {@link DistributionMode#NONE} and {@link DistributionMode#HASH}.
     *
     * @param mode to specify the write distribution mode.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder distributionMode(DistributionMode mode) {
      Preconditions.checkArgument(
          !DistributionMode.RANGE.equals(mode),
          "Flink does not support 'range' write distribution mode now.");
      if (mode != null) {
        writeOptions.put(FlinkWriteOptions.DISTRIBUTION_MODE.key(), mode.modeName());
      }
      return this;
    }

    /**
     * Configuring the write parallel number for iceberg stream writer.
     *
     * @param newWriteParallelism the number of parallel iceberg stream writer.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder writeParallelism(int newWriteParallelism) {
      this.writeParallelism = newWriteParallelism;
      return this;
    }

    /**
     * All INSERT/UPDATE_AFTER events from input stream will be transformed to UPSERT events, which
     * means it will DELETE the old records and then INSERT the new records. In partitioned table,
     * the partition fields should be a subset of equality fields, otherwise the old row that
     * located in partition-A could not be deleted by the new row that located in partition-B.
     *
     * @param enabled indicate whether it should transform all INSERT/UPDATE_AFTER events to UPSERT.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder upsert(boolean enabled) {
      writeOptions.put(FlinkWriteOptions.WRITE_UPSERT_ENABLED.key(), Boolean.toString(enabled));
      return this;
    }

    /**
     * Configuring the equality field columns for iceberg table that accept CDC or UPSERT events.
     *
     * @param columns defines the iceberg table's key.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder equalityFieldColumns(List<String> columns) {
      this.equalityFieldColumns = columns;
      return this;
    }

    /**
     * Set the uid prefix for FlinkSink. Actually operator uid will be appended with a suffix like
     * "uidPrefix-sink". <br>
     * <br>
     * If provided, this prefix is also applied to operator names. <br>
     * <br>
     * Flink auto generates operator uid if not set explicitly. It is a recommended <a
     * href="https://ci.apache.org/projects/flink/flink-docs-master/docs/ops/production_ready/">
     * best-practice to set uid for all operators</a> before deploying to production. Flink has an
     * option to {@code pipeline.auto-generate-uid=false} to disable auto-generation and force
     * explicit setting of all operator uid. <br>
     * <br>
     * Be careful with setting this for an existing job, because now we are changing the operator
     * uid from an auto-generated one to this new value. When deploying the change with a
     * checkpoint, Flink won't be able to restore the previous Flink sink state. You need to use
     * {@code --allowNonRestoredState} to ignore the previous sink state. During restore Flink sink
     * state is used to check if last commit was actually successful or not. {@code
     * --allowNonRestoredState} can lead to data loss if the Iceberg commit failed in the last
     * completed checkpoint.
     *
     * @param newPrefix prefix for Flink sink operator uid and name
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder uidPrefix(String newPrefix) {
      this.uidPrefix = newPrefix == null ? "" : newPrefix;
      return this;
    }

    public Builder setSnapshotProperties(Map<String, String> properties) {
      snapshotProperties.putAll(properties);
      return this;
    }

    public Builder setSnapshotProperty(String property, String value) {
      snapshotProperties.put(property, value);
      return this;
    }

    /** Append the iceberg sink operators to write records to iceberg table. */
    @SuppressWarnings("unchecked")
    public DataStreamSink<Void> append() {
      Preconditions.checkArgument(
          inputCreator != null,
          "Please use forRowData() or forMapperOutputType() to initialize the input DataStream.");
      Preconditions.checkNotNull(tableLoader, "Table loader shouldn't be null");

      DataStream<RowData> rowDataInput = inputCreator.apply(uidPrefix);
      DataStreamSink rowDataDataStreamSink = rowDataInput.sinkTo(build()).uid(uidPrefix + "-sink");
      if (writeParallelism != null) {
        rowDataDataStreamSink.setParallelism(writeParallelism);
      }
      return rowDataDataStreamSink;
    }

    public IcebergSink build() {
      this.table = checkAndGetTable(tableLoader, table);
      List<Integer> equalityFieldIds = checkAndGetEqualityFieldIds(table, equalityFieldColumns);
      this.flinkWriteConf = new FlinkWriteConf(table, writeOptions, readableConfig);
      validateUpsertMode(
          table,
          equalityFieldIds,
          equalityFieldColumns,
          flinkWriteConf.upsertMode(),
          flinkWriteConf.overwriteMode());

      return new IcebergSink(
          tableLoader,
          table,
          equalityFieldIds,
          equalityFieldColumns,
          uidPrefix,
          snapshotProperties,
          flinkWriteConf.upsertMode(),
          flinkWriteConf.overwriteMode(),
          tableSchema,
          flinkWriteConf.distributionMode(),
          flinkWriteConf.workerPoolSize(),
          flinkWriteConf.targetDataFileSize(),
          flinkWriteConf.dataFileFormat());
    }

    private String operatorName(String suffix) {
      return uidPrefix != null ? uidPrefix + "-" + suffix : suffix;
    }
  }

  static RowType toFlinkRowType(Schema schema, TableSchema requestedSchema) {
    if (requestedSchema != null) {
      // Convert the flink schema to iceberg schema firstly, then reassign ids to match the existing
      // iceberg schema.
      Schema writeSchema = TypeUtil.reassignIds(FlinkSchemaUtil.convert(requestedSchema), schema);
      TypeUtil.validateWriteSchema(schema, writeSchema, true, true);

      // We use this flink schema to read values from RowData. The flink's TINYINT and SMALLINT will
      // be promoted to iceberg INTEGER, that means if we use iceberg's table schema to read TINYINT
      // (backend by 1 'byte'), we will read 4 bytes rather than 1 byte, it will mess up the byte
      // array in
      // BinaryRowData. So here we must use flink schema.
      return (RowType) requestedSchema.toRowDataType().getLogicalType();
    } else {
      return FlinkSchemaUtil.convert(schema);
    }
  }

  static DataStream<RowData> distributeDataStream(
      DataStream<RowData> input,
      List<Integer> equalityFieldIds,
      PartitionSpec partitionSpec,
      Schema iSchema,
      RowType flinkRowType,
      DistributionMode distributionMode,
      List<String> equalityFieldColumns) {
    LOG.info("Write distribution mode is '{}'", distributionMode.modeName());
    switch (distributionMode) {
      case NONE:
        if (equalityFieldIds.isEmpty()) {
          return input;
        } else {
          LOG.info("Distribute rows by equality fields, because there are equality fields set");
          return input.keyBy(new EqualityFieldKeySelector(iSchema, flinkRowType, equalityFieldIds));
        }

      case HASH:
        if (equalityFieldIds.isEmpty()) {
          if (partitionSpec.isUnpartitioned()) {
            LOG.warn(
                "Fallback to use 'none' distribution mode, because there are no equality fields set "
                    + "and table is unpartitioned");
            return input;
          } else {
            return input.keyBy(new PartitionKeySelector(partitionSpec, iSchema, flinkRowType));
          }
        } else {
          if (partitionSpec.isUnpartitioned()) {
            LOG.info(
                "Distribute rows by equality fields, because there are equality fields set "
                    + "and table is unpartitioned");
            return input.keyBy(
                new EqualityFieldKeySelector(iSchema, flinkRowType, equalityFieldIds));
          } else {
            for (PartitionField partitionField : partitionSpec.fields()) {
              Preconditions.checkState(
                  equalityFieldIds.contains(partitionField.sourceId()),
                  "In 'hash' distribution mode with equality fields set, partition field '%s' "
                      + "should be included in equality fields: '%s'",
                  partitionField,
                  equalityFieldColumns);
            }
            return input.keyBy(new PartitionKeySelector(partitionSpec, iSchema, flinkRowType));
          }
        }

      case RANGE:
        if (equalityFieldIds.isEmpty()) {
          LOG.warn(
              "Fallback to use 'none' distribution mode, because there are no equality fields set "
                  + "and {}=range is not supported yet in flink",
              WRITE_DISTRIBUTION_MODE);
          return input;
        } else {
          LOG.info(
              "Distribute rows by equality fields, because there are equality fields set "
                  + "and {}=range is not supported yet in flink",
              WRITE_DISTRIBUTION_MODE);
          return input.keyBy(new EqualityFieldKeySelector(iSchema, flinkRowType, equalityFieldIds));
        }

      default:
        throw new RuntimeException(
            "Unrecognized " + WRITE_DISTRIBUTION_MODE + ": " + distributionMode);
    }
  }

  static List<Integer> checkAndGetEqualityFieldIds(Table table, List<String> equalityFieldColumns) {
    List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().identifierFieldIds());
    if (equalityFieldColumns != null && equalityFieldColumns.size() > 0) {
      Set<Integer> equalityFieldSet = Sets.newHashSetWithExpectedSize(equalityFieldColumns.size());
      for (String column : equalityFieldColumns) {
        org.apache.iceberg.types.Types.NestedField field = table.schema().findField(column);
        Preconditions.checkNotNull(
            field,
            "Missing required equality field column '%s' in table schema %s",
            column,
            table.schema());
        equalityFieldSet.add(field.fieldId());
      }

      if (!equalityFieldSet.equals(table.schema().identifierFieldIds())) {
        LOG.warn(
            "The configured equality field column IDs {} are not matched with the schema identifier field IDs"
                + " {}, use job specified equality field columns as the equality fields by default.",
            equalityFieldSet,
            table.schema().identifierFieldIds());
      }

      equalityFieldIds = Lists.newArrayList(equalityFieldSet);
    }
    return equalityFieldIds;
  }

  static Table checkAndGetTable(TableLoader tableLoader, Table table) {
    if (table == null) {
      tableLoader.open();
      try (TableLoader loader = tableLoader) {
        return SerializableTable.copyOf(loader.loadTable());
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Failed to load iceberg table from table loader: " + tableLoader, e);
      }
    }
    return SerializableTable.copyOf(table);
  }

  static void validateUpsertMode(
      Table table,
      List<Integer> equalityFieldIds,
      List<String> equalityFieldColumns,
      boolean upsertMode,
      boolean overwriteMode) {
    // Validate the equality fields and partition fields if we enable the upsert mode.
    if (upsertMode) {
      Preconditions.checkState(
          !overwriteMode,
          "OVERWRITE mode shouldn't be enable when configuring to use UPSERT data stream.");
      Preconditions.checkState(
          !equalityFieldIds.isEmpty(),
          "Equality field columns shouldn't be empty when configuring to use UPSERT data stream.");
      if (!table.spec().isUnpartitioned()) {
        for (PartitionField partitionField : table.spec().fields()) {
          Preconditions.checkState(
              equalityFieldIds.contains(partitionField.sourceId()),
              "In UPSERT mode, partition field '%s' should be included in equality fields: '%s'",
              partitionField,
              equalityFieldColumns);
        }
      }
    }
  }
}
