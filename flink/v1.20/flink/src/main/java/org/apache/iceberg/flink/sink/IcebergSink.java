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

import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION_STRATEGY;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.SupportsPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
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
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink v2 sink offer different hooks to insert custom topologies into the sink. We will use the
 * following:
 *
 * <ul>
 *   <li>{@link SupportsPreWriteTopology} which redistributes the data to the writers based on the
 *       {@link DistributionMode}
 *   <li>{@link org.apache.flink.api.connector.sink2.SinkWriter} which writes data/delete files, and
 *       generates the {@link org.apache.iceberg.io.WriteResult} objects for the files
 *   <li>{@link SupportsPreCommitTopology} which we use to place the {@link
 *       org.apache.iceberg.flink.sink.IcebergWriteAggregator} which merges the individual {@link
 *       org.apache.flink.api.connector.sink2.SinkWriter}'s {@link
 *       org.apache.iceberg.io.WriteResult}s to a single {@link
 *       org.apache.iceberg.flink.sink.IcebergCommittable}
 *   <li>{@link org.apache.iceberg.flink.sink.IcebergCommitter} which commits the incoming{@link
 *       org.apache.iceberg.flink.sink.IcebergCommittable}s to the Iceberg table
 *   <li>{@link SupportsPostCommitTopology} we could use for incremental compaction later. This is
 *       not implemented yet.
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
@Experimental
public class IcebergSink
    implements Sink<RowData>,
        SupportsPreWriteTopology<RowData>,
        SupportsCommitter<IcebergCommittable>,
        SupportsPreCommitTopology<WriteResult, IcebergCommittable>,
        SupportsPostCommitTopology<IcebergCommittable> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSink.class);
  private final TableLoader tableLoader;
  private final Map<String, String> snapshotProperties;
  private final String uidSuffix;
  private final String sinkId;
  private final Map<String, String> writeProperties;
  private final RowType flinkRowType;
  private final SerializableSupplier<Table> tableSupplier;
  private final transient FlinkWriteConf flinkWriteConf;
  private final List<Integer> equalityFieldIds;
  private final boolean upsertMode;
  private final FileFormat dataFileFormat;
  private final long targetDataFileSize;
  private final String branch;
  private final boolean overwriteMode;
  private final int workerPoolSize;

  private final Table table;
  private final List<String> equalityFieldColumns = null;

  private IcebergSink(
      TableLoader tableLoader,
      Table table,
      Map<String, String> snapshotProperties,
      String uidSuffix,
      Map<String, String> writeProperties,
      RowType flinkRowType,
      SerializableSupplier<Table> tableSupplier,
      FlinkWriteConf flinkWriteConf,
      List<Integer> equalityFieldIds,
      String branch,
      boolean overwriteMode) {
    this.tableLoader = tableLoader;
    this.snapshotProperties = snapshotProperties;
    this.uidSuffix = uidSuffix;
    this.writeProperties = writeProperties;
    this.flinkRowType = flinkRowType;
    this.tableSupplier = tableSupplier;
    this.flinkWriteConf = flinkWriteConf;
    this.equalityFieldIds = equalityFieldIds;
    this.branch = branch;
    this.overwriteMode = overwriteMode;
    this.table = table;
    this.upsertMode = flinkWriteConf.upsertMode();
    this.dataFileFormat = flinkWriteConf.dataFileFormat();
    this.targetDataFileSize = flinkWriteConf.targetDataFileSize();
    this.workerPoolSize = flinkWriteConf.workerPoolSize();
    // We generate a random UUID every time when a sink is created.
    // This is used to separate files generated by different sinks writing the same table.
    // Also used to generate the aggregator operator name
    this.sinkId = UUID.randomUUID().toString();
  }

  @Override
  public SinkWriter<RowData> createWriter(InitContext context) {
    RowDataTaskWriterFactory taskWriterFactory =
        new RowDataTaskWriterFactory(
            tableSupplier,
            flinkRowType,
            targetDataFileSize,
            dataFileFormat,
            writeProperties,
            equalityFieldIds,
            upsertMode);
    IcebergStreamWriterMetrics metrics =
        new IcebergStreamWriterMetrics(context.metricGroup(), table.name());
    return new IcebergSinkWriter(
        tableSupplier.get().name(),
        taskWriterFactory,
        metrics,
        context.getSubtaskId(),
        context.getAttemptNumber());
  }

  @Override
  public Committer<IcebergCommittable> createCommitter(CommitterInitContext context) {
    IcebergFilesCommitterMetrics metrics =
        new IcebergFilesCommitterMetrics(context.metricGroup(), table.name());
    return new IcebergCommitter(
        tableLoader, branch, snapshotProperties, overwriteMode, workerPoolSize, sinkId, metrics);
  }

  @Override
  public SimpleVersionedSerializer<IcebergCommittable> getCommittableSerializer() {
    return new IcebergCommittableSerializer();
  }

  @Override
  public void addPostCommitTopology(
      DataStream<CommittableMessage<IcebergCommittable>> committables) {
    // TODO Support small file compaction
  }

  @Override
  public DataStream<RowData> addPreWriteTopology(DataStream<RowData> inputDataStream) {
    return distributeDataStream(inputDataStream);
  }

  @Override
  public DataStream<CommittableMessage<IcebergCommittable>> addPreCommitTopology(
      DataStream<CommittableMessage<WriteResult>> writeResults) {
    TypeInformation<CommittableMessage<IcebergCommittable>> typeInformation =
        CommittableMessageTypeInfo.of(this::getCommittableSerializer);

    String suffix = defaultSuffix(uidSuffix, table.name());
    String preCommitAggregatorUid = String.format("Sink pre-commit aggregator: %s", suffix);

    // global forces all output records send to subtask 0 of the downstream committer operator.
    // This is to ensure commit only happen in one committer subtask.
    // Once upstream Flink provides the capability of setting committer operator
    // parallelism to 1, this can be removed.
    return writeResults
        .global()
        .transform(preCommitAggregatorUid, typeInformation, new IcebergWriteAggregator(tableLoader))
        .uid(preCommitAggregatorUid)
        .setParallelism(1)
        .setMaxParallelism(1)
        // global forces all output records send to subtask 0 of the downstream committer operator.
        // This is to ensure commit only happen in one committer subtask.
        // Once upstream Flink provides the capability of setting committer operator
        // parallelism to 1, this can be removed.
        .global();
  }

  @Override
  public SimpleVersionedSerializer<WriteResult> getWriteResultSerializer() {
    return new WriteResultSerializer();
  }

  public static class Builder implements BaseIcebergSinkBuilder {
    private TableLoader tableLoader;
    private String uidSuffix = "";
    private Function<String, DataStream<RowData>> inputCreator = null;
    private TableSchema tableSchema;
    private SerializableTable table;
    private final Map<String, String> writeOptions = Maps.newHashMap();
    private final Map<String, String> snapshotSummary = Maps.newHashMap();
    private ReadableConfig readableConfig = new Configuration();
    private List<String> equalityFieldColumns = null;

    private Builder() {}

    private Builder forRowData(DataStream<RowData> newRowDataInput) {
      this.inputCreator = ignored -> newRowDataInput;
      return this;
    }

    private Builder forRow(DataStream<Row> input, TableSchema inputTableSchema) {
      RowType rowType = (RowType) inputTableSchema.toRowDataType().getLogicalType();
      DataType[] fieldDataTypes = inputTableSchema.getFieldDataTypes();

      DataFormatConverters.RowConverter rowConverter =
          new DataFormatConverters.RowConverter(fieldDataTypes);
      return forMapperOutputType(
              input, rowConverter::toInternal, FlinkCompatibilityUtil.toTypeInfo(rowType))
          .tableSchema(inputTableSchema);
    }

    private <T> Builder forMapperOutputType(
        DataStream<T> input, MapFunction<T, RowData> mapper, TypeInformation<RowData> outputType) {
      this.inputCreator =
          newUidSuffix -> {
            // Input stream order is crucial for some situation(e.g. in cdc case). Therefore, we
            // need to set the parallelism of map operator same as its input to keep map operator
            // chaining its input, and avoid rebalanced by default.
            SingleOutputStreamOperator<RowData> inputStream =
                input.map(mapper, outputType).setParallelism(input.getParallelism());
            if (newUidSuffix != null) {
              String uid = String.format("Sink pre-writer mapper: %s", newUidSuffix);
              inputStream.name(uid).uid(uid);
            }
            return inputStream;
          };
      return this;
    }

    /**
     * This iceberg {@link SerializableTable} instance is used for initializing {@link
     * IcebergStreamWriter} which will write all the records into {@link DataFile}s and emit them to
     * downstream operator. Providing a table would avoid so many table loading from each separate
     * task.
     *
     * @param newTable the loaded iceberg table instance.
     * @return {@link IcebergSink.Builder} to connect the iceberg table.
     */
    @Override
    public Builder table(Table newTable) {
      this.table = (SerializableTable) SerializableTable.copyOf(newTable);
      return this;
    }

    /**
     * The table loader is used for loading tables in {@link
     * org.apache.iceberg.flink.sink.IcebergCommitter} lazily, we need this loader because {@link
     * Table} is not serializable and could not just use the loaded table from Builder#table in the
     * remote task manager.
     *
     * @param newTableLoader to load iceberg table inside tasks.
     * @return {@link Builder} to connect the iceberg table.
     */
    @Override
    public Builder tableLoader(TableLoader newTableLoader) {
      this.tableLoader = newTableLoader;
      return this;
    }

    TableLoader tableLoader() {
      return tableLoader;
    }

    /**
     * Set the write properties for IcebergSink. View the supported properties in {@link
     * FlinkWriteOptions}
     */
    public Builder set(String property, String value) {
      writeOptions.put(property, value);
      return this;
    }

    /**
     * Set the write properties for IcebergSink. View the supported properties in {@link
     * FlinkWriteOptions}
     */
    @Override
    public Builder setAll(Map<String, String> properties) {
      writeOptions.putAll(properties);
      return this;
    }

    @Override
    public Builder tableSchema(TableSchema newTableSchema) {
      this.tableSchema = newTableSchema;
      return this;
    }

    @Override
    public Builder overwrite(boolean newOverwrite) {
      writeOptions.put(FlinkWriteOptions.OVERWRITE_MODE.key(), Boolean.toString(newOverwrite));
      return this;
    }

    @Override
    public Builder flinkConf(ReadableConfig config) {
      this.readableConfig = config;
      return this;
    }

    /**
     * Configure the write {@link DistributionMode} that the IcebergSink will use. Currently, flink
     * support {@link DistributionMode#NONE} and {@link DistributionMode#HASH}.
     *
     * @param mode to specify the write distribution mode.
     * @return {@link IcebergSink.Builder} to connect the iceberg table.
     */
    @Override
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
     * @return {@link IcebergSink.Builder} to connect the iceberg table.
     */
    @Override
    public Builder writeParallelism(int newWriteParallelism) {
      writeOptions.put(
          FlinkWriteOptions.WRITE_PARALLELISM.key(), Integer.toString(newWriteParallelism));
      return this;
    }

    /**
     * All INSERT/UPDATE_AFTER events from input stream will be transformed to UPSERT events, which
     * means it will DELETE the old records and then INSERT the new records. In partitioned table,
     * the partition fields should be a subset of equality fields, otherwise the old row that
     * located in partition-A could not be deleted by the new row that located in partition-B.
     *
     * @param enabled indicate whether it should transform all INSERT/UPDATE_AFTER events to UPSERT.
     * @return {@link IcebergSink.Builder} to connect the iceberg table.
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
    @Override
    public Builder equalityFieldColumns(List<String> columns) {
      this.equalityFieldColumns = columns;
      return this;
    }

    /**
     * Set the uid suffix for IcebergSink operators. Note that IcebergSink internally consists of
     * multiple operators (like writer, committer, aggregator). Actual operator uid will be appended
     * with a suffix like "Sink Committer: $uidSuffix".
     *
     * <p>Flink auto generates operator uid if not set explicitly. It is a recommended <a
     * href="https://ci.apache.org/projects/flink/flink-docs-master/docs/ops/production_ready/">
     * best-practice to set uid for all operators</a> before deploying to production. Flink has an
     * option to {@code pipeline.auto-generate-uid=false} to disable auto-generation and force
     * explicit setting of all operator uid.
     *
     * <p>Be careful with setting this for an existing job, because now we are changing the operator
     * uid from an auto-generated one to this new value. When deploying the change with a
     * checkpoint, Flink won't be able to restore the previous IcebergSink operator state (more
     * specifically the committer operator state). You need to use {@code --allowNonRestoredState}
     * to ignore the previous sink state. During restore IcebergSink state is used to check if last
     * commit was actually successful or not. {@code --allowNonRestoredState} can lead to data loss
     * if the Iceberg commit failed in the last completed checkpoint.
     *
     * @param newSuffix suffix for Flink sink operator uid and name
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder uidSuffix(String newSuffix) {
      this.uidSuffix = newSuffix;
      return this;
    }

    public Builder snapshotProperties(Map<String, String> properties) {
      snapshotSummary.putAll(properties);
      return this;
    }

    public Builder setSnapshotProperty(String property, String value) {
      snapshotSummary.put(property, value);
      return this;
    }

    @Override
    public Builder toBranch(String branch) {
      writeOptions.put(FlinkWriteOptions.BRANCH.key(), branch);
      return this;
    }

    IcebergSink build() {

      Preconditions.checkArgument(
          inputCreator != null,
          "Please use forRowData() or forMapperOutputType() to initialize the input DataStream.");
      Preconditions.checkNotNull(tableLoader(), "Table loader shouldn't be null");

      // Set the table if it is not yet set in the builder, so we can do the equalityId checks
      SerializableTable serializableTable = checkAndGetTable(tableLoader(), table);
      this.table = serializableTable;
      // Init the `flinkWriteConf` here, so we can do the checks
      FlinkWriteConf flinkWriteConf = new FlinkWriteConf(table, writeOptions, readableConfig);

      Duration tableRefreshInterval = flinkWriteConf.tableRefreshInterval();
      SerializableSupplier<Table> tableSupplier;
      if (tableRefreshInterval != null) {
        tableSupplier = new CachingTableSupplier(table, tableLoader(), tableRefreshInterval);
      } else {
        tableSupplier = () -> serializableTable;
      }

      boolean overwriteMode = flinkWriteConf.overwriteMode();

      // Validate the equality fields and partition fields if we enable the upsert mode.
      List<Integer> equalityFieldIds =
          SinkUtil.checkAndGetEqualityFieldIds(table, equalityFieldColumns);

      if (flinkWriteConf.upsertMode()) {
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

      return new IcebergSink(
          tableLoader,
          table,
          snapshotSummary,
          uidSuffix,
          writeProperties(table, flinkWriteConf.dataFileFormat(), flinkWriteConf),
          toFlinkRowType(table.schema(), tableSchema),
          tableSupplier,
          flinkWriteConf,
          equalityFieldIds,
          flinkWriteConf.branch(),
          overwriteMode);
    }

    /**
     * Append the iceberg sink operators to write records to iceberg table.
     *
     * @return {@link DataStreamSink} for sink.
     */
    @Override
    public DataStreamSink<RowData> append() {
      IcebergSink sink = build();
      String suffix = defaultSuffix(uidSuffix, table.name());
      DataStream<RowData> rowDataInput = inputCreator.apply(suffix);
      // Please note that V2 sink framework will apply the uid here to the framework created
      // operators like writer,
      // committer. E.g. "Sink writer: <uidSuffix>
      DataStreamSink<RowData> rowDataDataStreamSink =
          rowDataInput.sinkTo(sink).uid(suffix).name(suffix);

      // Note that IcebergSink internally consists o multiple operators (like writer, committer,
      // aggregator).
      // The following parallelism will be propagated to all of the above operators.
      if (sink.flinkWriteConf.writeParallelism() != null) {
        rowDataDataStreamSink.setParallelism(sink.flinkWriteConf.writeParallelism());
      }
      return rowDataDataStreamSink;
    }
  }

  private static String defaultSuffix(String uidSuffix, String defaultSuffix) {
    if (uidSuffix == null || uidSuffix.isEmpty()) {
      return defaultSuffix;
    }
    return uidSuffix;
  }

  private static SerializableTable checkAndGetTable(TableLoader tableLoader, Table table) {
    if (table == null) {
      if (!tableLoader.isOpen()) {
        tableLoader.open();
      }

      try (TableLoader loader = tableLoader) {
        return (SerializableTable) SerializableTable.copyOf(loader.loadTable());
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Failed to load iceberg table from table loader: " + tableLoader, e);
      }
    }

    return (SerializableTable) SerializableTable.copyOf(table);
  }

  private static RowType toFlinkRowType(Schema schema, TableSchema requestedSchema) {
    if (requestedSchema != null) {
      // Convert the flink schema to iceberg schema firstly, then reassign ids to match the existing
      // iceberg schema.
      Schema writeSchema = TypeUtil.reassignIds(FlinkSchemaUtil.convert(requestedSchema), schema);
      TypeUtil.validateWriteSchema(schema, writeSchema, true, true);

      // We use this flink schema to read values from RowData. The flink's TINYINT and SMALLINT will
      // be promoted to iceberg INTEGER, that means if we use iceberg's table schema to read TINYINT
      // (backend by 1 'byte'), we will read 4 bytes rather than 1 byte, it will mess up the byte
      // array in BinaryRowData. So here we must use flink schema.
      return (RowType) requestedSchema.toRowDataType().getLogicalType();
    } else {
      return FlinkSchemaUtil.convert(schema);
    }
  }

  /**
   * Based on the {@link FileFormat} overwrites the table level compression properties for the table
   * write.
   *
   * @param table The table to get the table level settings
   * @param format The FileFormat to use
   * @param conf The write configuration
   * @return The properties to use for writing
   */
  private static Map<String, String> writeProperties(
      Table table, FileFormat format, FlinkWriteConf conf) {
    Map<String, String> writeProperties = Maps.newHashMap(table.properties());

    switch (format) {
      case PARQUET:
        writeProperties.put(PARQUET_COMPRESSION, conf.parquetCompressionCodec());
        String parquetCompressionLevel = conf.parquetCompressionLevel();
        if (parquetCompressionLevel != null) {
          writeProperties.put(PARQUET_COMPRESSION_LEVEL, parquetCompressionLevel);
        }

        break;
      case AVRO:
        writeProperties.put(AVRO_COMPRESSION, conf.avroCompressionCodec());
        String avroCompressionLevel = conf.avroCompressionLevel();
        if (avroCompressionLevel != null) {
          writeProperties.put(AVRO_COMPRESSION_LEVEL, conf.avroCompressionLevel());
        }

        break;
      case ORC:
        writeProperties.put(ORC_COMPRESSION, conf.orcCompressionCodec());
        writeProperties.put(ORC_COMPRESSION_STRATEGY, conf.orcCompressionStrategy());
        break;
      default:
        throw new IllegalArgumentException(String.format("Unknown file format %s", format));
    }

    return writeProperties;
  }

  private DataStream<RowData> distributeDataStream(DataStream<RowData> input) {
    DistributionMode mode = flinkWriteConf.distributionMode();
    Schema schema = table.schema();
    PartitionSpec spec = table.spec();
    LOG.info("Write distribution mode is '{}'", mode.modeName());
    switch (mode) {
      case NONE:
        if (equalityFieldIds.isEmpty()) {
          return input;
        } else {
          LOG.info("Distribute rows by equality fields, because there are equality fields set");
          return input.keyBy(new EqualityFieldKeySelector(schema, flinkRowType, equalityFieldIds));
        }

      case HASH:
        if (equalityFieldIds.isEmpty()) {
          if (table.spec().isUnpartitioned()) {
            LOG.warn(
                "Fallback to use 'none' distribution mode, because there are no equality fields set "
                    + "and table is unpartitioned");
            return input;
          } else {
            if (BucketPartitionerUtil.hasOneBucketField(spec)) {
              return input.partitionCustom(
                  new BucketPartitioner(spec),
                  new BucketPartitionKeySelector(spec, schema, flinkRowType));
            } else {
              return input.keyBy(new PartitionKeySelector(spec, schema, flinkRowType));
            }
          }
        } else {
          if (spec.isUnpartitioned()) {
            LOG.info(
                "Distribute rows by equality fields, because there are equality fields set "
                    + "and table is unpartitioned");
            return input.keyBy(
                new EqualityFieldKeySelector(schema, flinkRowType, equalityFieldIds));
          } else {
            for (PartitionField partitionField : spec.fields()) {
              Preconditions.checkState(
                  equalityFieldIds.contains(partitionField.sourceId()),
                  "In 'hash' distribution mode with equality fields set, partition field '%s' "
                      + "should be included in equality fields: '%s'",
                  partitionField,
                  equalityFieldColumns);
            }
            return input.keyBy(new PartitionKeySelector(spec, schema, flinkRowType));
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
                  + "and{}=range is not supported yet in flink",
              WRITE_DISTRIBUTION_MODE);
          return input.keyBy(new EqualityFieldKeySelector(schema, flinkRowType, equalityFieldIds));
        }

      default:
        throw new RuntimeException("Unrecognized " + WRITE_DISTRIBUTION_MODE + ": " + mode);
    }
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
    return new Builder().forRow(input, tableSchema);
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
}
