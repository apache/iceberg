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
import java.util.Set;
import java.util.function.Function;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
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
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.shuffle.DataStatisticsOperatorFactory;
import org.apache.iceberg.flink.sink.shuffle.RangePartitioner;
import org.apache.iceberg.flink.sink.shuffle.StatisticsOrRecord;
import org.apache.iceberg.flink.sink.shuffle.StatisticsType;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkSink {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkSink.class);

  private static final String ICEBERG_STREAM_WRITER_NAME =
      IcebergStreamWriter.class.getSimpleName();
  private static final String ICEBERG_FILES_COMMITTER_NAME =
      IcebergFilesCommitter.class.getSimpleName();

  private FlinkSink() {}

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

  public static class Builder {
    private Function<String, DataStream<RowData>> inputCreator = null;
    private TableLoader tableLoader;
    private Table table;
    private TableSchema tableSchema;
    private List<String> equalityFieldColumns = null;
    private String uidPrefix = null;
    private final Map<String, String> snapshotProperties = Maps.newHashMap();
    private ReadableConfig readableConfig = new Configuration();
    private final Map<String, String> writeOptions = Maps.newHashMap();
    private FlinkWriteConf flinkWriteConf = null;

    private Builder() {}

    private Builder forRowData(DataStream<RowData> newRowDataInput) {
      this.inputCreator = ignored -> newRowDataInput;
      return this;
    }

    private <T> Builder forMapperOutputType(
        DataStream<T> input, MapFunction<T, RowData> mapper, TypeInformation<RowData> outputType) {
      this.inputCreator =
          newUidPrefix -> {
            // Input stream order is crucial for some situation(e.g. in cdc case). Therefore, we
            // need to set the parallelism
            // of map operator same as its input to keep map operator chaining its input, and avoid
            // rebalanced by default.
            SingleOutputStreamOperator<RowData> inputStream =
                input.map(mapper, outputType).setParallelism(input.getParallelism());
            if (newUidPrefix != null) {
              inputStream.name(operatorName(newUidPrefix)).uid(newUidPrefix + "-mapper");
            }
            return inputStream;
          };
      return this;
    }

    /**
     * This iceberg {@link Table} instance is used for initializing {@link IcebergStreamWriter}
     * which will write all the records into {@link DataFile}s and emit them to downstream operator.
     * Providing a table would avoid so many table loading from each separate task.
     *
     * @param newTable the loaded iceberg table instance.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder table(Table newTable) {
      this.table = newTable;
      return this;
    }

    /**
     * The table loader is used for loading tables in {@link IcebergFilesCommitter} lazily, we need
     * this loader because {@link Table} is not serializable and could not just use the loaded table
     * from Builder#table in the remote task manager.
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
      if (mode != null) {
        writeOptions.put(FlinkWriteOptions.DISTRIBUTION_MODE.key(), mode.modeName());
      }
      return this;
    }

    /**
     * Range distribution needs to collect statistics about data distribution to properly shuffle
     * the records in relatively balanced way. In general, low cardinality should use {@link
     * StatisticsType#Map} and high cardinality should use {@link StatisticsType#Sketch} Refer to
     * {@link StatisticsType} Javadoc for more details.
     *
     * <p>Default is {@link StatisticsType#Auto} where initially Map statistics is used. But if
     * cardinality is higher than the threshold (currently 10K) as defined in {@code
     * SketchUtil#OPERATOR_SKETCH_SWITCH_THRESHOLD}, statistics collection automatically switches to
     * the sketch reservoir sampling.
     *
     * <p>Explicit set the statistics type if the default behavior doesn't work.
     *
     * @param type to specify the statistics type for range distribution.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder rangeDistributionStatisticsType(StatisticsType type) {
      if (type != null) {
        writeOptions.put(FlinkWriteOptions.RANGE_DISTRIBUTION_STATISTICS_TYPE.key(), type.name());
      }
      return this;
    }

    /**
     * If sort order contains partition columns, each sort key would map to one partition and data
     * file. This relative weight can avoid placing too many small files for sort keys with low
     * traffic. It is a double value that defines the minimal weight for each sort key. `0.02` means
     * each key has a base weight of `2%` of the targeted traffic weight per writer task.
     *
     * <p>E.g. the sink Iceberg table is partitioned daily by event time. Assume the data stream
     * contain events from now up to 180 days ago. With even time, traffic weight distribution
     * across different days typically has a long tail pattern. Current day contains the most
     * traffic. The older days (long tail) contain less and less traffic. Assume writer parallelism
     * is `10`. The total weight across all 180 days is `10,000`. Target traffic weight per writer
     * task would be `1,000`. Assume the weight sum for the oldest 150 days is `1,000`. Normally,
     * the range partitioner would put all the oldest 150 days in one writer task. That writer task
     * would write to 150 small files (one per day). If this config is set to `0.02`. It means every
     * sort key has a base weight of `2%` of targeted weight of `1,000` for every write task. It
     * would essentially avoid placing more than `50` data files (one per day) on one writer task no
     * matter how small they are.
     *
     * <p>This is only applicable to {@link StatisticsType#Map} for low-cardinality scenario. For
     * {@link StatisticsType#Sketch} high-cardinality sort columns, they are usually not used as
     * partition columns. Otherwise, too many partitions and small files may be generated during
     * write. Sketch range partitioner simply splits high-cardinality keys into ordered ranges.
     *
     * <p>Default is {@code 0.0%}.
     */
    public Builder rangeDistributionSortKeyBaseWeight(double weight) {
      writeOptions.put(
          FlinkWriteOptions.RANGE_DISTRIBUTION_SORT_KEY_BASE_WEIGHT.key(), Double.toString(weight));
      return this;
    }

    /**
     * Configuring the write parallel number for iceberg stream writer.
     *
     * @param newWriteParallelism the number of parallel iceberg stream writer.
     * @return {@link Builder} to connect the iceberg table.
     */
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
     * Set the uid prefix for FlinkSink operators. Note that FlinkSink internally consists of
     * multiple operators (like writer, committer, dummy sink etc.) Actually operator uid will be
     * appended with a suffix like "uidPrefix-writer". <br>
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
     * checkpoint, Flink won't be able to restore the previous Flink sink operator state (more
     * specifically the committer operator state). You need to use {@code --allowNonRestoredState}
     * to ignore the previous sink state. During restore Flink sink state is used to check if last
     * commit was actually successful or not. {@code --allowNonRestoredState} can lead to data loss
     * if the Iceberg commit failed in the last completed checkpoint.
     *
     * @param newPrefix prefix for Flink sink operator uid and name
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder uidPrefix(String newPrefix) {
      this.uidPrefix = newPrefix;
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

    public Builder toBranch(String branch) {
      writeOptions.put(FlinkWriteOptions.BRANCH.key(), branch);
      return this;
    }

    private <T> DataStreamSink<T> chainIcebergOperators() {
      Preconditions.checkArgument(
          inputCreator != null,
          "Please use forRowData() or forMapperOutputType() to initialize the input DataStream.");
      Preconditions.checkNotNull(tableLoader, "Table loader shouldn't be null");

      DataStream<RowData> rowDataInput = inputCreator.apply(uidPrefix);

      if (table == null) {
        if (!tableLoader.isOpen()) {
          tableLoader.open();
        }

        try (TableLoader loader = tableLoader) {
          this.table = loader.loadTable();
        } catch (IOException e) {
          throw new UncheckedIOException(
              "Failed to load iceberg table from table loader: " + tableLoader, e);
        }
      }

      flinkWriteConf = new FlinkWriteConf(table, writeOptions, readableConfig);

      // Find out the equality field id list based on the user-provided equality field column names.
      List<Integer> equalityFieldIds = checkAndGetEqualityFieldIds();

      RowType flinkRowType = toFlinkRowType(table.schema(), tableSchema);
      int writerParallelism =
          flinkWriteConf.writeParallelism() == null
              ? rowDataInput.getParallelism()
              : flinkWriteConf.writeParallelism();

      // Distribute the records from input data stream based on the write.distribution-mode and
      // equality fields.
      DataStream<RowData> distributeStream =
          distributeDataStream(rowDataInput, equalityFieldIds, flinkRowType, writerParallelism);

      // Add parallel writers that append rows to files
      SingleOutputStreamOperator<WriteResult> writerStream =
          appendWriter(distributeStream, flinkRowType, equalityFieldIds, writerParallelism);

      // Add single-parallelism committer that commits files
      // after successful checkpoint or end of input
      SingleOutputStreamOperator<Void> committerStream = appendCommitter(writerStream);

      // Add dummy discard sink
      return appendDummySink(committerStream);
    }

    /**
     * Append the iceberg sink operators to write records to iceberg table.
     *
     * @return {@link DataStreamSink} for sink.
     */
    public DataStreamSink<Void> append() {
      return chainIcebergOperators();
    }

    private String operatorName(String suffix) {
      return uidPrefix != null ? uidPrefix + "-" + suffix : suffix;
    }

    @VisibleForTesting
    List<Integer> checkAndGetEqualityFieldIds() {
      List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().identifierFieldIds());
      if (equalityFieldColumns != null && !equalityFieldColumns.isEmpty()) {
        Set<Integer> equalityFieldSet =
            Sets.newHashSetWithExpectedSize(equalityFieldColumns.size());
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

    @SuppressWarnings("unchecked")
    private <T> DataStreamSink<T> appendDummySink(
        SingleOutputStreamOperator<Void> committerStream) {
      DataStreamSink<T> resultStream =
          committerStream
              .addSink(new DiscardingSink())
              .name(operatorName(String.format("IcebergSink %s", this.table.name())))
              .setParallelism(1);
      if (uidPrefix != null) {
        resultStream = resultStream.uid(uidPrefix + "-dummysink");
      }
      return resultStream;
    }

    private SingleOutputStreamOperator<Void> appendCommitter(
        SingleOutputStreamOperator<WriteResult> writerStream) {
      IcebergFilesCommitter filesCommitter =
          new IcebergFilesCommitter(
              tableLoader,
              flinkWriteConf.overwriteMode(),
              snapshotProperties,
              flinkWriteConf.workerPoolSize(),
              flinkWriteConf.branch(),
              table.spec());
      SingleOutputStreamOperator<Void> committerStream =
          writerStream
              .transform(operatorName(ICEBERG_FILES_COMMITTER_NAME), Types.VOID, filesCommitter)
              .setParallelism(1)
              .setMaxParallelism(1);
      if (uidPrefix != null) {
        committerStream = committerStream.uid(uidPrefix + "-committer");
      }
      return committerStream;
    }

    private SingleOutputStreamOperator<WriteResult> appendWriter(
        DataStream<RowData> input,
        RowType flinkRowType,
        List<Integer> equalityFieldIds,
        int writerParallelism) {
      // Validate the equality fields and partition fields if we enable the upsert mode.
      if (flinkWriteConf.upsertMode()) {
        Preconditions.checkState(
            !flinkWriteConf.overwriteMode(),
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

      SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(table);
      Duration tableRefreshInterval = flinkWriteConf.tableRefreshInterval();

      SerializableSupplier<Table> tableSupplier;
      if (tableRefreshInterval != null) {
        tableSupplier =
            new CachingTableSupplier(serializableTable, tableLoader, tableRefreshInterval);
      } else {
        tableSupplier = () -> serializableTable;
      }

      IcebergStreamWriter<RowData> streamWriter =
          createStreamWriter(tableSupplier, flinkWriteConf, flinkRowType, equalityFieldIds);

      SingleOutputStreamOperator<WriteResult> writerStream =
          input
              .transform(
                  operatorName(ICEBERG_STREAM_WRITER_NAME),
                  TypeInformation.of(WriteResult.class),
                  streamWriter)
              .setParallelism(writerParallelism);
      if (uidPrefix != null) {
        writerStream = writerStream.uid(uidPrefix + "-writer");
      }
      return writerStream;
    }

    private DataStream<RowData> distributeDataStream(
        DataStream<RowData> input,
        List<Integer> equalityFieldIds,
        RowType flinkRowType,
        int writerParallelism) {
      DistributionMode writeMode = flinkWriteConf.distributionMode();
      LOG.info("Write distribution mode is '{}'", writeMode.modeName());

      Schema iSchema = table.schema();
      PartitionSpec partitionSpec = table.spec();
      SortOrder sortOrder = table.sortOrder();

      switch (writeMode) {
        case NONE:
          if (equalityFieldIds.isEmpty()) {
            return input;
          } else {
            LOG.info("Distribute rows by equality fields, because there are equality fields set");
            return input.keyBy(
                new EqualityFieldKeySelector(iSchema, flinkRowType, equalityFieldIds));
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
          // Ideally, exception should be thrown in the combination of range distribution and
          // equality fields. Primary key case should use hash distribution mode.
          // Keep the current behavior of falling back to keyBy for backward compatibility.
          if (!equalityFieldIds.isEmpty()) {
            LOG.warn(
                "Hash distribute rows by equality fields, even though {}=range is set. "
                    + "Range distribution for primary keys are not always safe in "
                    + "Flink streaming writer.",
                WRITE_DISTRIBUTION_MODE);
            return input.keyBy(
                new EqualityFieldKeySelector(iSchema, flinkRowType, equalityFieldIds));
          }

          // range distribute by partition key or sort key if table has an SortOrder
          Preconditions.checkState(
              sortOrder.isSorted() || partitionSpec.isPartitioned(),
              "Invalid write distribution mode: range. Need to define sort order and partition spec.");
          if (sortOrder.isUnsorted()) {
            sortOrder = Partitioning.sortOrderFor(partitionSpec);
            LOG.info("Construct sort order from partition spec");
          }

          LOG.info("Range distribute rows by sort order: {}", sortOrder);
          StatisticsType statisticsType = flinkWriteConf.rangeDistributionStatisticsType();
          SingleOutputStreamOperator<StatisticsOrRecord> shuffleStream =
              input
                  .transform(
                      operatorName("range-shuffle"),
                      TypeInformation.of(StatisticsOrRecord.class),
                      new DataStatisticsOperatorFactory(
                          iSchema,
                          sortOrder,
                          writerParallelism,
                          statisticsType,
                          flinkWriteConf.rangeDistributionSortKeyBaseWeight()))
                  // Set the parallelism same as input operator to encourage chaining
                  .setParallelism(input.getParallelism());
          if (uidPrefix != null) {
            shuffleStream = shuffleStream.uid(uidPrefix + "-shuffle");
          }

          return shuffleStream
              .partitionCustom(new RangePartitioner(iSchema, sortOrder), r -> r)
              .filter(StatisticsOrRecord::hasRecord)
              .map(StatisticsOrRecord::record);

        default:
          throw new RuntimeException("Unrecognized " + WRITE_DISTRIBUTION_MODE + ": " + writeMode);
      }
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
      // (backend by 1 'byte'), we will read 4 bytes rather than 1 byte, it will mess up the
      // byte array in BinaryRowData. So here we must use flink schema.
      return (RowType) requestedSchema.toRowDataType().getLogicalType();
    } else {
      return FlinkSchemaUtil.convert(schema);
    }
  }

  static IcebergStreamWriter<RowData> createStreamWriter(
      SerializableSupplier<Table> tableSupplier,
      FlinkWriteConf flinkWriteConf,
      RowType flinkRowType,
      List<Integer> equalityFieldIds) {
    Preconditions.checkArgument(tableSupplier != null, "Iceberg table supplier shouldn't be null");

    Table initTable = tableSupplier.get();
    FileFormat format = flinkWriteConf.dataFileFormat();
    TaskWriterFactory<RowData> taskWriterFactory =
        new RowDataTaskWriterFactory(
            tableSupplier,
            flinkRowType,
            flinkWriteConf.targetDataFileSize(),
            format,
            writeProperties(initTable, format, flinkWriteConf),
            equalityFieldIds,
            flinkWriteConf.upsertMode());

    return new IcebergStreamWriter<>(initTable.name(), taskWriterFactory);
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
}
