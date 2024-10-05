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
package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkReadConf;
import org.apache.iceberg.flink.FlinkReadOptions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.assigner.OrderedSplitAssignerFactory;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.flink.source.assigner.SplitAssigner;
import org.apache.iceberg.flink.source.assigner.SplitAssignerFactory;
import org.apache.iceberg.flink.source.enumerator.ContinuousIcebergEnumerator;
import org.apache.iceberg.flink.source.enumerator.ContinuousSplitPlanner;
import org.apache.iceberg.flink.source.enumerator.ContinuousSplitPlannerImpl;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumeratorState;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumeratorStateSerializer;
import org.apache.iceberg.flink.source.enumerator.StaticIcebergEnumerator;
import org.apache.iceberg.flink.source.reader.ColumnStatsWatermarkExtractor;
import org.apache.iceberg.flink.source.reader.ConverterReaderFunction;
import org.apache.iceberg.flink.source.reader.IcebergSourceReader;
import org.apache.iceberg.flink.source.reader.IcebergSourceReaderMetrics;
import org.apache.iceberg.flink.source.reader.MetaDataReaderFunction;
import org.apache.iceberg.flink.source.reader.ReaderFunction;
import org.apache.iceberg.flink.source.reader.RowDataConverter;
import org.apache.iceberg.flink.source.reader.RowDataReaderFunction;
import org.apache.iceberg.flink.source.reader.SerializableRecordEmitter;
import org.apache.iceberg.flink.source.reader.SplitWatermarkExtractor;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitSerializer;
import org.apache.iceberg.flink.source.split.SerializableComparator;
import org.apache.iceberg.flink.source.split.SplitComparators;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental
public class IcebergSource<T> implements Source<T, IcebergSourceSplit, IcebergEnumeratorState> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSource.class);

  // This table loader can be closed, and it is only safe to use this instance for resource
  // independent information (e.g. a table name). Copies of this are required to avoid lifecycle
  // management conflicts with the user provided table loader. e.g. a copy of this is required for
  // split planning, which uses the underlying io, and should be closed after split planning is
  // complete.
  private final TableLoader tableLoader;
  private final ScanContext scanContext;
  private final ReaderFunction<T> readerFunction;
  private final SplitAssignerFactory assignerFactory;
  private final SerializableComparator<IcebergSourceSplit> splitComparator;
  private final SerializableRecordEmitter<T> emitter;
  private final String tableName;

  // cache the discovered splits by planSplitsForBatch, which can be called twice. And they come
  // from two different threads: (1) source/stream construction by main thread (2) enumerator
  // creation. Hence need volatile here.
  private volatile List<IcebergSourceSplit> batchSplits;

  IcebergSource(
      TableLoader tableLoader,
      ScanContext scanContext,
      ReaderFunction<T> readerFunction,
      SplitAssignerFactory assignerFactory,
      SerializableComparator<IcebergSourceSplit> splitComparator,
      Table table,
      SerializableRecordEmitter<T> emitter) {
    Preconditions.checkNotNull(tableLoader, "tableLoader is required.");
    Preconditions.checkNotNull(readerFunction, "readerFunction is required.");
    Preconditions.checkNotNull(assignerFactory, "assignerFactory is required.");
    Preconditions.checkNotNull(table, "table is required.");
    this.tableLoader = tableLoader;
    this.scanContext = scanContext;
    this.readerFunction = readerFunction;
    this.assignerFactory = assignerFactory;
    this.splitComparator = splitComparator;
    this.emitter = emitter;
    this.tableName = table.name();
  }

  String name() {
    return "IcebergSource-" + tableName;
  }

  private String planningThreadName() {
    // Ideally, operatorId should be used as the threadPoolName as Flink guarantees its uniqueness
    // within a job. SplitEnumeratorContext doesn't expose the OperatorCoordinator.Context, which
    // would contain the OperatorID. Need to discuss with Flink community whether it is ok to expose
    // a public API like the protected method "OperatorCoordinator.Context getCoordinatorContext()"
    // from SourceCoordinatorContext implementation. For now, <table name>-<random UUID> is used as
    // the unique thread pool name.
    return tableName + "-" + UUID.randomUUID();
  }

  /**
   * Cache the enumerated splits for batch execution to avoid double planning as there are two code
   * paths obtaining splits: (1) infer parallelism (2) enumerator creation.
   */
  private List<IcebergSourceSplit> planSplitsForBatch(String threadName) {
    if (batchSplits != null) {
      return batchSplits;
    }

    ExecutorService workerPool =
        ThreadPools.newFixedThreadPool(threadName, scanContext.planParallelism());
    try (TableLoader loader = tableLoader.clone()) {
      loader.open();
      this.batchSplits =
          FlinkSplitPlanner.planIcebergSourceSplits(loader.loadTable(), scanContext, workerPool);
      LOG.info(
          "Discovered {} splits from table {} during job initialization",
          batchSplits.size(),
          tableName);
      return batchSplits;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close table loader", e);
    } finally {
      workerPool.shutdown();
    }
  }

  @Override
  public Boundedness getBoundedness() {
    return scanContext.isStreaming() ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED;
  }

  @Override
  public SourceReader<T, IcebergSourceSplit> createReader(SourceReaderContext readerContext) {
    IcebergSourceReaderMetrics metrics =
        new IcebergSourceReaderMetrics(readerContext.metricGroup(), tableName);
    return new IcebergSourceReader<>(
        emitter, metrics, readerFunction, splitComparator, readerContext);
  }

  @Override
  public SplitEnumerator<IcebergSourceSplit, IcebergEnumeratorState> createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext) {
    return createEnumerator(enumContext, null);
  }

  @Override
  public SplitEnumerator<IcebergSourceSplit, IcebergEnumeratorState> restoreEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext, IcebergEnumeratorState enumState) {
    return createEnumerator(enumContext, enumState);
  }

  @Override
  public SimpleVersionedSerializer<IcebergSourceSplit> getSplitSerializer() {
    return new IcebergSourceSplitSerializer(scanContext.caseSensitive());
  }

  @Override
  public SimpleVersionedSerializer<IcebergEnumeratorState> getEnumeratorCheckpointSerializer() {
    return new IcebergEnumeratorStateSerializer(scanContext.caseSensitive());
  }

  private SplitEnumerator<IcebergSourceSplit, IcebergEnumeratorState> createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext,
      @Nullable IcebergEnumeratorState enumState) {
    SplitAssigner assigner;
    if (enumState == null) {
      assigner = assignerFactory.createAssigner();
    } else {
      LOG.info(
          "Iceberg source restored {} splits from state for table {}",
          enumState.pendingSplits().size(),
          tableName);
      assigner = assignerFactory.createAssigner(enumState.pendingSplits());
    }
    if (scanContext.isStreaming()) {
      ContinuousSplitPlanner splitPlanner =
          new ContinuousSplitPlannerImpl(tableLoader, scanContext, planningThreadName());
      return new ContinuousIcebergEnumerator(
          enumContext, assigner, scanContext, splitPlanner, enumState);
    } else {
      if (enumState == null) {
        // Only do scan planning if nothing is restored from checkpoint state
        List<IcebergSourceSplit> splits = planSplitsForBatch(planningThreadName());
        assigner.onDiscoveredSplits(splits);
        // clear the cached splits after enumerator creation as they won't be needed anymore
        this.batchSplits = null;
      }

      return new StaticIcebergEnumerator(enumContext, assigner);
    }
  }

  private boolean shouldInferParallelism() {
    return !scanContext.isStreaming();
  }

  private int inferParallelism(ReadableConfig flinkConf, StreamExecutionEnvironment env) {
    int parallelism =
        SourceUtil.inferParallelism(
            flinkConf,
            scanContext.limit(),
            () -> {
              List<IcebergSourceSplit> splits = planSplitsForBatch(planningThreadName());
              return splits.size();
            });

    if (env.getMaxParallelism() > 0) {
      parallelism = Math.min(parallelism, env.getMaxParallelism());
    }

    return parallelism;
  }

  /**
   * Create a source builder.
   *
   * @deprecated since 1.7.0. Will be removed in 2.0.0; use{@link IcebergSource#forRowData()} or
   *     {@link IcebergSource#forOutputType(RowDataConverter)} instead
   */
  @Deprecated
  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  /** Create a source builder for RowData output type. */
  public static Builder<RowData> forRowData() {
    return new Builder<>();
  }

  /**
   * Create a source builder that would convert {@link RowData} to the output type {@code T}.
   *
   * @param converter convert {@link RowData} to output type {@code T}
   * @param <T> output type
   * @return an IcebergSource builder
   */
  public static <T> Builder<T> forOutputType(RowDataConverter<T> converter) {
    return new Builder<T>().converter(converter);
  }

  public static class Builder<T> {
    private TableLoader tableLoader;
    private Table table;
    private SplitAssignerFactory splitAssignerFactory;
    private SerializableComparator<IcebergSourceSplit> splitComparator;
    private ReaderFunction<T> readerFunction;
    private RowDataConverter<T> converter;
    private ReadableConfig flinkConfig = new Configuration();
    private final ScanContext.Builder contextBuilder = ScanContext.builder();
    private TableSchema projectedFlinkSchema;
    private Boolean exposeLocality;

    private final Map<String, String> readOptions = Maps.newHashMap();

    Builder() {}

    public Builder<T> tableLoader(TableLoader loader) {
      this.tableLoader = loader;
      return this;
    }

    public Builder<T> table(Table newTable) {
      this.table = newTable;
      return this;
    }

    public Builder<T> assignerFactory(SplitAssignerFactory assignerFactory) {
      this.splitAssignerFactory = assignerFactory;
      return this;
    }

    public Builder<T> splitComparator(
        SerializableComparator<IcebergSourceSplit> newSplitComparator) {
      this.splitComparator = newSplitComparator;
      return this;
    }

    /**
     * @deprecated since 1.7.0. Will be removed in 2.0.0; use{@link
     *     IcebergSource#forOutputType(RowDataConverter)} instead to produce output type other than
     *     {@link RowData}.
     */
    @Deprecated
    public Builder<T> readerFunction(ReaderFunction<T> newReaderFunction) {
      Preconditions.checkState(
          converter == null,
          "Cannot set reader function when builder was created via IcebergSource.forOutputType(Converter)");
      this.readerFunction = newReaderFunction;
      return this;
    }

    /**
     * Don't need to be public. It is set by {@link IcebergSource#forOutputType(RowDataConverter)}.
     */
    private Builder<T> converter(RowDataConverter<T> newConverter) {
      this.converter = newConverter;
      return this;
    }

    public Builder<T> flinkConfig(ReadableConfig config) {
      this.flinkConfig = config;
      return this;
    }

    public Builder<T> caseSensitive(boolean newCaseSensitive) {
      readOptions.put(FlinkReadOptions.CASE_SENSITIVE, Boolean.toString(newCaseSensitive));
      return this;
    }

    public Builder<T> useSnapshotId(Long newSnapshotId) {
      if (newSnapshotId != null) {
        readOptions.put(FlinkReadOptions.SNAPSHOT_ID.key(), Long.toString(newSnapshotId));
      }
      return this;
    }

    public Builder<T> streamingStartingStrategy(StreamingStartingStrategy newStartingStrategy) {
      readOptions.put(FlinkReadOptions.STARTING_STRATEGY, newStartingStrategy.name());
      return this;
    }

    public Builder<T> startSnapshotTimestamp(Long newStartSnapshotTimestamp) {
      if (newStartSnapshotTimestamp != null) {
        readOptions.put(
            FlinkReadOptions.START_SNAPSHOT_TIMESTAMP.key(),
            Long.toString(newStartSnapshotTimestamp));
      }
      return this;
    }

    public Builder<T> startSnapshotId(Long newStartSnapshotId) {
      if (newStartSnapshotId != null) {
        readOptions.put(
            FlinkReadOptions.START_SNAPSHOT_ID.key(), Long.toString(newStartSnapshotId));
      }
      return this;
    }

    public Builder<T> tag(String tag) {
      readOptions.put(FlinkReadOptions.TAG.key(), tag);
      return this;
    }

    public Builder<T> branch(String branch) {
      readOptions.put(FlinkReadOptions.BRANCH.key(), branch);
      return this;
    }

    public Builder<T> startTag(String startTag) {
      readOptions.put(FlinkReadOptions.START_TAG.key(), startTag);
      return this;
    }

    public Builder<T> endTag(String endTag) {
      readOptions.put(FlinkReadOptions.END_TAG.key(), endTag);
      return this;
    }

    public Builder<T> endSnapshotId(Long newEndSnapshotId) {
      if (newEndSnapshotId != null) {
        readOptions.put(FlinkReadOptions.END_SNAPSHOT_ID.key(), Long.toString(newEndSnapshotId));
      }
      return this;
    }

    public Builder<T> asOfTimestamp(Long newAsOfTimestamp) {
      if (newAsOfTimestamp != null) {
        readOptions.put(FlinkReadOptions.AS_OF_TIMESTAMP.key(), Long.toString(newAsOfTimestamp));
      }
      return this;
    }

    public Builder<T> splitSize(Long newSplitSize) {
      if (newSplitSize != null) {
        readOptions.put(FlinkReadOptions.SPLIT_SIZE, Long.toString(newSplitSize));
      }
      return this;
    }

    public Builder<T> splitLookback(Integer newSplitLookback) {
      if (newSplitLookback != null) {
        readOptions.put(FlinkReadOptions.SPLIT_LOOKBACK, Integer.toString(newSplitLookback));
      }
      return this;
    }

    public Builder<T> splitOpenFileCost(Long newSplitOpenFileCost) {
      if (newSplitOpenFileCost != null) {
        readOptions.put(FlinkReadOptions.SPLIT_FILE_OPEN_COST, Long.toString(newSplitOpenFileCost));
      }

      return this;
    }

    public Builder<T> streaming(boolean streaming) {
      readOptions.put(FlinkReadOptions.STREAMING, Boolean.toString(streaming));
      return this;
    }

    public Builder<T> monitorInterval(Duration newMonitorInterval) {
      if (newMonitorInterval != null) {
        readOptions.put(FlinkReadOptions.MONITOR_INTERVAL, newMonitorInterval.toNanos() + " ns");
      }
      return this;
    }

    public Builder<T> nameMapping(String newNameMapping) {
      readOptions.put(TableProperties.DEFAULT_NAME_MAPPING, newNameMapping);
      return this;
    }

    public Builder<T> project(Schema newProjectedSchema) {
      this.contextBuilder.project(newProjectedSchema);
      return this;
    }

    public Builder<T> project(TableSchema newProjectedFlinkSchema) {
      this.projectedFlinkSchema = newProjectedFlinkSchema;
      return this;
    }

    public Builder<T> filters(List<Expression> newFilters) {
      this.contextBuilder.filters(newFilters);
      return this;
    }

    public Builder<T> limit(Long newLimit) {
      if (newLimit != null) {
        readOptions.put(FlinkReadOptions.LIMIT, Long.toString(newLimit));
      }
      return this;
    }

    public Builder<T> includeColumnStats(boolean newIncludeColumnStats) {
      readOptions.put(
          FlinkReadOptions.INCLUDE_COLUMN_STATS, Boolean.toString(newIncludeColumnStats));
      return this;
    }

    public Builder<T> planParallelism(int planParallelism) {
      readOptions.put(
          FlinkConfigOptions.TABLE_EXEC_ICEBERG_WORKER_POOL_SIZE.key(),
          Integer.toString(planParallelism));
      return this;
    }

    public Builder<T> exposeLocality(boolean newExposeLocality) {
      this.exposeLocality = newExposeLocality;
      return this;
    }

    public Builder<T> maxAllowedPlanningFailures(int maxAllowedPlanningFailures) {
      readOptions.put(
          FlinkReadOptions.MAX_ALLOWED_PLANNING_FAILURES_OPTION.key(),
          Integer.toString(maxAllowedPlanningFailures));
      return this;
    }

    /**
     * Set the read properties for Flink source. View the supported properties in {@link
     * FlinkReadOptions}
     */
    public Builder<T> set(String property, String value) {
      readOptions.put(property, value);
      return this;
    }

    /**
     * Set the read properties for Flink source. View the supported properties in {@link
     * FlinkReadOptions}
     */
    public Builder<T> setAll(Map<String, String> properties) {
      readOptions.putAll(properties);
      return this;
    }

    /**
     * Emits watermarks once per split based on the min value of column statistics from files
     * metadata in the given split. The generated watermarks are also used for ordering the splits
     * for read. Accepted column types are timestamp/timestamptz/long. For long columns consider
     * setting {@link #watermarkColumnTimeUnit(TimeUnit)}.
     *
     * <p>Consider setting `read.split.open-file-cost` to prevent combining small files to a single
     * split when the watermark is used for watermark alignment.
     */
    public Builder<T> watermarkColumn(String columnName) {
      Preconditions.checkArgument(
          splitAssignerFactory == null,
          "Watermark column and SplitAssigner should not be set in the same source");
      readOptions.put(FlinkReadOptions.WATERMARK_COLUMN, columnName);
      return this;
    }

    /**
     * When the type of the {@link #watermarkColumn} is {@link
     * org.apache.iceberg.types.Types.LongType}, then sets the {@link TimeUnit} to convert the
     * value. The default value is {@link TimeUnit#MICROSECONDS}.
     */
    public Builder<T> watermarkColumnTimeUnit(TimeUnit timeUnit) {
      readOptions.put(FlinkReadOptions.WATERMARK_COLUMN_TIME_UNIT, timeUnit.name());
      return this;
    }

    /**
     * @deprecated Use {@link #setAll} instead.
     */
    @Deprecated
    public Builder<T> properties(Map<String, String> properties) {
      readOptions.putAll(properties);
      return this;
    }

    public IcebergSource<T> build() {
      if (table == null) {
        try (TableLoader loader = tableLoader) {
          loader.open();
          this.table = tableLoader.loadTable();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      contextBuilder.resolveConfig(table, readOptions, flinkConfig);
      contextBuilder.exposeLocality(
          SourceUtil.isLocalityEnabled(table, flinkConfig, exposeLocality));
      contextBuilder.planParallelism(
          flinkConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_WORKER_POOL_SIZE));
      Schema icebergSchema = table.schema();
      if (projectedFlinkSchema != null) {
        contextBuilder.project(FlinkSchemaUtil.convert(icebergSchema, projectedFlinkSchema));
      }

      SerializableRecordEmitter<T> emitter = SerializableRecordEmitter.defaultEmitter();
      FlinkReadConf flinkReadConf = new FlinkReadConf(table, readOptions, flinkConfig);
      String watermarkColumn = flinkReadConf.watermarkColumn();
      TimeUnit watermarkTimeUnit = flinkReadConf.watermarkColumnTimeUnit();

      if (watermarkColumn != null) {
        // Column statistics is needed for watermark generation
        contextBuilder.includeColumnStats(Sets.newHashSet(watermarkColumn));

        SplitWatermarkExtractor watermarkExtractor =
            new ColumnStatsWatermarkExtractor(icebergSchema, watermarkColumn, watermarkTimeUnit);
        emitter = SerializableRecordEmitter.emitterWithWatermark(watermarkExtractor);
        splitAssignerFactory =
            new OrderedSplitAssignerFactory(SplitComparators.watermark(watermarkExtractor));
      }

      ScanContext context = contextBuilder.build();
      context.validate();
      if (readerFunction == null) {
        this.readerFunction = readerFunction(context);
      }

      if (splitAssignerFactory == null) {
        if (splitComparator == null) {
          splitAssignerFactory = new SimpleSplitAssignerFactory();
        } else {
          splitAssignerFactory = new OrderedSplitAssignerFactory(splitComparator);
        }
      }

      // Since builder already load the table, pass it to the source to avoid double loading
      return new IcebergSource<>(
          tableLoader,
          context,
          readerFunction,
          splitAssignerFactory,
          splitComparator,
          table,
          emitter);
    }

    /**
     * Build the {@link IcebergSource} and create a {@link DataStream} from the source. Watermark
     * strategy is set to {@link WatermarkStrategy#noWatermarks()}.
     *
     * @return data stream from the Iceberg source
     */
    public DataStream<T> buildStream(StreamExecutionEnvironment env) {
      // buildStream should only be called with RowData or Converter paths.
      Preconditions.checkState(
          readerFunction == null,
          "Cannot set reader function when building a data stream from the source");
      IcebergSource<T> source = build();
      TypeInformation<T> outputTypeInfo =
          outputTypeInfo(converter, table.schema(), source.scanContext.project());
      DataStreamSource<T> stream =
          env.fromSource(source, WatermarkStrategy.noWatermarks(), source.name(), outputTypeInfo);
      if (source.shouldInferParallelism()) {
        stream = stream.setParallelism(source.inferParallelism(flinkConfig, env));
      }

      return stream;
    }

    private static <T> TypeInformation<T> outputTypeInfo(
        RowDataConverter<T> converter, Schema tableSchema, Schema projected) {
      if (converter != null) {
        return converter.getProducedType();
      } else {
        // output type is RowData
        Schema readSchema = projected != null ? projected : tableSchema;
        return (TypeInformation<T>)
            FlinkCompatibilityUtil.toTypeInfo(FlinkSchemaUtil.convert(readSchema));
      }
    }

    private ReaderFunction<T> readerFunction(ScanContext context) {
      if (table instanceof BaseMetadataTable) {
        MetaDataReaderFunction rowDataReaderFunction =
            new MetaDataReaderFunction(
                flinkConfig, table.schema(), context.project(), table.io(), table.encryption());
        return (ReaderFunction<T>) rowDataReaderFunction;
      } else {
        if (converter == null) {
          return (ReaderFunction<T>)
              new RowDataReaderFunction(
                  flinkConfig,
                  table.schema(),
                  context.project(),
                  context.nameMapping(),
                  context.caseSensitive(),
                  table.io(),
                  table.encryption(),
                  context.filters(),
                  context.limit());
        } else {
          return new ConverterReaderFunction<>(
              converter,
              flinkConfig,
              table.schema(),
              context.project(),
              context.nameMapping(),
              context.caseSensitive(),
              table.io(),
              table.encryption(),
              context.filters(),
              context.limit());
        }
      }
    }
  }
}
