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
package org.apache.iceberg.flink.sink.dynamic;

import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION_STRATEGY;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_LEVEL;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Dynamic version of the IcebergSink which supports:
 *
 * <ol>
 *   <li>Writing to any number of tables (No more 1:1 sink/topic relationship).
 *   <li>Creating and updating tables based on the user-supplied routing.
 *   <li>Updating the schema and partition spec of tables based on the user-supplied specification.
 * </ol>
 */
@Experimental
public class DynamicIcebergSink
    implements Sink<DynamicRecordInternal>,
        SupportsPreWriteTopology<DynamicRecordInternal>,
        SupportsCommitter<DynamicCommittable>,
        SupportsPreCommitTopology<DynamicWriteResult, DynamicCommittable>,
        SupportsPostCommitTopology<DynamicCommittable> {

  private final CatalogLoader catalogLoader;
  private final Map<String, String> snapshotProperties;
  private final String uidPrefix;
  private final String sinkId;
  private final Map<String, String> writeProperties;
  private final transient FlinkWriteConf flinkWriteConf;
  private final FileFormat dataFileFormat;
  private final long targetDataFileSize;
  private final boolean overwriteMode;
  private final int workerPoolSize;

  private DynamicIcebergSink(
      CatalogLoader catalogLoader,
      Map<String, String> snapshotProperties,
      String uidPrefix,
      Map<String, String> writeProperties,
      FlinkWriteConf flinkWriteConf) {
    this.catalogLoader = catalogLoader;
    this.snapshotProperties = snapshotProperties;
    this.uidPrefix = uidPrefix;
    this.writeProperties = writeProperties;
    this.flinkWriteConf = flinkWriteConf;
    this.dataFileFormat = flinkWriteConf.dataFileFormat();
    this.targetDataFileSize = flinkWriteConf.targetDataFileSize();
    this.overwriteMode = flinkWriteConf.overwriteMode();
    this.workerPoolSize = flinkWriteConf.workerPoolSize();
    // We generate a random UUID every time when a sink is created.
    // This is used to separate files generated by different sinks writing the same table.
    // Also used to generate the aggregator operator name
    this.sinkId = UUID.randomUUID().toString();
  }

  @Override
  public SinkWriter<DynamicRecordInternal> createWriter(InitContext context) {
    return new DynamicWriter(
        catalogLoader.loadCatalog(),
        dataFileFormat,
        targetDataFileSize,
        writeProperties,
        new DynamicWriterMetrics(context.metricGroup()),
        context.getTaskInfo().getIndexOfThisSubtask(),
        context.getTaskInfo().getAttemptNumber());
  }

  @Override
  public Committer<DynamicCommittable> createCommitter(CommitterInitContext context) {
    DynamicCommitterMetrics metrics = new DynamicCommitterMetrics(context.metricGroup());
    return new DynamicCommitter(
        catalogLoader.loadCatalog(),
        snapshotProperties,
        overwriteMode,
        workerPoolSize,
        sinkId,
        metrics);
  }

  @Override
  public SimpleVersionedSerializer<DynamicCommittable> getCommittableSerializer() {
    return new DynamicCommittableSerializer();
  }

  @Override
  public void addPostCommitTopology(
      DataStream<CommittableMessage<DynamicCommittable>> committables) {}

  @Override
  public DataStream<DynamicRecordInternal> addPreWriteTopology(
      DataStream<DynamicRecordInternal> inputDataStream) {
    return distributeDataStream(inputDataStream);
  }

  @Override
  public DataStream<CommittableMessage<DynamicCommittable>> addPreCommitTopology(
      DataStream<CommittableMessage<DynamicWriteResult>> writeResults) {
    TypeInformation<CommittableMessage<DynamicCommittable>> typeInformation =
        CommittableMessageTypeInfo.of(this::getCommittableSerializer);

    return writeResults
        .keyBy(
            committable -> {
              if (committable instanceof CommittableSummary) {
                return "__summary";
              } else {
                CommittableWithLineage<DynamicWriteResult> result =
                    (CommittableWithLineage<DynamicWriteResult>) committable;
                return result.getCommittable().key().tableName();
              }
            })
        .transform(
            prefixIfNotNull(uidPrefix, sinkId + " Pre Commit"),
            typeInformation,
            new DynamicWriteResultAggregator(catalogLoader))
        .uid(prefixIfNotNull(uidPrefix, sinkId + "-pre-commit-topology"));
  }

  @Override
  public SimpleVersionedSerializer<DynamicWriteResult> getWriteResultSerializer() {
    return new DynamicWriteResultSerializer();
  }

  public static class Builder<T> {
    private DataStream<T> input;
    private DynamicRecordConverter<T> converter;
    private CatalogLoader catalogLoader;
    private String uidPrefix = null;
    private final Map<String, String> writeOptions = Maps.newHashMap();
    private final Map<String, String> snapshotSummary = Maps.newHashMap();
    private ReadableConfig readableConfig = new Configuration();
    private boolean immediateUpdate = false;
    private int cacheMaximumSize = 100;
    private long cacheRefreshMs = 1_000;

    private Builder() {}

    public Builder<T> forInput(DataStream<T> inputStream) {
      this.input = inputStream;
      return this;
    }

    public Builder<T> withConverter(DynamicRecordConverter<T> inputConverter) {
      this.converter = inputConverter;
      return this;
    }

    /**
     * The catalog loader is used for loading tables in {@link DynamicCommitter} lazily, we need
     * this loader because {@link Table} is not serializable and could not just use the loaded table
     * from Builder#table in the remote task manager.
     *
     * @param newCatalogLoader to load iceberg table inside tasks.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder<T> catalogLoader(CatalogLoader newCatalogLoader) {
      this.catalogLoader = newCatalogLoader;
      return this;
    }

    /**
     * Set the write properties for IcebergSink. View the supported properties in {@link
     * FlinkWriteOptions}
     */
    public Builder<T> set(String property, String value) {
      writeOptions.put(property, value);
      return this;
    }

    /**
     * Set the write properties for IcebergSink. View the supported properties in {@link
     * FlinkWriteOptions}
     */
    public Builder<T> setAll(Map<String, String> properties) {
      writeOptions.putAll(properties);
      return this;
    }

    public Builder<T> overwrite(boolean newOverwrite) {
      writeOptions.put(FlinkWriteOptions.OVERWRITE_MODE.key(), Boolean.toString(newOverwrite));
      return this;
    }

    public Builder<T> flinkConf(ReadableConfig config) {
      this.readableConfig = config;
      return this;
    }

    /**
     * Configuring the write parallel number for iceberg stream writer.
     *
     * @param newWriteParallelism the number of parallel iceberg stream writer.
     * @return {@link DynamicIcebergSink.Builder} to connect the iceberg table.
     */
    public Builder<T> writeParallelism(int newWriteParallelism) {
      writeOptions.put(
          FlinkWriteOptions.WRITE_PARALLELISM.key(), Integer.toString(newWriteParallelism));
      return this;
    }

    /**
     * Set the uid prefix for IcebergSink operators. Note that IcebergSink internally consists of
     * multiple operators (like writer, committer, aggregator) Actual operator uid will be appended
     * with a suffix like "uidPrefix-writer".
     *
     * <p>If provided, this prefix is also applied to operator names.
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
     * @param newPrefix prefix for Flink sink operator uid and name
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder<T> uidPrefix(String newPrefix) {
      this.uidPrefix = newPrefix;
      return this;
    }

    public Builder<T> snapshotProperties(Map<String, String> properties) {
      snapshotSummary.putAll(properties);
      return this;
    }

    public Builder<T> setSnapshotProperty(String property, String value) {
      snapshotSummary.put(property, value);
      return this;
    }

    public Builder<T> toBranch(String branch) {
      writeOptions.put(FlinkWriteOptions.BRANCH.key(), branch);
      return this;
    }

    public Builder<T> immediateTableUpdate(boolean newImmediateUpdate) {
      this.immediateUpdate = newImmediateUpdate;
      return this;
    }

    /** Maximum size of the caches used in Dynamic Sink for table data and serializers. */
    public Builder<T> cacheMaxSize(int maxSize) {
      this.cacheMaximumSize = maxSize;
      return this;
    }

    /** Maximum interval for cache items renewals. */
    public Builder<T> cacheRefreshMs(long refreshMs) {
      this.cacheRefreshMs = refreshMs;
      return this;
    }

    private String operatorName(String suffix) {
      return uidPrefix != null ? uidPrefix + "-" + suffix : suffix;
    }

    public DynamicIcebergSink build() {

      Preconditions.checkArgument(
          converter != null, "Please use withConverter() to convert the input DataStream.");
      Preconditions.checkNotNull(catalogLoader, "Catalog loader shouldn't be null");

      // Init the `flinkWriteConf` here, so we can do the checks
      FlinkWriteConf flinkWriteConf = new FlinkWriteConf(writeOptions, readableConfig);

      Map<String, String> writeProperties =
          writeProperties(flinkWriteConf.dataFileFormat(), flinkWriteConf);

      uidPrefix = Optional.ofNullable(uidPrefix).orElse("");

      // FlinkWriteConf properties needed to be set separately, so we do not have to serialize the
      // full conf
      return new DynamicIcebergSink(
          catalogLoader, snapshotSummary, uidPrefix, writeProperties, flinkWriteConf);
    }

    /**
     * Append the iceberg sink operators to write records to iceberg table.
     *
     * @return {@link DataStreamSink} for sink.
     */
    public DataStreamSink<DynamicRecordInternal> append() {
      DynamicRecordInternalType type =
          new DynamicRecordInternalType(catalogLoader, false, cacheMaximumSize);
      DynamicIcebergSink sink = build();
      SingleOutputStreamOperator<DynamicRecordInternal> converted =
          input
              .process(
                  new DynamicRecordProcessor<>(
                      converter, catalogLoader, immediateUpdate, cacheMaximumSize, cacheRefreshMs))
              .uid(prefixIfNotNull(uidPrefix, "-converter"))
              .name(operatorName("Converter"))
              .returns(type);

      DataStreamSink<DynamicRecordInternal> rowDataDataStreamSink =
          converted
              .getSideOutput(
                  new OutputTag<>(
                      DynamicRecordProcessor.DYNAMIC_TABLE_UPDATE_STREAM,
                      new DynamicRecordInternalType(catalogLoader, true, cacheMaximumSize)))
              .keyBy((KeySelector<DynamicRecordInternal, String>) DynamicRecordInternal::tableName)
              .map(new DynamicTableUpdateOperator(catalogLoader, cacheMaximumSize, cacheRefreshMs))
              .uid(prefixIfNotNull(uidPrefix, "-updater"))
              .name(operatorName("Updater"))
              .returns(type)
              .union(converted)
              .sinkTo(sink)
              .uid(prefixIfNotNull(uidPrefix, "-sink"));
      if (sink.flinkWriteConf.writeParallelism() != null) {
        rowDataDataStreamSink.setParallelism(sink.flinkWriteConf.writeParallelism());
      }

      return rowDataDataStreamSink;
    }
  }

  /**
   * Based on the {@link FileFormat} overwrites the table level compression properties for the table
   * write.
   *
   * @param format The FileFormat to use
   * @param conf The write configuration
   * @return The properties to use for writing
   */
  private static Map<String, String> writeProperties(FileFormat format, FlinkWriteConf conf) {
    Map<String, String> writeProperties = Maps.newHashMap();

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

  DataStream<DynamicRecordInternal> distributeDataStream(DataStream<DynamicRecordInternal> input) {
    return input.keyBy(DynamicRecordInternal::writerKey);
  }

  private static String prefixIfNotNull(String uidPrefix, String suffix) {
    return uidPrefix != null ? uidPrefix + "-" + suffix : suffix;
  }

  /**
   * Initialize a {@link IcebergSink.Builder} to export the data from input data stream with {@link
   * RowData}s into iceberg table.
   *
   * @param input the source input data stream with {@link RowData}s.
   * @return {@link IcebergSink.Builder} to connect the iceberg table.
   */
  public static <T> Builder<T> forInput(DataStream<T> input) {
    return new Builder<T>().forInput(input);
  }
}
