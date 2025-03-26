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
package org.apache.iceberg.orc;

import static org.apache.iceberg.TableProperties.DELETE_ORC_BLOCK_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.DELETE_ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.DELETE_ORC_COMPRESSION_STRATEGY;
import static org.apache.iceberg.TableProperties.DELETE_ORC_STRIPE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.DELETE_ORC_WRITE_BATCH_SIZE;
import static org.apache.iceberg.TableProperties.ORC_BLOCK_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.ORC_BLOCK_SIZE_BYTES_DEFAULT;
import static org.apache.iceberg.TableProperties.ORC_BLOOM_FILTER_COLUMNS;
import static org.apache.iceberg.TableProperties.ORC_BLOOM_FILTER_COLUMNS_DEFAULT;
import static org.apache.iceberg.TableProperties.ORC_BLOOM_FILTER_FPP;
import static org.apache.iceberg.TableProperties.ORC_BLOOM_FILTER_FPP_DEFAULT;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION_DEFAULT;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION_STRATEGY;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION_STRATEGY_DEFAULT;
import static org.apache.iceberg.TableProperties.ORC_STRIPE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.ORC_STRIPE_SIZE_BYTES_DEFAULT;
import static org.apache.iceberg.TableProperties.ORC_WRITE_BATCH_SIZE;
import static org.apache.iceberg.TableProperties.ORC_WRITE_BATCH_SIZE_DEFAULT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.orc.GenericOrcWriters;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.encryption.NativeEncryptionInputFile;
import org.apache.iceberg.encryption.NativeEncryptionOutputFile;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFile.CompressionStrategy;
import org.apache.orc.OrcFile.ReaderOptions;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class ORC {

  /**
   * @deprecated use {@link TableProperties#ORC_WRITE_BATCH_SIZE} instead
   */
  @Deprecated private static final String VECTOR_ROW_BATCH_SIZE = "iceberg.orc.vectorbatch.size";

  private ORC() {}

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link #appender(EncryptedOutputFile)}
   *     instead.
   */
  @Deprecated
  public static WriteBuilder write(OutputFile file) {
    return new WriteBuilder(file);
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link #appender(EncryptedOutputFile)}
   *     instead.
   */
  @Deprecated
  public static WriteBuilder write(EncryptedOutputFile file) {
    Preconditions.checkState(
        !(file instanceof NativeEncryptionOutputFile), "Native ORC encryption is not supported");
    return new WriteBuilder(file.encryptingOutputFile());
  }

  public static <E> AppenderBuilder<E> appender(EncryptedOutputFile file) {
    Preconditions.checkState(
        !(file instanceof NativeEncryptionOutputFile), "Native ORC encryption is not supported");
    return new AppenderBuilder<>(file.encryptingOutputFile());
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link AppenderBuilder} instead.
   */
  @Deprecated
  public static class WriteBuilder extends AppenderBuilderInternal<WriteBuilder, Object> {
    private WriteBuilder(OutputFile file) {
      super(file);
    }
  }

  public static class AppenderBuilder<E> extends AppenderBuilderInternal<AppenderBuilder<E>, E> {
    private AppenderBuilder(OutputFile file) {
      super(file);
    }
  }

  /** Will be removed when the {@link WriteBuilder} is removed. */
  @SuppressWarnings("unchecked")
  static class AppenderBuilderInternal<B extends AppenderBuilderInternal<B, E>, E>
      implements org.apache.iceberg.io.AppenderBuilder<B, E> {
    private final OutputFile file;
    private final Configuration conf;
    private Schema schema = null;
    private BiFunction<Schema, TypeDescription, OrcRowWriter<?>> createWriterFunc;
    private WriterFunction<E> writerFunction;
    private final Map<String, byte[]> metadata = Maps.newHashMap();
    private MetricsConfig metricsConfig;
    private Function<Map<String, String>, Context> createContextFunc = Context::dataContext;
    private final Map<String, String> config = Maps.newLinkedHashMap();
    private boolean overwrite = false;
    private E engineSchema;
    private Function<CharSequence, ?> pathTransformFunc;

    private AppenderBuilderInternal(OutputFile file) {
      this.file = file;
      if (file instanceof HadoopOutputFile) {
        this.conf = new Configuration(((HadoopOutputFile) file).getConf());
      } else {
        this.conf = new Configuration();
      }
    }

    /**
     * @deprecated Since 1.10.0, will be removed in 1.11.0. Use specific methods {@link
     *     #schema(org.apache.iceberg.Schema)}, {@link #set(String, String)}, {@link
     *     #metricsConfig(MetricsConfig)} instead.
     */
    @Deprecated
    public B forTable(Table table) {
      schema(table.schema());
      setAll(table.properties());
      metricsConfig(MetricsConfig.forTable(table));
      return (B) this;
    }

    /**
     * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link #meta(String, String)}
     *     instead.
     */
    @Deprecated
    public B metadata(String property, String value) {
      metadata.put(property, value.getBytes(StandardCharsets.UTF_8));
      return (B) this;
    }

    @Override
    public B set(String property, String value) {
      config.put(property, value);
      return (B) this;
    }

    /**
     * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link
     *     #writerFunction(WriterFunction)} instead.
     */
    @Deprecated
    public B createWriterFunc(
        BiFunction<Schema, TypeDescription, OrcRowWriter<?>> newWriterFunction) {
      Preconditions.checkState(
          writerFunction == null, "Cannot set multiple writer builder functions");
      this.createWriterFunc = newWriterFunction;
      return (B) this;
    }

    public B writerFunction(WriterFunction<E> newWriterFunction) {
      Preconditions.checkState(
          createWriterFunc == null, "Cannot set multiple writer builder functions");
      this.writerFunction = newWriterFunction;
      return (B) this;
    }

    public B pathTransformFunc(Function<CharSequence, ?> newPathTransformFunc) {
      this.pathTransformFunc = newPathTransformFunc;
      return (B) this;
    }

    /**
     * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link #set(String, String)}
     *     instead.
     */
    @Deprecated
    public B setAll(Map<String, String> properties) {
      config.putAll(properties);
      return (B) this;
    }

    @Override
    public B meta(String property, String value) {
      metadata.put(property, value.getBytes(StandardCharsets.UTF_8));
      return (B) this;
    }

    @Override
    public B schema(Schema newSchema) {
      this.schema = newSchema;
      return (B) this;
    }

    @Override
    public B engineSchema(E newEngineSchema) {
      this.engineSchema = newEngineSchema;
      return (B) this;
    }

    /**
     * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link #overwrite(boolean)} instead.
     */
    @Deprecated
    public B overwrite() {
      return overwrite(true);
    }

    @Override
    public B overwrite(boolean enabled) {
      this.overwrite = enabled;
      return (B) this;
    }

    @Override
    public B metricsConfig(MetricsConfig newMetricsConfig) {
      this.metricsConfig = newMetricsConfig;
      return (B) this;
    }

    // supposed to always be a private method used strictly by data and delete write builders
    // package-protected because of inheritance until deprecation of the WriteBuilder
    B createContextFunc(Function<Map<String, String>, Context> newCreateContextFunc) {
      this.createContextFunc = newCreateContextFunc;
      return (B) this;
    }

    @Override
    public <D> FileAppender<D> build(WriteMode mode) throws IOException {
      Preconditions.checkState(writerFunction != null, "Writer function has to be set.");
      switch (mode) {
        case APPENDER:
        case DATA_WRITER:
          this.createWriterFunc =
              (icebergSchema, typeDescription) ->
                  writerFunction.write(icebergSchema, typeDescription, engineSchema);
          this.createContextFunc = Context::dataContext;
          break;
        case EQUALITY_DELETE_WRITER:
          this.createWriterFunc =
              (icebergSchema, typeDescription) ->
                  writerFunction.write(icebergSchema, typeDescription, engineSchema);
          this.createContextFunc = Context::deleteContext;
          break;
        case POSITION_DELETE_WRITER:
          this.createWriterFunc =
              (icebergSchema, typeDescription) ->
                  GenericOrcWriters.positionDelete(
                      GenericOrcWriter.buildWriter(icebergSchema, typeDescription),
                      Function.identity());
          this.createContextFunc = Context::deleteContext;
          break;
        case POSITION_DELETE_WITH_ROW_WRITER:
          Preconditions.checkState(
              pathTransformFunc != null, "Path transform function has to be set.");
          this.createWriterFunc =
              (icebergSchema, typeDescription) ->
                  GenericOrcWriters.positionDelete(
                      writerFunction.write(icebergSchema, typeDescription, engineSchema),
                      pathTransformFunc);
          this.createContextFunc = Context::deleteContext;
          break;
        default:
          throw new IllegalArgumentException("Not supported mode: " + mode);
      }

      return build();
    }

    /**
     * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link #build(WriteMode)} instead.
     */
    @Deprecated
    public <D> FileAppender<D> build() {
      Preconditions.checkNotNull(schema, "Schema is required");

      for (Map.Entry<String, String> entry : config.entrySet()) {
        this.conf.set(entry.getKey(), entry.getValue());
      }

      // for compatibility
      if (conf.get(VECTOR_ROW_BATCH_SIZE) != null && config.get(ORC_WRITE_BATCH_SIZE) == null) {
        config.put(ORC_WRITE_BATCH_SIZE, conf.get(VECTOR_ROW_BATCH_SIZE));
      }

      // Map Iceberg properties to pass down to the ORC writer
      Context context = createContextFunc.apply(config);

      OrcConf.STRIPE_SIZE.setLong(conf, context.stripeSize());
      OrcConf.BLOCK_SIZE.setLong(conf, context.blockSize());
      OrcConf.COMPRESS.setString(conf, context.compressionKind().name());
      OrcConf.COMPRESSION_STRATEGY.setString(conf, context.compressionStrategy().name());
      OrcConf.OVERWRITE_OUTPUT_FILE.setBoolean(conf, overwrite);
      OrcConf.BLOOM_FILTER_COLUMNS.setString(conf, context.bloomFilterColumns());
      OrcConf.BLOOM_FILTER_FPP.setDouble(conf, context.bloomFilterFpp());

      return new OrcFileAppender<>(
          schema,
          this.file,
          createWriterFunc,
          conf,
          metadata,
          context.vectorizedRowBatchSize(),
          metricsConfig);
    }

    // protected because of inheritance until deprecation of the WriteBuilder
    static class Context {
      private final long stripeSize;
      private final long blockSize;
      private final int vectorizedRowBatchSize;
      private final CompressionKind compressionKind;
      private final CompressionStrategy compressionStrategy;
      private final String bloomFilterColumns;
      private final double bloomFilterFpp;

      public long stripeSize() {
        return stripeSize;
      }

      public long blockSize() {
        return blockSize;
      }

      public int vectorizedRowBatchSize() {
        return vectorizedRowBatchSize;
      }

      public CompressionKind compressionKind() {
        return compressionKind;
      }

      public CompressionStrategy compressionStrategy() {
        return compressionStrategy;
      }

      public String bloomFilterColumns() {
        return bloomFilterColumns;
      }

      public double bloomFilterFpp() {
        return bloomFilterFpp;
      }

      private Context(
          long stripeSize,
          long blockSize,
          int vectorizedRowBatchSize,
          CompressionKind compressionKind,
          CompressionStrategy compressionStrategy,
          String bloomFilterColumns,
          double bloomFilterFpp) {
        this.stripeSize = stripeSize;
        this.blockSize = blockSize;
        this.vectorizedRowBatchSize = vectorizedRowBatchSize;
        this.compressionKind = compressionKind;
        this.compressionStrategy = compressionStrategy;
        this.bloomFilterColumns = bloomFilterColumns;
        this.bloomFilterFpp = bloomFilterFpp;
      }

      static Context dataContext(Map<String, String> config) {
        long stripeSize =
            PropertyUtil.propertyAsLong(
                config, OrcConf.STRIPE_SIZE.getAttribute(), ORC_STRIPE_SIZE_BYTES_DEFAULT);
        stripeSize = PropertyUtil.propertyAsLong(config, ORC_STRIPE_SIZE_BYTES, stripeSize);
        Preconditions.checkArgument(stripeSize > 0, "Stripe size must be > 0");

        long blockSize =
            PropertyUtil.propertyAsLong(
                config, OrcConf.BLOCK_SIZE.getAttribute(), ORC_BLOCK_SIZE_BYTES_DEFAULT);
        blockSize = PropertyUtil.propertyAsLong(config, ORC_BLOCK_SIZE_BYTES, blockSize);
        Preconditions.checkArgument(blockSize > 0, "Block size must be > 0");

        int vectorizedRowBatchSize =
            PropertyUtil.propertyAsInt(config, ORC_WRITE_BATCH_SIZE, ORC_WRITE_BATCH_SIZE_DEFAULT);
        Preconditions.checkArgument(
            vectorizedRowBatchSize > 0, "VectorizedRow batch size must be > 0");

        String codecAsString =
            PropertyUtil.propertyAsString(
                config, OrcConf.COMPRESS.getAttribute(), ORC_COMPRESSION_DEFAULT);
        codecAsString = PropertyUtil.propertyAsString(config, ORC_COMPRESSION, codecAsString);
        CompressionKind compressionKind = toCompressionKind(codecAsString);

        String strategyAsString =
            PropertyUtil.propertyAsString(
                config,
                OrcConf.COMPRESSION_STRATEGY.getAttribute(),
                ORC_COMPRESSION_STRATEGY_DEFAULT);
        strategyAsString =
            PropertyUtil.propertyAsString(config, ORC_COMPRESSION_STRATEGY, strategyAsString);
        CompressionStrategy compressionStrategy = toCompressionStrategy(strategyAsString);

        String bloomFilterColumns =
            PropertyUtil.propertyAsString(
                config,
                OrcConf.BLOOM_FILTER_COLUMNS.getAttribute(),
                ORC_BLOOM_FILTER_COLUMNS_DEFAULT);
        bloomFilterColumns =
            PropertyUtil.propertyAsString(config, ORC_BLOOM_FILTER_COLUMNS, bloomFilterColumns);

        double bloomFilterFpp =
            PropertyUtil.propertyAsDouble(
                config, OrcConf.BLOOM_FILTER_FPP.getAttribute(), ORC_BLOOM_FILTER_FPP_DEFAULT);
        bloomFilterFpp =
            PropertyUtil.propertyAsDouble(config, ORC_BLOOM_FILTER_FPP, bloomFilterFpp);
        Preconditions.checkArgument(
            bloomFilterFpp > 0.0 && bloomFilterFpp < 1.0,
            "Bloom filter fpp must be > 0.0 and < 1.0");

        return new Context(
            stripeSize,
            blockSize,
            vectorizedRowBatchSize,
            compressionKind,
            compressionStrategy,
            bloomFilterColumns,
            bloomFilterFpp);
      }

      static Context deleteContext(Map<String, String> config) {
        Context dataContext = dataContext(config);

        long stripeSize =
            PropertyUtil.propertyAsLong(
                config, DELETE_ORC_STRIPE_SIZE_BYTES, dataContext.stripeSize());

        long blockSize =
            PropertyUtil.propertyAsLong(
                config, DELETE_ORC_BLOCK_SIZE_BYTES, dataContext.blockSize());

        int vectorizedRowBatchSize =
            PropertyUtil.propertyAsInt(
                config, DELETE_ORC_WRITE_BATCH_SIZE, dataContext.vectorizedRowBatchSize());

        String codecAsString = config.get(DELETE_ORC_COMPRESSION);
        CompressionKind compressionKind =
            codecAsString != null
                ? toCompressionKind(codecAsString)
                : dataContext.compressionKind();

        String strategyAsString = config.get(DELETE_ORC_COMPRESSION_STRATEGY);
        CompressionStrategy compressionStrategy =
            strategyAsString != null
                ? toCompressionStrategy(strategyAsString)
                : dataContext.compressionStrategy();

        return new Context(
            stripeSize,
            blockSize,
            vectorizedRowBatchSize,
            compressionKind,
            compressionStrategy,
            dataContext.bloomFilterColumns(),
            dataContext.bloomFilterFpp());
      }

      private static CompressionKind toCompressionKind(String codecAsString) {
        try {
          return CompressionKind.valueOf(codecAsString.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("Unsupported compression codec: " + codecAsString);
        }
      }

      private static CompressionStrategy toCompressionStrategy(String strategyAsString) {
        try {
          return CompressionStrategy.valueOf(strategyAsString.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException(
              "Unsupported compression strategy: " + strategyAsString);
        }
      }
    }
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use ObjectModelRegistry.writerBuilder
   *     instead.
   */
  @Deprecated
  public static DataWriteBuilder writeData(OutputFile file) {
    return new DataWriteBuilder(file);
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use ObjectModelRegistry.writerBuilder
   *     instead.
   */
  @Deprecated
  public static DataWriteBuilder writeData(EncryptedOutputFile file) {
    Preconditions.checkState(
        !(file instanceof NativeEncryptionOutputFile), "Native ORC encryption is not supported");
    return new DataWriteBuilder(file.encryptingOutputFile());
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use ObjectModelRegistry.writerBuilder
   *     instead.
   */
  @Deprecated
  public static class DataWriteBuilder {
    private final WriteBuilder appenderBuilder;
    private final String location;
    private PartitionSpec spec = null;
    private StructLike partition = null;
    private EncryptionKeyMetadata keyMetadata = null;
    private SortOrder sortOrder = null;

    private DataWriteBuilder(OutputFile file) {
      this.appenderBuilder = write(file);
      this.location = file.location();
    }

    public DataWriteBuilder forTable(Table table) {
      schema(table.schema());
      withSpec(table.spec());
      setAll(table.properties());
      metricsConfig(MetricsConfig.forTable(table));
      return this;
    }

    public DataWriteBuilder schema(Schema newSchema) {
      appenderBuilder.schema(newSchema);
      return this;
    }

    public DataWriteBuilder set(String property, String value) {
      appenderBuilder.set(property, value);
      return this;
    }

    public DataWriteBuilder setAll(Map<String, String> properties) {
      appenderBuilder.setAll(properties);
      return this;
    }

    public DataWriteBuilder meta(String property, String value) {
      appenderBuilder.metadata(property, value);
      return this;
    }

    public DataWriteBuilder overwrite() {
      return overwrite(true);
    }

    public DataWriteBuilder overwrite(boolean enabled) {
      appenderBuilder.overwrite(enabled);
      return this;
    }

    public DataWriteBuilder metricsConfig(MetricsConfig newMetricsConfig) {
      appenderBuilder.metricsConfig(newMetricsConfig);
      return this;
    }

    public DataWriteBuilder createWriterFunc(
        BiFunction<Schema, TypeDescription, OrcRowWriter<?>> writerFunction) {
      appenderBuilder.createWriterFunc(writerFunction);
      return this;
    }

    public DataWriteBuilder withSpec(PartitionSpec newSpec) {
      this.spec = newSpec;
      return this;
    }

    public DataWriteBuilder withPartition(StructLike newPartition) {
      this.partition = newPartition;
      return this;
    }

    public DataWriteBuilder withKeyMetadata(EncryptionKeyMetadata metadata) {
      this.keyMetadata = metadata;
      return this;
    }

    public DataWriteBuilder withSortOrder(SortOrder newSortOrder) {
      this.sortOrder = newSortOrder;
      return this;
    }

    public <T> DataWriter<T> build() {
      Preconditions.checkArgument(spec != null, "Cannot create data writer without spec");
      Preconditions.checkArgument(
          spec.isUnpartitioned() || partition != null,
          "Partition must not be null when creating data writer for partitioned spec");

      FileAppender<T> fileAppender = appenderBuilder.build();
      return new DataWriter<>(
          fileAppender, FileFormat.ORC, location, spec, partition, keyMetadata, sortOrder);
    }
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use
   *     ObjectModelRegistry.positionDeleteWriterBuilder and
   *     ObjectModelRegistry.equalityDeleteWriterBuilder instead.
   */
  @Deprecated
  public static DeleteWriteBuilder writeDeletes(OutputFile file) {
    return new DeleteWriteBuilder(file);
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use
   *     ObjectModelRegistry.positionDeleteWriterBuilder and
   *     ObjectModelRegistry.equalityDeleteWriterBuilder instead.
   */
  @Deprecated
  public static DeleteWriteBuilder writeDeletes(EncryptedOutputFile file) {
    Preconditions.checkState(
        !(file instanceof NativeEncryptionOutputFile), "Native ORC encryption is not supported");
    return new DeleteWriteBuilder(file.encryptingOutputFile());
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use
   *     ObjectModelRegistry.positionDeleteWriterBuilder and
   *     ObjectModelRegistry.equalityDeleteWriterBuilder instead.
   */
  @Deprecated
  public static class DeleteWriteBuilder {
    private final WriteBuilder appenderBuilder;
    private final String location;
    private BiFunction<Schema, TypeDescription, OrcRowWriter<?>> createWriterFunc = null;
    private Schema rowSchema = null;
    private PartitionSpec spec = null;
    private StructLike partition = null;
    private EncryptionKeyMetadata keyMetadata = null;
    private int[] equalityFieldIds = null;
    private SortOrder sortOrder;
    private Function<CharSequence, ?> pathTransformFunc = Function.identity();

    private DeleteWriteBuilder(OutputFile file) {
      this.appenderBuilder = write(file);
      this.location = file.location();
    }

    public DeleteWriteBuilder forTable(Table table) {
      rowSchema(table.schema());
      withSpec(table.spec());
      setAll(table.properties());
      metricsConfig(MetricsConfig.forTable(table));
      return this;
    }

    public DeleteWriteBuilder set(String property, String value) {
      appenderBuilder.set(property, value);
      return this;
    }

    public DeleteWriteBuilder setAll(Map<String, String> properties) {
      appenderBuilder.setAll(properties);
      return this;
    }

    public DeleteWriteBuilder meta(String property, String value) {
      appenderBuilder.metadata(property, value);
      return this;
    }

    public DeleteWriteBuilder overwrite() {
      return overwrite(true);
    }

    public DeleteWriteBuilder overwrite(boolean enabled) {
      appenderBuilder.overwrite(enabled);
      return this;
    }

    public DeleteWriteBuilder metricsConfig(MetricsConfig newMetricsConfig) {
      appenderBuilder.metricsConfig(newMetricsConfig);
      return this;
    }

    public DeleteWriteBuilder createWriterFunc(
        BiFunction<Schema, TypeDescription, OrcRowWriter<?>> newWriterFunc) {
      this.createWriterFunc = newWriterFunc;
      return this;
    }

    public DeleteWriteBuilder rowSchema(Schema newSchema) {
      this.rowSchema = newSchema;
      return this;
    }

    public DeleteWriteBuilder withSpec(PartitionSpec newSpec) {
      this.spec = newSpec;
      return this;
    }

    public DeleteWriteBuilder withPartition(StructLike key) {
      this.partition = key;
      return this;
    }

    public DeleteWriteBuilder withKeyMetadata(EncryptionKeyMetadata metadata) {
      this.keyMetadata = metadata;
      return this;
    }

    public DeleteWriteBuilder equalityFieldIds(List<Integer> fieldIds) {
      this.equalityFieldIds = ArrayUtil.toIntArray(fieldIds);
      return this;
    }

    public DeleteWriteBuilder equalityFieldIds(int... fieldIds) {
      this.equalityFieldIds = fieldIds;
      return this;
    }

    public DeleteWriteBuilder transformPaths(Function<CharSequence, ?> newPathTransformFunc) {
      this.pathTransformFunc = newPathTransformFunc;
      return this;
    }

    public DeleteWriteBuilder withSortOrder(SortOrder newSortOrder) {
      this.sortOrder = newSortOrder;
      return this;
    }

    public <T> EqualityDeleteWriter<T> buildEqualityWriter() {
      Preconditions.checkState(
          rowSchema != null, "Cannot create equality delete file without a schema");
      Preconditions.checkState(
          equalityFieldIds != null, "Cannot create equality delete file without delete field ids");
      Preconditions.checkState(
          createWriterFunc != null,
          "Cannot create equality delete file unless createWriterFunc is set");
      Preconditions.checkArgument(
          spec != null, "Spec must not be null when creating equality delete writer");
      Preconditions.checkArgument(
          spec.isUnpartitioned() || partition != null,
          "Partition must not be null for partitioned writes");

      meta("delete-type", "equality");
      meta(
          "delete-field-ids",
          IntStream.of(equalityFieldIds)
              .mapToObj(Objects::toString)
              .collect(Collectors.joining(", ")));

      // the appender uses the row schema without extra columns
      appenderBuilder.schema(rowSchema);
      appenderBuilder.createWriterFunc(createWriterFunc);
      appenderBuilder.createContextFunc(AppenderBuilderInternal.Context::deleteContext);

      return new EqualityDeleteWriter<>(
          appenderBuilder.build(),
          FileFormat.ORC,
          location,
          spec,
          partition,
          keyMetadata,
          sortOrder,
          equalityFieldIds);
    }

    public <T> PositionDeleteWriter<T> buildPositionWriter() {
      Preconditions.checkState(
          equalityFieldIds == null, "Cannot create position delete file using delete field ids");
      Preconditions.checkArgument(
          spec != null, "Spec must not be null when creating position delete writer");
      Preconditions.checkArgument(
          spec.isUnpartitioned() || partition != null,
          "Partition must not be null for partitioned writes");
      Preconditions.checkArgument(
          rowSchema == null || createWriterFunc != null,
          "Create function should be provided if we write row data");

      meta("delete-type", "position");

      if (rowSchema != null && createWriterFunc != null) {
        Schema deleteSchema = DeleteSchemaUtil.posDeleteSchema(rowSchema);
        appenderBuilder.schema(deleteSchema);

        appenderBuilder.createWriterFunc(
            (schema, typeDescription) ->
                GenericOrcWriters.positionDelete(
                    createWriterFunc.apply(deleteSchema, typeDescription), pathTransformFunc));
      } else {
        appenderBuilder.schema(DeleteSchemaUtil.pathPosSchema());

        // We ignore the 'createWriterFunc' and 'rowSchema' even if is provided, since we do not
        // write row data itself
        appenderBuilder.createWriterFunc(
            (schema, typeDescription) ->
                GenericOrcWriters.positionDelete(
                    GenericOrcWriter.buildWriter(schema, typeDescription), Function.identity()));
      }

      appenderBuilder.createContextFunc(AppenderBuilderInternal.Context::deleteContext);

      return new PositionDeleteWriter<>(
          appenderBuilder.build(), FileFormat.ORC, location, spec, partition, keyMetadata);
    }
  }

  public static ReadBuilder read(InputFile file) {
    Preconditions.checkState(
        !(file instanceof NativeEncryptionInputFile), "Native ORC encryption is not supported");

    return new ReadBuilder(file);
  }

  public static class ReadBuilder implements org.apache.iceberg.io.ReadBuilder<ReadBuilder> {
    private final InputFile file;
    private final Configuration conf;
    private Schema schema = null;
    private Long start = null;
    private Long length = null;
    private Expression filter = null;
    private boolean filterCaseSensitive = true;
    private NameMapping nameMapping = null;

    private Function<TypeDescription, OrcRowReader<?>> readerFunc;
    private Function<TypeDescription, OrcBatchReader<?>> batchedReaderFunc;
    private ReaderFunction<?> readerFunction;
    private BatchReaderFunction<?> batchReaderFunction;
    private Map<Integer, ?> constantFieldAccessors = ImmutableMap.of();

    private ReadBuilder(InputFile file) {
      Preconditions.checkNotNull(file, "Input file cannot be null");
      this.file = file;
      if (file instanceof HadoopInputFile) {
        this.conf = new Configuration(((HadoopInputFile) file).getConf());
      } else {
        this.conf = new Configuration();
      }

      // We need to turn positional schema evolution off since we use column name based schema
      // evolution for projection
      this.conf.setBoolean(OrcConf.FORCE_POSITIONAL_EVOLUTION.getHiveConfName(), false);
    }

    /**
     * Restricts the read to the given range: [start, start + length).
     *
     * @param newStart the start position for this read
     * @param newLength the length of the range this read should scan
     * @return this builder for method chaining
     */
    @Override
    public ReadBuilder split(long newStart, long newLength) {
      this.start = newStart;
      this.length = newLength;
      return this;
    }

    @Override
    public ReadBuilder project(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    /**
     * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link #filter(Expression, boolean)}
     *     instead.
     */
    @Deprecated
    public ReadBuilder caseSensitive(boolean newCaseSensitive) {
      OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(this.conf, newCaseSensitive);
      this.filterCaseSensitive = newCaseSensitive;
      return this;
    }

    /**
     * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link #set(String, String)}
     *     instead.
     */
    @Deprecated
    public ReadBuilder config(String property, String value) {
      conf.set(property, value);
      return this;
    }

    @Override
    public ReadBuilder set(String property, String value) {
      conf.set(property, value);
      return this;
    }

    /**
     * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link
     *     #readerFunction(ReaderFunction)} instead.
     */
    @Deprecated
    public ReadBuilder createReaderFunc(Function<TypeDescription, OrcRowReader<?>> newReaderFunc) {
      Preconditions.checkState(
          batchedReaderFunc == null && readerFunction == null && batchReaderFunction == null,
          "Cannot set multiple read builder functions");
      this.readerFunc = newReaderFunc;
      return this;
    }

    @Override
    public ReadBuilder filter(Expression newFilter, boolean newFilterCaseSensitive) {
      OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(this.conf, newFilterCaseSensitive);
      this.filterCaseSensitive = newFilterCaseSensitive;
      this.filter = newFilter;
      return this;
    }

    @Override
    public ReadBuilder reuseContainers(boolean newReuseContainers) {
      // ORC always reuses the containers so ignore this
      return this;
    }

    /**
     * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link
     *     #batchReaderFunction(BatchReaderFunction)} instead.
     */
    @Deprecated
    public ReadBuilder createBatchedReaderFunc(
        Function<TypeDescription, OrcBatchReader<?>> newReaderFunction) {
      Preconditions.checkState(
          readerFunc == null && readerFunction == null && batchReaderFunction == null,
          "Cannot set multiple read builder functions");
      this.batchedReaderFunc = newReaderFunction;
      return this;
    }

    public <D> ReadBuilder readerFunction(ReaderFunction<D> newReaderFunction) {
      Preconditions.checkState(
          readerFunc == null && batchedReaderFunc == null && batchReaderFunction == null,
          "Cannot set multiple read builder functions");
      this.readerFunction = newReaderFunction;
      return this;
    }

    public <D> ReadBuilder batchReaderFunction(BatchReaderFunction<D> newReaderFunction) {
      Preconditions.checkState(
          readerFunc == null && batchedReaderFunc == null && readerFunction == null,
          "Cannot set multiple read builder functions");
      this.batchReaderFunction = newReaderFunction;
      return this;
    }

    @Override
    public ReadBuilder constantFieldAccessors(Map<Integer, ?> newConstantFieldAccessors) {
      this.constantFieldAccessors = newConstantFieldAccessors;
      return this;
    }

    /**
     * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link #set(String, String)} with
     *     {@link ReadBuilder#RECORDS_PER_BATCH_KEY} instead.
     */
    @Deprecated
    public ReadBuilder recordsPerBatch(int numRecordsPerBatch) {
      return set(RECORDS_PER_BATCH_KEY, String.valueOf(numRecordsPerBatch));
    }

    @Override
    public ReadBuilder withNameMapping(NameMapping newNameMapping) {
      this.nameMapping = newNameMapping;
      return this;
    }

    @Override
    public <D> CloseableIterable<D> build() {
      Preconditions.checkNotNull(schema, "Schema is required");
      Function<TypeDescription, OrcRowReader<?>> reader =
          readerFunc != null
              ? readerFunc
              : readerFunction != null
                  ? fileType -> readerFunction.read(schema, fileType, constantFieldAccessors)
                  : null;
      Function<TypeDescription, OrcBatchReader<?>> batchReader =
          batchedReaderFunc != null
              ? batchedReaderFunc
              : batchReaderFunction != null
                  ? fileType -> batchReaderFunction.read(schema, fileType, constantFieldAccessors)
                  : null;
      return new OrcIterable<>(
          file,
          conf,
          // This is a behavioral change. Previously there were an error if constant columns were
          // present in the schema, now they are removed and the correct reader is created
          TypeUtil.selectNot(
              schema,
              Sets.union(constantFieldAccessors.keySet(), MetadataColumns.metadataFieldIds())),
          nameMapping,
          start,
          length,
          reader,
          filterCaseSensitive,
          filter,
          batchReader,
          conf.getInt(RECORDS_PER_BATCH_KEY, VectorizedRowBatch.DEFAULT_SIZE));
    }
  }

  static Reader newFileReader(InputFile file, Configuration config) {
    ReaderOptions readerOptions = OrcFile.readerOptions(config).useUTCTimestamp(true);
    if (file instanceof HadoopInputFile) {
      readerOptions.filesystem(((HadoopInputFile) file).getFileSystem());
    } else {
      // In case of any other InputFile we wrap the InputFile with InputFileSystem that only
      // supports the creation of an InputStream. To prevent a file status call to determine the
      // length we supply the length as input
      readerOptions.filesystem(new FileIOFSUtil.InputFileSystem(file)).maxLength(file.getLength());
    }
    try {
      return OrcFile.createReader(new Path(file.location()), readerOptions);
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to open file: %s", file.location());
    }
  }

  static Writer newFileWriter(
      OutputFile file, OrcFile.WriterOptions options, Map<String, byte[]> metadata) {
    if (file instanceof HadoopOutputFile) {
      options.fileSystem(((HadoopOutputFile) file).getFileSystem());
    } else {
      options.fileSystem(new FileIOFSUtil.OutputFileSystem(file));
    }
    final Path locPath = new Path(file.location());
    final Writer writer;

    try {
      writer = OrcFile.createWriter(locPath, options);
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Can't create file %s", locPath);
    }

    metadata.forEach((key, value) -> writer.addUserMetadata(key, ByteBuffer.wrap(value)));

    return writer;
  }

  public interface WriterFunction<E> {
    OrcRowWriter<?> write(Schema schema, TypeDescription messageType, E nativeSchema);
  }

  public interface ReaderFunction<D> {
    OrcRowReader<D> read(
        Schema schema, TypeDescription messageType, Map<Integer, ?> constantFieldAccessors);
  }

  public interface BatchReaderFunction<D> {
    OrcBatchReader<D> read(
        Schema schema, TypeDescription messageType, Map<Integer, ?> constantFieldAccessors);
  }
}
