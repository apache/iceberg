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
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.ObjectModelRegistry;
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
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link
   *     ObjectModelRegistry#writeBuilder(FileFormat, String, EncryptedOutputFile)} instead.
   */
  @Deprecated
  public static WriteBuilder write(OutputFile file) {
    return new WriteBuilder(file);
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link
   *     ObjectModelRegistry#writeBuilder(FileFormat, String, EncryptedOutputFile)} instead.
   */
  @Deprecated
  public static WriteBuilder write(EncryptedOutputFile file) {
    Preconditions.checkState(
        !(file instanceof NativeEncryptionOutputFile), "Native ORC encryption is not supported");
    return new WriteBuilder(file.encryptingOutputFile());
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link
   *     ObjectModelRegistry#writeBuilder(FileFormat, String, EncryptedOutputFile)} instead.
   */
  @Deprecated
  public static class WriteBuilder {
    private final WriteBuilderImpl<?, ?> impl;

    private WriteBuilder(OutputFile file) {
      this.impl = new WriteBuilderImpl<>(file, null);
    }

    public WriteBuilder forTable(Table table) {
      impl.fileSchema(table.schema())
          .set(table.properties())
          .metricsConfig(MetricsConfig.forTable(table));
      return this;
    }

    public WriteBuilder metadata(String property, String value) {
      impl.meta(property, value);
      return this;
    }

    public WriteBuilder set(String property, String value) {
      impl.set(property, value);
      return this;
    }

    public WriteBuilder createWriterFunc(
        BiFunction<Schema, TypeDescription, OrcRowWriter<?>> writerFunction) {
      impl.createWriterFunc = writerFunction;
      return this;
    }

    public WriteBuilder setAll(Map<String, String> properties) {
      impl.set(properties);
      return this;
    }

    public WriteBuilder schema(Schema newSchema) {
      impl.fileSchema(newSchema);
      return this;
    }

    public WriteBuilder overwrite() {
      return overwrite(true);
    }

    public WriteBuilder overwrite(boolean enabled) {
      impl.overwrite = enabled;
      return this;
    }

    public WriteBuilder metricsConfig(MetricsConfig newMetricsConfig) {
      impl.metricsConfig(newMetricsConfig);
      return this;
    }

    // supposed to always be a private method used strictly by data and delete write builders
    private void createContextFunc(
        Function<Map<String, String>, WriteBuilderImpl.Context> newCreateContextFunc) {
      impl.createContextFunc = newCreateContextFunc;
    }

    public <D> FileAppender<D> build() {
      return (FileAppender<D>) impl.build();
    }
  }

  static class WriteBuilderImpl<E, D>
      implements org.apache.iceberg.io.WriteBuilder<WriteBuilderImpl<E, D>, E, D> {
    private final OutputFile file;
    private final FileContent content;
    private final Configuration conf;
    private Schema schema = null;
    private BiFunction<Schema, TypeDescription, OrcRowWriter<?>> createWriterFunc;
    private ORCObjectModelFactory.WriterFunction<E> writerFunction;
    private final Map<String, byte[]> metadata = Maps.newHashMap();
    private MetricsConfig metricsConfig;
    private Function<Map<String, String>, Context> createContextFunc = Context::dataContext;
    private final Map<String, String> config = Maps.newLinkedHashMap();
    private boolean overwrite = false;
    private E engineSchema;
    private Function<CharSequence, ?> pathTransformFunc;

    WriteBuilderImpl(OutputFile file, FileContent content) {
      this.file = file;
      this.content = content;
      if (file instanceof HadoopOutputFile) {
        this.conf = new Configuration(((HadoopOutputFile) file).getConf());
      } else {
        this.conf = new Configuration();
      }
    }

    WriteBuilderImpl<E, D> writerFunction(
        ORCObjectModelFactory.WriterFunction<E> newWriterFunction) {
      Preconditions.checkState(
          createWriterFunc == null, "Cannot set multiple writer builder functions");
      this.writerFunction = newWriterFunction;
      return this;
    }

    WriteBuilderImpl<E, D> pathTransformFunc(Function<CharSequence, ?> newPathTransformFunc) {
      this.pathTransformFunc = newPathTransformFunc;
      return this;
    }

    @Override
    public WriteBuilderImpl<E, D> set(String property, String value) {
      config.put(property, value);
      return this;
    }

    @Override
    public WriteBuilderImpl<E, D> meta(String property, String value) {
      metadata.put(property, value.getBytes(StandardCharsets.UTF_8));
      return this;
    }

    @Override
    public WriteBuilderImpl<E, D> fileSchema(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    @Override
    public WriteBuilderImpl<E, D> modelSchema(E newModelSchema) {
      this.engineSchema = newModelSchema;
      return this;
    }

    @Override
    public WriteBuilderImpl<E, D> overwrite() {
      this.overwrite = true;
      return this;
    }

    @Override
    public WriteBuilderImpl<E, D> metricsConfig(MetricsConfig newMetricsConfig) {
      this.metricsConfig = newMetricsConfig;
      return this;
    }

    private void initWriterFunctionAndContext() {
      Preconditions.checkState(writerFunction != null, "Writer function has to be set.");
      switch (content) {
        case DATA:
          this.createWriterFunc =
              (icebergSchema, typeDescription) ->
                  writerFunction.write(icebergSchema, typeDescription, engineSchema);
          this.createContextFunc = Context::dataContext;
          break;
        case EQUALITY_DELETES:
          this.createWriterFunc =
              (icebergSchema, typeDescription) ->
                  writerFunction.write(icebergSchema, typeDescription, engineSchema);
          this.createContextFunc = Context::deleteContext;
          break;
        case POSITION_DELETES:
          this.createContextFunc = Context::deleteContext;
          if (schema.columns().size() == DeleteSchemaUtil.pathPosSchema().columns().size()) {
            // this is a position delete without rows
            this.createWriterFunc =
                (icebergSchema, typeDescription) ->
                    GenericOrcWriters.positionDelete(
                        GenericOrcWriter.buildWriter(icebergSchema, typeDescription),
                        Function.identity());
          } else {
            // this is a position delete with rows
            Preconditions.checkState(
                pathTransformFunc != null, "Path transform function has to be set.");
            this.createWriterFunc =
                (icebergSchema, typeDescription) ->
                    GenericOrcWriters.positionDelete(
                        writerFunction.write(icebergSchema, typeDescription, engineSchema),
                        pathTransformFunc);
          }
          break;
        default:
          throw new IllegalArgumentException("Not supported content: " + content);
      }
    }

    @Override
    public FileAppender<D> build() {
      Preconditions.checkNotNull(schema, "Schema is required");

      if (content != null) {
        initWriterFunctionAndContext();
      }

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
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link
   *     ObjectModelRegistry#writeBuilder(FileFormat, String, EncryptedOutputFile)} instead.
   */
  @Deprecated
  public static DataWriteBuilder writeData(OutputFile file) {
    return new DataWriteBuilder(file);
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link
   *     ObjectModelRegistry#writeBuilder(FileFormat, String, EncryptedOutputFile)} instead.
   */
  @Deprecated
  public static DataWriteBuilder writeData(EncryptedOutputFile file) {
    Preconditions.checkState(
        !(file instanceof NativeEncryptionOutputFile), "Native ORC encryption is not supported");
    return new DataWriteBuilder(file.encryptingOutputFile());
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link
   *     ObjectModelRegistry#writeBuilder(FileFormat, String, EncryptedOutputFile)} instead.
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

      return new DataWriter<>(
          appenderBuilder.build(),
          FileFormat.ORC,
          location,
          spec,
          partition,
          keyMetadata,
          sortOrder);
    }
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link
   *     ObjectModelRegistry#positionDeleteWriteBuilder(FileFormat, String, EncryptedOutputFile)}
   *     and {@link ObjectModelRegistry#equalityDeleteWriteBuilder(FileFormat, String,
   *     EncryptedOutputFile)} instead.
   */
  @Deprecated
  public static DeleteWriteBuilder writeDeletes(OutputFile file) {
    return new DeleteWriteBuilder(file);
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link
   *     ObjectModelRegistry#positionDeleteWriteBuilder(FileFormat, String, EncryptedOutputFile)}
   *     and {@link ObjectModelRegistry#equalityDeleteWriteBuilder(FileFormat, String,
   *     EncryptedOutputFile)} instead.
   */
  @Deprecated
  public static DeleteWriteBuilder writeDeletes(EncryptedOutputFile file) {
    Preconditions.checkState(
        !(file instanceof NativeEncryptionOutputFile), "Native ORC encryption is not supported");
    return new DeleteWriteBuilder(file.encryptingOutputFile());
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link
   *     ObjectModelRegistry#positionDeleteWriteBuilder(FileFormat, String, EncryptedOutputFile)}
   *     and {@link ObjectModelRegistry#equalityDeleteWriteBuilder(FileFormat, String,
   *     EncryptedOutputFile)} instead.
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
      appenderBuilder.createContextFunc(WriteBuilderImpl.Context::deleteContext);

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

      appenderBuilder.createContextFunc(WriteBuilderImpl.Context::deleteContext);

      return new PositionDeleteWriter<>(
          appenderBuilder.build(), FileFormat.ORC, location, spec, partition, keyMetadata);
    }
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use the {@link
   *     ObjectModelRegistry#readBuilder(FileFormat, String, InputFile)} instead.
   */
  @Deprecated
  public static ReadBuilder read(InputFile file) {
    Preconditions.checkState(
        !(file instanceof NativeEncryptionInputFile), "Native ORC encryption is not supported");

    return new ReadBuilder(file);
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use the {@link
   *     ObjectModelRegistry#readBuilder(FileFormat, String, InputFile)} instead.
   */
  @Deprecated
  public static class ReadBuilder {
    private final ReadBuilderImpl<Object> impl;

    private ReadBuilder(InputFile file) {
      this.impl = new ReadBuilderImpl<>(file);
    }

    /**
     * Restricts the read to the given range: [start, start + length).
     *
     * @param newStart the start position for this read
     * @param newLength the length of the range this read should scan
     * @return this builder for method chaining
     */
    public ReadBuilder split(long newStart, long newLength) {
      impl.split(newStart, newLength);
      return this;
    }

    public ReadBuilder project(Schema newSchema) {
      impl.project(newSchema);
      return this;
    }

    public ReadBuilder caseSensitive(boolean newCaseSensitive) {
      impl.caseSensitive(newCaseSensitive);
      return this;
    }

    public ReadBuilder config(String property, String value) {
      impl.set(property, value);
      return this;
    }

    public ReadBuilder createReaderFunc(
        Function<TypeDescription, OrcRowReader<?>> newReaderFunction) {
      Preconditions.checkState(
          impl.batchedReaderFunc == null
              && impl.readerFunction == null
              && impl.batchReaderFunction == null,
          "Cannot set multiple read builder functions");
      if (newReaderFunction != null) {
        impl.readerFunc = t -> (OrcRowReader<Object>) newReaderFunction.apply(t);
      } else {
        impl.readerFunc = null;
      }

      return this;
    }

    public ReadBuilder filter(Expression newFilter) {
      impl.filter(newFilter);
      return this;
    }

    public ReadBuilder createBatchedReaderFunc(
        Function<TypeDescription, OrcBatchReader<?>> newBatchReaderFunction) {
      Preconditions.checkState(
          impl.readerFunc == null
              && impl.readerFunction == null
              && impl.batchReaderFunction == null,
          "Cannot set multiple read builder functions");
      if (newBatchReaderFunction != null) {
        impl.batchedReaderFunc = t -> (OrcBatchReader<Object>) newBatchReaderFunction.apply(t);
      } else {
        impl.batchedReaderFunc = null;
      }

      return this;
    }

    public ReadBuilder recordsPerBatch(int numRecordsPerBatch) {
      impl.set(
          org.apache.iceberg.io.ReadBuilder.RECORDS_PER_BATCH_KEY,
          String.valueOf(numRecordsPerBatch));
      return this;
    }

    public ReadBuilder withNameMapping(NameMapping newNameMapping) {
      impl.nameMapping(newNameMapping);
      return this;
    }

    public <D> CloseableIterable<D> build() {
      return (CloseableIterable<D>) impl.build();
    }
  }

  public static class ReadBuilderImpl<D>
      implements org.apache.iceberg.io.ReadBuilder<ReadBuilderImpl<D>, D> {
    private final InputFile file;
    private final Configuration conf;
    private Schema schema = null;
    private Long start = null;
    private Long length = null;
    private Expression filter = null;
    private boolean filterCaseSensitive = true;
    private NameMapping nameMapping = null;
    private boolean reuseContainers = false;

    private Function<TypeDescription, OrcRowReader<D>> readerFunc;
    private Function<TypeDescription, OrcBatchReader<D>> batchedReaderFunc;
    private ORCObjectModelFactory.ReaderFunction<D> readerFunction;
    private ORCObjectModelFactory.BatchReaderFunction<D> batchReaderFunction;
    private Map<Integer, ?> constantFieldAccessors = ImmutableMap.of();

    protected ReadBuilderImpl(InputFile file) {
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

    ReadBuilderImpl<D> readerFunction(ORCObjectModelFactory.ReaderFunction<D> newReaderFunction) {
      Preconditions.checkState(
          readerFunc == null && batchedReaderFunc == null && batchReaderFunction == null,
          "Cannot set multiple read builder functions");
      this.readerFunction = newReaderFunction;
      return this;
    }

    ReadBuilderImpl<D> batchReaderFunction(
        ORCObjectModelFactory.BatchReaderFunction<D> newReaderFunction) {
      Preconditions.checkState(
          readerFunc == null && batchedReaderFunc == null && readerFunction == null,
          "Cannot set multiple read builder functions");
      this.batchReaderFunction = newReaderFunction;
      return this;
    }

    /**
     * Restricts the read to the given range: [start, start + length).
     *
     * @param newStart the start position for this read
     * @param newLength the length of the range this read should scan
     * @return this builder for method chaining
     */
    @Override
    public ReadBuilderImpl<D> split(long newStart, long newLength) {
      this.start = newStart;
      this.length = newLength;
      return this;
    }

    @Override
    public ReadBuilderImpl<D> project(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    @Override
    public ReadBuilderImpl<D> set(String property, String value) {
      conf.set(property, value);
      return this;
    }

    @Override
    public ReadBuilderImpl<D> reuseContainers() {
      this.reuseContainers = true;
      return this;
    }

    @Override
    public ReadBuilderImpl<D> caseSensitive(boolean newFilterCaseSensitive) {
      OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(this.conf, newFilterCaseSensitive);
      this.filterCaseSensitive = newFilterCaseSensitive;
      return this;
    }

    @Override
    public ReadBuilderImpl<D> filter(Expression newFilter) {
      this.filter = newFilter;
      return this;
    }

    @Override
    public ReadBuilderImpl<D> constantFieldAccessors(Map<Integer, ?> newConstantFieldAccessors) {
      this.constantFieldAccessors = newConstantFieldAccessors;
      return this;
    }

    @Override
    public ReadBuilderImpl<D> nameMapping(NameMapping newNameMapping) {
      this.nameMapping = newNameMapping;
      return this;
    }

    @Override
    public CloseableIterable<D> build() {
      Preconditions.checkNotNull(schema, "Schema is required");
      Preconditions.checkNotNull(reuseContainers, "Reuse containers is required for ORC read");
      Function<TypeDescription, OrcRowReader<D>> reader =
          readerFunc != null
              ? readerFunc
              : readerFunction != null
                  ? fileType -> readerFunction.read(schema, fileType, constantFieldAccessors)
                  : null;
      Function<TypeDescription, OrcBatchReader<D>> batchReader =
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
}
