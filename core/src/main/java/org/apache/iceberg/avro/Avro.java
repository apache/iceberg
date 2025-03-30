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
package org.apache.iceberg.avro;

import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION_DEFAULT;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION_LEVEL_DEFAULT;
import static org.apache.iceberg.TableProperties.DELETE_AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.DELETE_AVRO_COMPRESSION_LEVEL;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificData;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.AppenderBuilder;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ArrayUtil;

public class Avro {
  private Avro() {}

  private enum Codec {
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    ZSTD
  }

  private static final int ZSTD_COMPRESSION_LEVEL_DEFAULT = 1;
  private static final int GZIP_COMPRESSION_LEVEL_DEFAULT = 9;

  private static final GenericData DEFAULT_MODEL = new SpecificData();

  static {
    LogicalTypes.register(LogicalMap.NAME, schema -> LogicalMap.get());
    LogicalTypes.register(VariantLogicalType.NAME, schema -> VariantLogicalType.get());
    DEFAULT_MODEL.addLogicalTypeConversion(new Conversions.DecimalConversion());
    DEFAULT_MODEL.addLogicalTypeConversion(new UUIDConversion());
    DEFAULT_MODEL.addLogicalTypeConversion(new VariantConversion());
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use the ObjectModelRegistry instead.
   */
  @Deprecated
  public static WriteBuilder write(OutputFile file) {
    if (file instanceof EncryptedOutputFile) {
      return write((EncryptedOutputFile) file);
    }

    return new WriteBuilder(file);
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use the ObjectModelRegistry instead.
   */
  @Deprecated
  public static WriteBuilder write(EncryptedOutputFile file) {
    return new WriteBuilder(file.encryptingOutputFile());
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use the ObjectModelRegistry instead.
   */
  @Deprecated
  public static class WriteBuilder extends AppenderBuilderInternal<WriteBuilder, Object> {
    private WriteBuilder(OutputFile file) {
      super(file);
    }
  }

  public static class ObjectModel<E> implements org.apache.iceberg.io.ObjectModel<E> {
    private final String name;
    private final BiFunction<org.apache.iceberg.Schema, Map<Integer, ?>, DatumReader<?>>
        readerFunction;
    private final BiFunction<Schema, E, DatumWriter<?>> writerFunction;
    private final BiFunction<Schema, E, DatumWriter<?>> deleteRowWriterFunction;

    public ObjectModel(
        String name,
        BiFunction<org.apache.iceberg.Schema, Map<Integer, ?>, DatumReader<?>> readerFunction,
        BiFunction<Schema, E, DatumWriter<?>> writerFunction,
        BiFunction<Schema, E, DatumWriter<?>> deleteRowWriterFunction) {
      this.name = name;
      this.readerFunction = readerFunction;
      this.writerFunction = writerFunction;
      this.deleteRowWriterFunction = deleteRowWriterFunction;
    }

    public ObjectModel(
        String name,
        BiFunction<org.apache.iceberg.Schema, Map<Integer, ?>, DatumReader<?>> readerFunction,
        BiFunction<Schema, E, DatumWriter<?>> writerFunction) {
      this(name, readerFunction, writerFunction, null);
    }

    @Override
    public FileFormat format() {
      return FileFormat.AVRO;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public <B extends AppenderBuilder<B, E>> B appenderBuilder(OutputFile outputFile) {
      AppenderBuilderInternal<?, E> internal = new AppenderBuilderInternal<>(outputFile);
      return (B)
          internal.writerFunction(writerFunction).deleteRowWriterFunction(deleteRowWriterFunction);
    }

    @Override
    public <B extends org.apache.iceberg.io.ReadBuilder<B>> B readBuilder(InputFile inputFile) {
      return (B) new ReadBuilder(inputFile).readerFunction(readerFunction);
    }
  }

  @SuppressWarnings("unchecked")
  private static class AppenderBuilderInternal<B extends AppenderBuilderInternal<B, E>, E>
      implements InternalData.WriteBuilder, AppenderBuilder<B, E> {
    private final OutputFile file;
    private final Map<String, String> config = Maps.newHashMap();
    private final Map<String, String> metadata = Maps.newLinkedHashMap();
    private org.apache.iceberg.Schema schema = null;
    private String name = "table";
    private Function<Schema, DatumWriter<?>> createWriterFunc = null;
    private BiFunction<Schema, E, DatumWriter<?>> writerFunction = null;
    private BiFunction<Schema, E, DatumWriter<?>> deleteRowWriterFunction = null;
    private boolean overwrite;
    private MetricsConfig metricsConfig;
    private Function<Map<String, String>, Context> createContextFunc = Context::dataContext;
    private E engineSchema;

    private AppenderBuilderInternal(OutputFile file) {
      this.file = file;
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

    @Override
    public B schema(org.apache.iceberg.Schema newSchema) {
      this.schema = newSchema;
      return (B) this;
    }

    @Override
    public B named(String newName) {
      this.name = newName;
      return (B) this;
    }

    public B createWriterFunc(Function<Schema, DatumWriter<?>> newWriterFunction) {
      Preconditions.checkState(
          writerFunction == null && deleteRowWriterFunction == null,
          "Cannot set multiple writer builder functions");
      this.createWriterFunc = newWriterFunction;
      return (B) this;
    }

    public B writerFunction(BiFunction<Schema, E, DatumWriter<?>> newWriterFunction) {
      Preconditions.checkState(
          createWriterFunc == null, "Cannot set multiple writer builder functions");
      this.writerFunction = newWriterFunction;
      return (B) this;
    }

    public B deleteRowWriterFunction(BiFunction<Schema, E, DatumWriter<?>> newWriterFunction) {
      Preconditions.checkState(
          createWriterFunc == null, "Cannot set multiple writer builder functions");
      this.deleteRowWriterFunction = newWriterFunction;
      return (B) this;
    }

    @Override
    public B set(String property, String value) {
      config.put(property, value);
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
      metadata.put(property, value);
      return (B) this;
    }

    @Override
    public B meta(Map<String, String> properties) {
      metadata.putAll(properties);
      return (B) this;
    }

    @Override
    public B metricsConfig(MetricsConfig newMetricsConfig) {
      this.metricsConfig = newMetricsConfig;
      return (B) this;
    }

    @Override
    public B overwrite() {
      return overwrite(true);
    }

    @Override
    public B overwrite(boolean enabled) {
      this.overwrite = enabled;
      return (B) this;
    }

    // supposed to always be a private method used strictly by data and delete write builders
    // package-private because of inheritance until deprecation of the WriteBuilder
    B createContextFunc(Function<Map<String, String>, Context> newCreateContextFunc) {
      this.createContextFunc = newCreateContextFunc;
      return (B) this;
    }

    @Override
    public B engineSchema(E newEngineSchema) {
      this.engineSchema = newEngineSchema;
      return (B) this;
    }

    @Override
    public <D> FileAppender<D> build(WriteMode mode) throws IOException {
      switch (mode) {
        case APPENDER:
        case DATA_WRITER:
          Preconditions.checkState(writerFunction != null, "Writer function has to be set.");
          this.createWriterFunc = avroSchema -> writerFunction.apply(avroSchema, engineSchema);
          this.createContextFunc = Context::dataContext;
          break;
        case EQUALITY_DELETE_WRITER:
          Preconditions.checkState(writerFunction != null, "Writer function has to be set.");
          this.createWriterFunc = avroSchema -> writerFunction.apply(avroSchema, engineSchema);
          this.createContextFunc = Context::deleteContext;
          break;
        case POSITION_DELETE_WRITER:
          this.createWriterFunc = ignored -> new PositionDatumWriter();
          this.createContextFunc = Context::deleteContext;
          break;
        case POSITION_DELETE_WITH_ROW_WRITER:
          Preconditions.checkState(
              deleteRowWriterFunction != null || writerFunction != null,
              "Writer function has to be set.");
          this.createWriterFunc =
              deleteRowWriterFunction != null
                  ? avroSchema ->
                      new PositionAndRowDatumWriter<>(
                          deleteRowWriterFunction.apply(avroSchema, engineSchema))
                  : avroSchema ->
                      new PositionAndRowDatumWriter<>(
                          writerFunction.apply(avroSchema, engineSchema));
          break;
        default:
          throw new IllegalArgumentException("Not supported mode: " + mode);
      }

      return build();
    }

    @Override
    public <D> FileAppender<D> build() throws IOException {
      Preconditions.checkNotNull(schema, "Schema is required");
      Preconditions.checkNotNull(name, "Table name is required and cannot be null");

      Function<Schema, DatumWriter<?>> writerFunc;
      if (createWriterFunc != null) {
        writerFunc = createWriterFunc;
      } else {
        writerFunc = GenericAvroWriter::new;
      }

      // add the Iceberg schema to keyValueMetadata
      meta("iceberg.schema", SchemaParser.toJson(schema));

      Context context = createContextFunc.apply(config);
      CodecFactory codec = context.codec();

      return new AvroFileAppender<>(
          schema,
          AvroSchemaUtil.convert(schema, name),
          file,
          writerFunc,
          codec,
          metadata,
          metricsConfig,
          overwrite);
    }

    // supposed to always be a private method used strictly by data and delete write builders
    // package-protected because of inheritance until deprecation of the WriteBuilder
    static class Context {
      private final CodecFactory codec;

      private Context(CodecFactory codec) {
        this.codec = codec;
      }

      static Context dataContext(Map<String, String> config) {
        String codecAsString = config.getOrDefault(AVRO_COMPRESSION, AVRO_COMPRESSION_DEFAULT);
        String compressionLevel =
            config.getOrDefault(AVRO_COMPRESSION_LEVEL, AVRO_COMPRESSION_LEVEL_DEFAULT);
        CodecFactory codec = toCodec(codecAsString, compressionLevel);

        return new Context(codec);
      }

      static Context deleteContext(Map<String, String> config) {
        // default delete config using data config
        Context dataContext = dataContext(config);

        String codecAsString = config.get(DELETE_AVRO_COMPRESSION);
        String compressionLevel =
            config.getOrDefault(DELETE_AVRO_COMPRESSION_LEVEL, AVRO_COMPRESSION_LEVEL_DEFAULT);
        CodecFactory codec =
            codecAsString != null ? toCodec(codecAsString, compressionLevel) : dataContext.codec();

        return new Context(codec);
      }

      private static CodecFactory toCodec(String codecAsString, String compressionLevel) {
        CodecFactory codecFactory;
        try {
          switch (Codec.valueOf(codecAsString.toUpperCase(Locale.ENGLISH))) {
            case UNCOMPRESSED:
              codecFactory = CodecFactory.nullCodec();
              break;
            case SNAPPY:
              codecFactory = CodecFactory.snappyCodec();
              break;
            case ZSTD:
              codecFactory =
                  CodecFactory.zstandardCodec(
                      compressionLevelAsInt(compressionLevel, ZSTD_COMPRESSION_LEVEL_DEFAULT));
              break;
            case GZIP:
              codecFactory =
                  CodecFactory.deflateCodec(
                      compressionLevelAsInt(compressionLevel, GZIP_COMPRESSION_LEVEL_DEFAULT));
              break;
            default:
              throw new IllegalArgumentException("Unsupported compression codec: " + codecAsString);
          }
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("Unsupported compression codec: " + codecAsString);
        }
        return codecFactory;
      }

      private static int compressionLevelAsInt(
          String tableCompressionLevel, int defaultCompressionLevel) {
        return tableCompressionLevel != null
            ? Integer.parseInt(tableCompressionLevel)
            : defaultCompressionLevel;
      }

      CodecFactory codec() {
        return codec;
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

    public DataWriteBuilder schema(org.apache.iceberg.Schema newSchema) {
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
      appenderBuilder.meta(property, value);
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

    public DataWriteBuilder createWriterFunc(Function<Schema, DatumWriter<?>> newCreateWriterFunc) {
      appenderBuilder.createWriterFunc(newCreateWriterFunc);
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

    public <T> DataWriter<T> build() throws IOException {
      Preconditions.checkArgument(spec != null, "Cannot create data writer without spec");
      Preconditions.checkArgument(
          spec.isUnpartitioned() || partition != null,
          "Partition must not be null when creating data writer for partitioned spec");

      FileAppender<T> fileAppender = appenderBuilder.build();
      return new DataWriter<>(
          fileAppender, FileFormat.AVRO, location, spec, partition, keyMetadata, sortOrder);
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
    private Function<Schema, DatumWriter<?>> createWriterFunc = null;
    private org.apache.iceberg.Schema rowSchema;
    private PartitionSpec spec;
    private StructLike partition;
    private EncryptionKeyMetadata keyMetadata = null;
    private int[] equalityFieldIds = null;
    private SortOrder sortOrder;

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
      appenderBuilder.meta(property, value);
      return this;
    }

    public DeleteWriteBuilder meta(Map<String, String> properties) {
      appenderBuilder.meta(properties);
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

    public DeleteWriteBuilder createWriterFunc(Function<Schema, DatumWriter<?>> writerFunction) {
      this.createWriterFunc = writerFunction;
      return this;
    }

    public DeleteWriteBuilder rowSchema(org.apache.iceberg.Schema newRowSchema) {
      this.rowSchema = newRowSchema;
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

    public DeleteWriteBuilder withSortOrder(SortOrder newSortOrder) {
      this.sortOrder = newSortOrder;
      return this;
    }

    public <T> EqualityDeleteWriter<T> buildEqualityWriter() throws IOException {
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
          FileFormat.AVRO,
          location,
          spec,
          partition,
          keyMetadata,
          sortOrder,
          equalityFieldIds);
    }

    public <T> PositionDeleteWriter<T> buildPositionWriter() throws IOException {
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
        // the appender uses the row schema wrapped with position fields
        appenderBuilder.schema(DeleteSchemaUtil.posDeleteSchema(rowSchema));

        appenderBuilder.createWriterFunc(
            avroSchema -> new PositionAndRowDatumWriter<>(createWriterFunc.apply(avroSchema)));

      } else {
        appenderBuilder.schema(DeleteSchemaUtil.pathPosSchema());

        // We ignore the 'createWriterFunc' and 'rowSchema' even if is provided, since we do not
        // write row data itself
        appenderBuilder.createWriterFunc(ignored -> new PositionDatumWriter());
      }

      appenderBuilder.createContextFunc(AppenderBuilderInternal.Context::deleteContext);

      return new PositionDeleteWriter<>(
          appenderBuilder.build(), FileFormat.AVRO, location, spec, partition, keyMetadata);
    }
  }

  /** A {@link DatumWriter} implementation that wraps another to produce position deletes. */
  private static class PositionDatumWriter implements MetricsAwareDatumWriter<PositionDelete<?>> {
    private static final ValueWriter<Object> PATH_WRITER = ValueWriters.strings();
    private static final ValueWriter<Long> POS_WRITER = ValueWriters.longs();

    @Override
    public void setSchema(Schema schema) {}

    @Override
    public void write(PositionDelete<?> delete, Encoder out) throws IOException {
      PATH_WRITER.write(delete.path(), out);
      POS_WRITER.write(delete.pos(), out);
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      return Stream.concat(PATH_WRITER.metrics(), POS_WRITER.metrics());
    }
  }

  /**
   * A {@link DatumWriter} implementation that wraps another to produce position deletes with row
   * data.
   *
   * @param <D> the type of datum written as a deleted row
   */
  private static class PositionAndRowDatumWriter<D>
      implements MetricsAwareDatumWriter<PositionDelete<D>> {
    private static final ValueWriter<Object> PATH_WRITER = ValueWriters.strings();
    private static final ValueWriter<Long> POS_WRITER = ValueWriters.longs();

    private final DatumWriter<D> rowWriter;

    private PositionAndRowDatumWriter(DatumWriter<D> rowWriter) {
      this.rowWriter = rowWriter;
    }

    @Override
    public void setSchema(Schema schema) {
      Schema.Field rowField = schema.getField("row");
      if (rowField != null) {
        rowWriter.setSchema(rowField.schema());
      }
    }

    @Override
    public void write(PositionDelete<D> delete, Encoder out) throws IOException {
      PATH_WRITER.write(delete.path(), out);
      POS_WRITER.write(delete.pos(), out);
      rowWriter.write(delete.row(), out);
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      return Stream.concat(PATH_WRITER.metrics(), POS_WRITER.metrics());
    }
  }

  public static ReadBuilder read(InputFile file) {
    return new ReadBuilder(file);
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Use the ObjectModelRegistry instead.
   */
  @Deprecated
  public static class ReadBuilder
      implements InternalData.ReadBuilder, org.apache.iceberg.io.ReadBuilder<ReadBuilder> {
    private final InputFile file;
    private final Map<String, String> renames = Maps.newLinkedHashMap();
    private final Map<Integer, Class<? extends StructLike>> typeMap = Maps.newHashMap();
    private Class<? extends StructLike> rootType = null;
    private ClassLoader loader = Thread.currentThread().getContextClassLoader();
    private NameMapping nameMapping;
    private boolean reuseContainers = false;
    private org.apache.iceberg.Schema schema = null;
    private Function<Schema, DatumReader<?>> createReaderFunc = null;
    private BiFunction<org.apache.iceberg.Schema, Schema, DatumReader<?>> createReaderBiFunc = null;
    private Function<org.apache.iceberg.Schema, DatumReader<?>> createResolvingReaderFunc = null;
    private BiFunction<org.apache.iceberg.Schema, Map<Integer, ?>, DatumReader<?>> readerFunction;
    private Map<Integer, ?> constantFieldAccessors = ImmutableMap.of();

    @SuppressWarnings("UnnecessaryLambda")
    private final Function<org.apache.iceberg.Schema, DatumReader<?>> defaultCreateReaderFunc =
        readSchema -> {
          GenericAvroReader<?> reader = GenericAvroReader.create(readSchema);
          reader.setClassLoader(loader);
          return reader;
        };

    private Long start = null;
    private Long length = null;

    private ReadBuilder(InputFile file) {
      Preconditions.checkNotNull(file, "Input file cannot be null");
      this.file = file;
    }

    public ReadBuilder createResolvingReader(
        Function<org.apache.iceberg.Schema, DatumReader<?>> newReaderFunction) {
      Preconditions.checkState(
          createReaderBiFunc == null && createReaderFunc == null && readerFunction == null,
          "Cannot set multiple read builder functions");
      this.createResolvingReaderFunc = newReaderFunction;
      return this;
    }

    /**
     * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link #readerFunction(BiFunction)}
     *     instead.
     */
    @Deprecated
    public ReadBuilder createReaderFunc(Function<Schema, DatumReader<?>> newReaderFunction) {
      Preconditions.checkState(
          createReaderBiFunc == null && createResolvingReaderFunc == null && readerFunction == null,
          "Cannot set multiple read builder functions");
      this.createReaderFunc = newReaderFunction;
      return this;
    }

    /**
     * @deprecated Since 1.10.0, will be removed in 1.11.0. Use {@link #readerFunction(BiFunction)}
     *     instead.
     */
    @Deprecated
    public ReadBuilder createReaderFunc(
        BiFunction<org.apache.iceberg.Schema, Schema, DatumReader<?>> newReaderFunction) {
      Preconditions.checkState(
          createReaderFunc == null && createResolvingReaderFunc == null && readerFunction == null,
          "Cannot set multiple read builder functions");
      this.createReaderBiFunc = newReaderFunction;
      return this;
    }

    public ReadBuilder readerFunction(
        BiFunction<org.apache.iceberg.Schema, Map<Integer, ?>, DatumReader<?>> newReaderFunction) {
      Preconditions.checkState(
          createReaderBiFunc == null
              && createReaderFunc == null
              && createResolvingReaderFunc == null,
          "Cannot set multiple read builder functions");
      this.readerFunction = newReaderFunction;
      return this;
    }

    /**
     * Restricts the read to the given range: [start, end = start + length).
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
    public ReadBuilder project(org.apache.iceberg.Schema projectedSchema) {
      this.schema = projectedSchema;
      return this;
    }

    @Override
    public ReadBuilder set(String key, String value) {
      // Configuration is not used for Avro reader creation
      return this;
    }

    @Override
    public ReadBuilder reuseContainers() {
      this.reuseContainers = true;
      return this;
    }

    @Override
    public ReadBuilder reuseContainers(boolean shouldReuse) {
      this.reuseContainers = shouldReuse;
      return this;
    }

    @Override
    public ReadBuilder constantFieldAccessors(Map<Integer, ?> newConstantFieldAccessors) {
      this.constantFieldAccessors = newConstantFieldAccessors;
      return this;
    }

    @Deprecated
    public ReadBuilder rename(String fullName, String newName) {
      renames.put(fullName, newName);
      return this;
    }

    @Override
    public InternalData.ReadBuilder setRootType(Class<? extends StructLike> rootClass) {
      this.rootType = rootClass;
      return this;
    }

    @Override
    public InternalData.ReadBuilder setCustomType(
        int fieldId, Class<? extends StructLike> structClass) {
      typeMap.put(fieldId, structClass);
      return this;
    }

    @Override
    public ReadBuilder withNameMapping(NameMapping newNameMapping) {
      this.nameMapping = newNameMapping;
      return this;
    }

    @Deprecated
    public ReadBuilder classLoader(ClassLoader classLoader) {
      this.loader = classLoader;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <D> AvroIterable<D> build() {
      Preconditions.checkNotNull(schema, "Schema is required");

      if (null == nameMapping) {
        this.nameMapping = MappingUtil.create(schema);
      }

      DatumReader<D> reader;
      if (createReaderBiFunc != null) {
        reader =
            new ProjectionDatumReader<>(
                avroSchema -> createReaderBiFunc.apply(schema, avroSchema), schema, renames, null);
      } else if (createReaderFunc != null) {
        reader = new ProjectionDatumReader<>(createReaderFunc, schema, renames, null);
      } else if (createResolvingReaderFunc != null) {
        reader = (DatumReader<D>) createResolvingReaderFunc.apply(schema);
      } else if (readerFunction != null) {
        reader = (DatumReader<D>) readerFunction.apply(schema, constantFieldAccessors);
      } else {
        reader = (DatumReader<D>) defaultCreateReaderFunc.apply(schema);
      }

      if (reader instanceof SupportsCustomRecords) {
        ((SupportsCustomRecords) reader).setClassLoader(loader);
        ((SupportsCustomRecords) reader).setRenames(renames);
      }

      if (reader instanceof SupportsCustomTypes) {
        ((SupportsCustomTypes) reader).setCustomTypes(rootType, typeMap);
      }

      return new AvroIterable<>(
          file, new NameMappingDatumReader<>(nameMapping, reader), start, length, reuseContainers);
    }
  }

  /**
   * Returns number of rows in specified Avro file
   *
   * @param file Avro file
   * @return number of rows in file
   */
  public static long rowCount(InputFile file) {
    return AvroIO.findStartingRowPos(file::newStream, Long.MAX_VALUE);
  }
}
