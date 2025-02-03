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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFileReaderService;
import org.apache.iceberg.DataFileWriterService;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.orc.GenericOrcWriters;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.NativeEncryptionInputFile;
import org.apache.iceberg.encryption.NativeEncryptionOutputFile;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileFormatAppenderBuilder;
import org.apache.iceberg.io.FileFormatAppenderBuilderBase;
import org.apache.iceberg.io.FileFormatDataWriterBuilder;
import org.apache.iceberg.io.FileFormatDataWriterBuilderBase;
import org.apache.iceberg.io.FileFormatEqualityDeleteWriterBuilder;
import org.apache.iceberg.io.FileFormatEqualityDeleteWriterBuilderBase;
import org.apache.iceberg.io.FileFormatPositionDeleteWriterBuilder;
import org.apache.iceberg.io.FileFormatReadBuilder;
import org.apache.iceberg.io.FileFormatReadBuilderBase;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PartitionUtil;
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

  public static WriteBuilder write(OutputFile file) {
    return new WriteBuilder(file);
  }

  public static WriteBuilder write(EncryptedOutputFile file) {
    Preconditions.checkState(
        !(file instanceof NativeEncryptionOutputFile), "Native ORC encryption is not supported");
    return new WriteBuilder(file.encryptingOutputFile());
  }

  public static Schema schemaWithoutConstantAndMetadataFields(
      Schema target, Map<Integer, ?> idToConstant) {
    return TypeUtil.selectNot(
        target, Sets.union(idToConstant.keySet(), MetadataColumns.metadataFieldIds()));
  }

  public static class WriteBuilder extends FileFormatAppenderBuilderBase<WriteBuilder> {
    private final Configuration conf;
    private BiFunction<Schema, TypeDescription, OrcRowWriter<?>> createWriterFunc;
    private Function<Map<String, String>, Context> createContextFunc = Context::dataContext;

    private WriteBuilder(OutputFile file) {
      super(file);
      if (file instanceof HadoopOutputFile) {
        this.conf = new Configuration(((HadoopOutputFile) file).getConf());
      } else {
        this.conf = new Configuration();
      }
    }

    public WriteBuilder createWriterFunc(
        BiFunction<Schema, TypeDescription, OrcRowWriter<?>> writerFunction) {
      this.createWriterFunc = writerFunction;
      return this;
    }

    // supposed to always be a private method used strictly by data and delete write builders
    private WriteBuilder createContextFunc(
        Function<Map<String, String>, Context> newCreateContextFunc) {
      this.createContextFunc = newCreateContextFunc;
      return this;
    }

    @Override
    public <D> FileAppender<D> build() {
      Preconditions.checkNotNull(schema(), "Schema is required");

      for (Map.Entry<String, String> entry : config().entrySet()) {
        this.conf.set(entry.getKey(), entry.getValue());
      }

      // for compatibility
      if (conf.get(VECTOR_ROW_BATCH_SIZE) != null && config().get(ORC_WRITE_BATCH_SIZE) == null) {
        config().put(ORC_WRITE_BATCH_SIZE, conf.get(VECTOR_ROW_BATCH_SIZE));
      }

      // Map Iceberg properties to pass down to the ORC writer
      Context context = createContextFunc.apply(config());

      OrcConf.STRIPE_SIZE.setLong(conf, context.stripeSize());
      OrcConf.BLOCK_SIZE.setLong(conf, context.blockSize());
      OrcConf.COMPRESS.setString(conf, context.compressionKind().name());
      OrcConf.COMPRESSION_STRATEGY.setString(conf, context.compressionStrategy().name());
      OrcConf.OVERWRITE_OUTPUT_FILE.setBoolean(conf, isOverwrite());
      OrcConf.BLOOM_FILTER_COLUMNS.setString(conf, context.bloomFilterColumns());
      OrcConf.BLOOM_FILTER_FPP.setDouble(conf, context.bloomFilterFpp());

      return new OrcFileAppender<>(
          schema(),
          file(),
          createWriterFunc,
          conf,
          metadata().entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey, e -> e.getValue().getBytes(StandardCharsets.UTF_8))),
          context.vectorizedRowBatchSize(),
          metricsConfig());
    }

    private static class Context {
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

  public static DataWriteBuilder writeData(OutputFile file) {
    return new DataWriteBuilder(file);
  }

  public static DataWriteBuilder writeData(EncryptedOutputFile file) {
    Preconditions.checkState(
        !(file instanceof NativeEncryptionOutputFile), "Native ORC encryption is not supported");
    return new DataWriteBuilder(file.encryptingOutputFile());
  }

  public static class DataWriteBuilder extends FileFormatDataWriterBuilderBase<DataWriteBuilder> {

    private DataWriteBuilder(OutputFile file) {
      super(write(file), FileFormat.ORC);
    }

    public DataWriteBuilder createWriterFunc(
        BiFunction<Schema, TypeDescription, OrcRowWriter<?>> writerFunction) {
      ((WriteBuilder) appenderBuilder()).createWriterFunc(writerFunction);
      return this;
    }
  }

  public static DeleteWriteBuilder writeDeletes(OutputFile file) {
    return new DeleteWriteBuilder(file);
  }

  public static DeleteWriteBuilder writeDeletes(EncryptedOutputFile file) {
    Preconditions.checkState(
        !(file instanceof NativeEncryptionOutputFile), "Native ORC encryption is not supported");
    return new DeleteWriteBuilder(file.encryptingOutputFile());
  }

  public static class DeleteWriteBuilder
      extends FileFormatEqualityDeleteWriterBuilderBase<DeleteWriteBuilder>
      implements FileFormatPositionDeleteWriterBuilder<DeleteWriteBuilder> {
    private BiFunction<Schema, TypeDescription, OrcRowWriter<?>> createWriterFunc = null;
    private Function<CharSequence, ?> pathTransformFunc = Function.identity();

    private DeleteWriteBuilder(OutputFile file) {
      super(write(file), FileFormat.ORC);
    }

    public DeleteWriteBuilder createWriterFunc(
        BiFunction<Schema, TypeDescription, OrcRowWriter<?>> newWriterFunc) {
      this.createWriterFunc = newWriterFunc;
      return this;
    }

    @Deprecated
    public DeleteWriteBuilder rowSchema(Schema newSchema) {
      return schema(newSchema);
    }

    public DeleteWriteBuilder transformPaths(Function<CharSequence, ?> newPathTransformFunc) {
      this.pathTransformFunc = newPathTransformFunc;
      return this;
    }

    @Override
    public <T> EqualityDeleteWriter<T> buildEqualityWriter() throws IOException {
      Preconditions.checkState(
          appenderBuilder().schema() != null,
          "Cannot create equality delete file without a schema");
      Preconditions.checkState(
          equalityFieldIds() != null,
          "Cannot create equality delete file without delete field ids");
      Preconditions.checkState(
          createWriterFunc != null,
          "Cannot create equality delete file unless createWriterFunc is set");
      Preconditions.checkArgument(
          spec() != null, "Spec must not be null when creating equality delete writer");
      Preconditions.checkArgument(
          spec().isUnpartitioned() || partition() != null,
          "Partition must not be null for partitioned writes");

      meta("delete-type", "equality");
      meta(
          "delete-field-ids",
          IntStream.of(equalityFieldIds())
              .mapToObj(Objects::toString)
              .collect(Collectors.joining(", ")));

      // the appender uses the row schema without extra columns
      ((WriteBuilder) appenderBuilder()).createWriterFunc(createWriterFunc);
      ((WriteBuilder) appenderBuilder()).createContextFunc(WriteBuilder.Context::deleteContext);

      return new EqualityDeleteWriter<>(
          appenderBuilder().build(),
          FileFormat.ORC,
          appenderBuilder().location(),
          spec(),
          partition(),
          keyMetadata(),
          sortOrder(),
          equalityFieldIds());
    }

    @Override
    public <T> PositionDeleteWriter<T> buildPositionWriter() throws IOException {
      Preconditions.checkState(
          equalityFieldIds() == null, "Cannot create position delete file using delete field ids");
      Preconditions.checkArgument(
          spec() != null, "Spec must not be null when creating position delete writer");
      Preconditions.checkArgument(
          spec().isUnpartitioned() || partition() != null,
          "Partition must not be null for partitioned writes");
      Preconditions.checkArgument(
          appenderBuilder().schema() == null || createWriterFunc != null,
          "Create function should be provided if we write row data");

      meta("delete-type", "position");

      if (appenderBuilder().schema() != null && createWriterFunc != null) {
        Schema deleteSchema = DeleteSchemaUtil.posDeleteSchema(appenderBuilder().schema());
        appenderBuilder().schema(deleteSchema);

        ((WriteBuilder) appenderBuilder())
            .createWriterFunc(
                (schema, typeDescription) ->
                    GenericOrcWriters.positionDelete(
                        createWriterFunc.apply(deleteSchema, typeDescription), pathTransformFunc));
      } else {
        ((WriteBuilder) appenderBuilder()).schema(DeleteSchemaUtil.pathPosSchema());

        // We ignore the 'createWriterFunc' and 'rowSchema' even if is provided, since we do not
        // write row data itself
        ((WriteBuilder) appenderBuilder())
            .createWriterFunc(
                (schema, typeDescription) ->
                    GenericOrcWriters.positionDelete(
                        GenericOrcWriter.buildWriter(schema, typeDescription),
                        Function.identity()));
      }

      ((WriteBuilder) appenderBuilder()).createContextFunc(WriteBuilder.Context::deleteContext);

      return new PositionDeleteWriter<>(
          appenderBuilder().build(),
          FileFormat.ORC,
          appenderBuilder().location(),
          spec(),
          partition(),
          keyMetadata());
    }
  }

  public static ReadBuilder read(InputFile file) {
    Preconditions.checkState(
        !(file instanceof NativeEncryptionInputFile), "Native ORC encryption is not supported");

    return new ReadBuilder(file);
  }

  public static class ReadBuilder extends FileFormatReadBuilderBase<ReadBuilder> {
    private final Configuration conf;
    private Function<TypeDescription, OrcRowReader<?>> readerFunc;
    private Function<TypeDescription, OrcBatchReader<?>> batchedReaderFunc;

    ReadBuilder(InputFile file) {
      super(file);
      if (file instanceof HadoopInputFile) {
        this.conf = new Configuration(((HadoopInputFile) file).getConf());
      } else {
        this.conf = new Configuration();
      }

      // We need to turn positional schema evolution off since we use column name based schema
      // evolution for projection
      this.conf.setBoolean(OrcConf.FORCE_POSITIONAL_EVOLUTION.getHiveConfName(), false);
      this.recordsPerBatch(VectorizedRowBatch.DEFAULT_SIZE);
    }

    @Deprecated
    public ReadBuilder config(String property, String value) {
      return set(property, value);
    }

    @Override
    public ReadBuilder set(String key, String value) {
      super.set(key, value);
      conf.set(key, value);
      return this;
    }

    public ReadBuilder createReaderFunc(Function<TypeDescription, OrcRowReader<?>> readerFunction) {
      Preconditions.checkArgument(
          this.batchedReaderFunc == null,
          "Reader function cannot be set since the batched version is already set");
      this.readerFunc = readerFunction;
      return this;
    }

    public ReadBuilder createBatchedReaderFunc(
        Function<TypeDescription, OrcBatchReader<?>> batchReaderFunction) {
      Preconditions.checkArgument(
          this.readerFunc == null,
          "Batched reader function cannot be set since the non-batched version is already set");
      this.batchedReaderFunc = batchReaderFunction;
      return this;
    }

    @Override
    public <D> CloseableIterable<D> build() {
      Preconditions.checkNotNull(schema(), "Schema is required");
      return new OrcIterable<>(
          file(),
          conf,
          schema(),
          nameMapping(),
          start(),
          length(),
          readerFunc,
          isCaseSensitive(),
          filter(),
          batchedReaderFunc,
          recordsPerBatch());
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

  public static class ReaderService implements DataFileReaderService {
    @Override
    public FileFormat format() {
      return FileFormat.ORC;
    }

    @Override
    public Class<?> returnType() {
      return Record.class;
    }

    @Override
    public FileFormatReadBuilder<?> builder(
        InputFile inputFile,
        ContentScanTask<?> task,
        Schema readSchema,
        Table table,
        DeleteFilter<?> deleteFilter) {
      Map<Integer, ?> idToConstant = PartitionUtil.constantsMap(task, readSchema);
      return new ReadBuilder(inputFile)
          .project(ORC.schemaWithoutConstantAndMetadataFields(readSchema, idToConstant))
          .createReaderFunc(
              fileSchema -> GenericOrcReader.buildReader(readSchema, fileSchema, idToConstant));
    }
  }

  public static class WriterService implements DataFileWriterService<Schema> {
    @Override
    public FileFormat format() {
      return FileFormat.ORC;
    }

    @Override
    public Class<?> returnType() {
      return Record.class;
    }

    @Override
    public FileFormatAppenderBuilder<?> appenderBuilder(
        EncryptedOutputFile outputFile, Schema rowType) {
      return ORC.write(outputFile).createWriterFunc(GenericOrcWriter::buildWriter);
    }

    @Override
    public FileFormatDataWriterBuilder<?> dataWriterBuilder(
        EncryptedOutputFile outputFile, Schema rowType) {
      return new DataWriteBuilder(outputFile.encryptingOutputFile())
          .createWriterFunc(GenericOrcWriter::buildWriter);
    }

    @Override
    public FileFormatEqualityDeleteWriterBuilder<?> equalityDeleteWriterBuilder(
        EncryptedOutputFile outputFile, Schema rowType) {
      return new DeleteWriteBuilder(outputFile.encryptingOutputFile())
          .createWriterFunc(GenericOrcWriter::buildWriter);
    }

    @Override
    public FileFormatPositionDeleteWriterBuilder<?> positionDeleteWriterBuilder(
        EncryptedOutputFile outputFile, Schema rowType) {
      return new DeleteWriteBuilder(outputFile.encryptingOutputFile())
          .createWriterFunc(GenericOrcWriter::buildWriter);
    }
  }
}
