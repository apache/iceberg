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
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFileReaderService;
import org.apache.iceberg.DataFileWriterService;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
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
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PartitionUtil;

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
    DEFAULT_MODEL.addLogicalTypeConversion(new Conversions.DecimalConversion());
    DEFAULT_MODEL.addLogicalTypeConversion(new UUIDConversion());
  }

  public static WriteBuilder write(OutputFile file) {
    return new WriteBuilder(file);
  }

  public static WriteBuilder write(EncryptedOutputFile file) {
    return new WriteBuilder(file.encryptingOutputFile());
  }

  public static class WriteBuilder extends FileFormatAppenderBuilderBase<WriteBuilder> {
    private Function<Schema, DatumWriter<?>> createWriterFunc = null;
    private Function<Map<String, String>, Context> createContextFunc = Context::dataContext;

    private WriteBuilder(OutputFile file) {
      super(file);
    }

    public WriteBuilder createWriterFunc(Function<Schema, DatumWriter<?>> writerFunction) {
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
    public <D> FileAppender<D> build() throws IOException {
      Preconditions.checkNotNull(schema(), "Schema is required");
      Preconditions.checkNotNull(name(), "Table name is required and cannot be null");

      Function<Schema, DatumWriter<?>> writerFunc;
      if (createWriterFunc != null) {
        writerFunc = createWriterFunc;
      } else {
        writerFunc = GenericAvroWriter::new;
      }

      // add the Iceberg schema to keyValueMetadata
      meta("iceberg.schema", SchemaParser.toJson(schema()));

      Context context = createContextFunc.apply(config());
      CodecFactory codec = context.codec();

      return new AvroFileAppender<>(
          schema(),
          AvroSchemaUtil.convert(schema(), name()),
          file(),
          writerFunc,
          codec,
          metadata(),
          metricsConfig(),
          isOverwrite());
    }

    private static class Context {
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

  public static DataWriteBuilder writeData(OutputFile file) {
    return new DataWriteBuilder(file);
  }

  public static DataWriteBuilder writeData(EncryptedOutputFile file) {
    return new DataWriteBuilder(file.encryptingOutputFile());
  }

  public static class DataWriteBuilder extends FileFormatDataWriterBuilderBase<DataWriteBuilder> {
    private DataWriteBuilder(OutputFile file) {
      super(write(file), FileFormat.AVRO);
    }

    public DataWriteBuilder createWriterFunc(Function<Schema, DatumWriter<?>> newCreateWriterFunc) {
      ((WriteBuilder) appenderBuilder()).createWriterFunc(newCreateWriterFunc);
      return this;
    }
  }

  public static DeleteWriteBuilder writeDeletes(OutputFile file) {
    return new DeleteWriteBuilder(file);
  }

  public static DeleteWriteBuilder writeDeletes(EncryptedOutputFile file) {
    return new DeleteWriteBuilder(file.encryptingOutputFile());
  }

  public static class DeleteWriteBuilder
      extends FileFormatEqualityDeleteWriterBuilderBase<DeleteWriteBuilder>  implements FileFormatPositionDeleteWriterBuilder<DeleteWriteBuilder> {
    private Function<Schema, DatumWriter<?>> createWriterFunc = null;

    private DeleteWriteBuilder(OutputFile file) {
      super(write(file), FileFormat.AVRO);
    }

    public DeleteWriteBuilder createWriterFunc(Function<Schema, DatumWriter<?>> writerFunction) {
      this.createWriterFunc = writerFunction;
      return this;
    }

    @Deprecated
    public DeleteWriteBuilder rowSchema(org.apache.iceberg.Schema newSchema) {
      return schema(newSchema);
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
          FileFormat.AVRO,
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
        // the appender uses the row schema wrapped with position fields
        appenderBuilder().schema(DeleteSchemaUtil.posDeleteSchema(appenderBuilder().schema()));

        ((WriteBuilder) appenderBuilder())
            .createWriterFunc(
                avroSchema -> new PositionAndRowDatumWriter<>(createWriterFunc.apply(avroSchema)));

      } else {
        appenderBuilder().schema(DeleteSchemaUtil.pathPosSchema());

        // We ignore the 'createWriterFunc' and 'rowSchema' even if is provided, since we do not
        // write row data itself
        ((WriteBuilder) appenderBuilder()).createWriterFunc(ignored -> new PositionDatumWriter());
      }

      ((WriteBuilder) appenderBuilder()).createContextFunc(WriteBuilder.Context::deleteContext);

      return new PositionDeleteWriter<>(
          appenderBuilder().build(),
          FileFormat.AVRO,
          appenderBuilder().location(),
          spec(),
          partition(),
          keyMetadata());
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

  public static class ReadBuilder extends FileFormatReadBuilderBase<ReadBuilder> {
    private final Map<String, String> renames = Maps.newLinkedHashMap();
    private ClassLoader loader = Thread.currentThread().getContextClassLoader();
    private Function<Schema, DatumReader<?>> createReaderFunc = null;
    private BiFunction<org.apache.iceberg.Schema, Schema, DatumReader<?>> createReaderBiFunc = null;
    private Function<org.apache.iceberg.Schema, DatumReader<?>> createResolvingReaderFunc = null;

    @SuppressWarnings("UnnecessaryLambda")
    private final Function<org.apache.iceberg.Schema, DatumReader<?>> defaultCreateReaderFunc =
        readSchema -> {
          GenericAvroReader<?> reader = GenericAvroReader.create(readSchema);
          reader.setClassLoader(loader);
          return reader;
        };

    private ReadBuilder(InputFile file) {
      super(file);
    }

    public ReadBuilder createResolvingReader(
        Function<org.apache.iceberg.Schema, DatumReader<?>> readerFunction) {
      Preconditions.checkState(
          createReaderBiFunc == null && createReaderFunc == null,
          "Cannot set multiple read builder functions");
      this.createResolvingReaderFunc = readerFunction;
      return this;
    }

    public ReadBuilder createReaderFunc(Function<Schema, DatumReader<?>> readerFunction) {
      Preconditions.checkState(
          createReaderBiFunc == null && createResolvingReaderFunc == null,
          "Cannot set multiple read builder functions");
      this.createReaderFunc = readerFunction;
      return this;
    }

    public ReadBuilder createReaderFunc(
        BiFunction<org.apache.iceberg.Schema, Schema, DatumReader<?>> readerFunction) {
      Preconditions.checkState(
          createReaderFunc == null && createResolvingReaderFunc == null,
          "Cannot set multiple read builder functions");
      this.createReaderBiFunc = readerFunction;
      return this;
    }

    public ReadBuilder rename(String fullName, String newName) {
      renames.put(fullName, newName);
      return this;
    }

    public ReadBuilder classLoader(ClassLoader classLoader) {
      this.loader = classLoader;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <D> AvroIterable<D> build() {
      Preconditions.checkNotNull(schema(), "Schema is required");

      NameMapping nameMapping =
          nameMapping() != null ? nameMapping() : MappingUtil.create(schema());

      DatumReader<D> reader;
      if (createReaderBiFunc != null) {
        reader =
            new ProjectionDatumReader<>(
                avroSchema -> createReaderBiFunc.apply(schema(), avroSchema),
                schema(),
                renames,
                null);
      } else if (createReaderFunc != null) {
        reader = new ProjectionDatumReader<>(createReaderFunc, schema(), renames, null);
      } else if (createResolvingReaderFunc != null) {
        reader = (DatumReader<D>) createResolvingReaderFunc.apply(schema());
      } else {
        reader = (DatumReader<D>) defaultCreateReaderFunc.apply(schema());
      }

      if (reader instanceof SupportsCustomRecords) {
        ((SupportsCustomRecords) reader).setClassLoader(loader);
        ((SupportsCustomRecords) reader).setRenames(renames);
      }

      return new AvroIterable<>(
          file(),
          new NameMappingDatumReader<>(nameMapping, reader),
          start(),
          length(),
          isReuseContainers());
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

  public static class ReaderService implements DataFileReaderService {
    @Override
    public FileFormat format() {
      return FileFormat.AVRO;
    }

    @Override
    public Class<?> returnType() {
      return Record.class;
    }

    @Override
    public FileFormatReadBuilder<?> builder(
        InputFile inputFile,
        ContentScanTask<?> task,
        org.apache.iceberg.Schema readSchema,
        Table table,
        DeleteFilter<?> deleteFilter) {
      return Avro.read(inputFile)
          .project(readSchema)
          .createResolvingReader(
              fileSchema ->
                  PlannedDataReader.create(
                      fileSchema, PartitionUtil.constantsMap(task, readSchema)));
    }
  }

  public static class WriterService implements DataFileWriterService {
    @Override
    public FileFormat format() {
      return FileFormat.AVRO;
    }

    @Override
    public Class<?> returnType() {
      return Record.class;
    }

    @Override
    public FileFormatAppenderBuilder<?> appenderBuilder(EncryptedOutputFile outputFile) {
      return Avro.write(outputFile)
          .createWriterFunc(DataWriter::create);
    }

    @Override
    public FileFormatDataWriterBuilder<?> dataWriterBuilder(EncryptedOutputFile outputFile) {
      return new DataWriteBuilder(outputFile.encryptingOutputFile()).createWriterFunc(DataWriter::create);
    }

    @Override
    public FileFormatEqualityDeleteWriterBuilder<?> equalityDeleteWriterBuilder(EncryptedOutputFile outputFile) {
      return new DeleteWriteBuilder(outputFile.encryptingOutputFile()).createWriterFunc(DataWriter::create);
    }

    @Override
    public FileFormatPositionDeleteWriterBuilder<?> positionDeleteWriterBuilder(EncryptedOutputFile outputFile) {
      return new DeleteWriteBuilder(outputFile.encryptingOutputFile()).createWriterFunc(DataWriter::create);
    }
  }
}
