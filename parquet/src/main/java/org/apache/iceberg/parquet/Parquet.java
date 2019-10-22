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

package org.apache.iceberg.parquet;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_DICT_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.PARQUET_DICT_SIZE_BYTES_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_PAGE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.PARQUET_PAGE_SIZE_BYTES_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT;

public class Parquet {
  private Parquet() {
  }

  private static final Collection<String> READ_PROPERTIES_TO_REMOVE = Sets.newHashSet(
      "parquet.read.filter", "parquet.private.read.filter.predicate", "parquet.read.support.class");

  public static WriteBuilder write(OutputFile file) {
    return new WriteBuilder(file);
  }

  public static class WriteBuilder {
    private final OutputFile file;
    private Schema schema = null;
    private String name = "table";
    private WriteSupport<?> writeSupport = null;
    private Map<String, String> metadata = Maps.newLinkedHashMap();
    private Map<String, String> config = Maps.newLinkedHashMap();
    private Function<MessageType, ParquetValueWriter<?>> createWriterFunc = null;
    private MetricsConfig metricsConfig = MetricsConfig.getDefault();
    private ParquetFileWriter.Mode writeMode = ParquetFileWriter.Mode.CREATE;

    private WriteBuilder(OutputFile file) {
      this.file = file;
    }

    public WriteBuilder forTable(Table table) {
      schema(table.schema());
      setAll(table.properties());
      metricsConfig(MetricsConfig.fromProperties(table.properties()));
      return this;
    }

    public WriteBuilder schema(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    public WriteBuilder named(String newName) {
      this.name = newName;
      return this;
    }

    public WriteBuilder writeSupport(WriteSupport<?> newWriteSupport) {
      this.writeSupport = newWriteSupport;
      return this;
    }

    public WriteBuilder set(String property, String value) {
      config.put(property, value);
      return this;
    }

    public WriteBuilder setAll(Map<String, String> properties) {
      config.putAll(properties);
      return this;
    }

    public WriteBuilder meta(String property, String value) {
      metadata.put(property, value);
      return this;
    }

    public WriteBuilder createWriterFunc(Function<MessageType, ParquetValueWriter<?>> newCreateWriterFunc) {
      this.createWriterFunc = newCreateWriterFunc;
      return this;
    }

    public WriteBuilder metricsConfig(MetricsConfig newMetricsConfig) {
      this.metricsConfig = newMetricsConfig;
      return this;
    }

    public WriteBuilder overwrite() {
      return overwrite(true);
    }

    public WriteBuilder overwrite(boolean enabled) {
      this.writeMode = enabled ? ParquetFileWriter.Mode.OVERWRITE : ParquetFileWriter.Mode.CREATE;
      return this;
    }

    @SuppressWarnings("unchecked")
    private <T> WriteSupport<T> getWriteSupport(MessageType type) {
      if (writeSupport != null) {
        return (WriteSupport<T>) writeSupport;
      } else {
        return new AvroWriteSupport<>(
            type,
            ParquetAvro.parquetAvroSchema(AvroSchemaUtil.convert(schema, name)),
            ParquetAvro.DEFAULT_MODEL);
      }
    }

    private CompressionCodecName codec() {
      String codec = config.getOrDefault(PARQUET_COMPRESSION, PARQUET_COMPRESSION_DEFAULT);
      try {
        return CompressionCodecName.valueOf(codec.toUpperCase(Locale.ENGLISH));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Unsupported compression codec: " + codec);
      }
    }

    public <D> FileAppender<D> build() throws IOException {
      Preconditions.checkNotNull(schema, "Schema is required");
      Preconditions.checkNotNull(name, "Table name is required and cannot be null");

      // add the Iceberg schema to keyValueMetadata
      meta("iceberg.schema", SchemaParser.toJson(schema));

      // Map Iceberg properties to pass down to the Parquet writer
      int rowGroupSize = Integer.parseInt(config.getOrDefault(
          PARQUET_ROW_GROUP_SIZE_BYTES, PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT));
      int pageSize = Integer.parseInt(config.getOrDefault(
          PARQUET_PAGE_SIZE_BYTES, PARQUET_PAGE_SIZE_BYTES_DEFAULT));
      int dictionaryPageSize = Integer.parseInt(config.getOrDefault(
          PARQUET_DICT_SIZE_BYTES, PARQUET_DICT_SIZE_BYTES_DEFAULT));

      WriterVersion writerVersion = WriterVersion.PARQUET_1_0;

      set("parquet.avro.write-old-list-structure", "false");
      MessageType type = ParquetSchemaUtil.convert(schema, name);

      if (createWriterFunc != null) {
        Preconditions.checkArgument(writeSupport == null,
            "Cannot write with both write support and Parquet value writer");
        Configuration conf;
        if (file instanceof HadoopOutputFile) {
          conf = ((HadoopOutputFile) file).getConf();
        } else {
          conf = new Configuration();
        }

        for (Map.Entry<String, String> entry : config.entrySet()) {
          conf.set(entry.getKey(), entry.getValue());
        }

        ParquetProperties parquetProperties = ParquetProperties.builder()
            .withWriterVersion(writerVersion)
            .withPageSize(pageSize)
            .withDictionaryPageSize(dictionaryPageSize)
            .build();

        return new org.apache.iceberg.parquet.ParquetWriter<>(
            conf, file, schema, rowGroupSize, metadata, createWriterFunc, codec(),
            parquetProperties, metricsConfig, writeMode);
      } else {
        return new ParquetWriteAdapter<>(new ParquetWriteBuilder<D>(ParquetIO.file(file))
            .withWriterVersion(writerVersion)
            .setType(type)
            .setConfig(config)
            .setKeyValueMetadata(metadata)
            .setWriteSupport(getWriteSupport(type))
            .withCompressionCodec(codec())
            .withWriteMode(writeMode)
            .withRowGroupSize(rowGroupSize)
            .withPageSize(pageSize)
            .withDictionaryPageSize(dictionaryPageSize)
            .build(),
            metricsConfig);
      }
    }
  }

  private static class ParquetWriteBuilder<T> extends ParquetWriter.Builder<T, ParquetWriteBuilder<T>> {
    private Map<String, String> keyValueMetadata = Maps.newHashMap();
    private Map<String, String> config = Maps.newHashMap();
    private MessageType type;
    private WriteSupport<T> writeSupport;

    private ParquetWriteBuilder(org.apache.parquet.io.OutputFile path) {
      super(path);
    }

    @Override
    protected ParquetWriteBuilder<T> self() {
      return this;
    }

    public ParquetWriteBuilder<T> setKeyValueMetadata(Map<String, String> keyValueMetadata) {
      this.keyValueMetadata = keyValueMetadata;
      return self();
    }

    public ParquetWriteBuilder<T> setConfig(Map<String, String> config) {
      this.config = config;
      return self();
    }

    public ParquetWriteBuilder<T> setType(MessageType type) {
      this.type = type;
      return self();
    }

    public ParquetWriteBuilder<T> setWriteSupport(WriteSupport<T> writeSupport) {
      this.writeSupport = writeSupport;
      return self();
    }

    @Override
    protected WriteSupport<T> getWriteSupport(Configuration configuration) {
      for (Map.Entry<String, String> entry : config.entrySet()) {
        configuration.set(entry.getKey(), entry.getValue());
      }
      return new ParquetWriteSupport<>(type, keyValueMetadata, writeSupport);
    }
  }

  public static ReadBuilder read(InputFile file) {
    return new ReadBuilder(file);
  }

  public static class ReadBuilder {
    private final InputFile file;
    private Long start = null;
    private Long length = null;
    private Schema schema = null;
    private Expression filter = null;
    private ReadSupport<?> readSupport = null;
    private Function<MessageType, ParquetValueReader<?>> readerFunc = null;
    private boolean filterRecords = true;
    private boolean caseSensitive = true;
    private Map<String, String> properties = Maps.newHashMap();
    private boolean callInit = false;
    private boolean reuseContainers = false;

    private ReadBuilder(InputFile file) {
      this.file = file;
    }

    /**
     * Restricts the read to the given range: [start, start + length).
     *
     * @param newStart the start position for this read
     * @param newLength the length of the range this read should scan
     * @return this builder for method chaining
     */
    public ReadBuilder split(long newStart, long newLength) {
      this.start = newStart;
      this.length = newLength;
      return this;
    }

    public ReadBuilder project(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    public ReadBuilder caseInsensitive() {
      return caseSensitive(false);
    }

    public ReadBuilder caseSensitive(boolean newCaseSensitive) {
      this.caseSensitive = newCaseSensitive;
      return this;
    }

    public ReadBuilder filterRecords(boolean newFilterRecords) {
      this.filterRecords = newFilterRecords;
      return this;
    }

    public ReadBuilder filter(Expression newFilter) {
      this.filter = newFilter;
      return this;
    }

    public ReadBuilder readSupport(ReadSupport<?> newFilterSupport) {
      this.readSupport = newFilterSupport;
      return this;
    }

    public ReadBuilder createReaderFunc(Function<MessageType, ParquetValueReader<?>> newReaderFunction) {
      this.readerFunc = newReaderFunction;
      return this;
    }

    public ReadBuilder set(String key, String value) {
      properties.put(key, value);
      return this;
    }

    public ReadBuilder callInit() {
      this.callInit = true;
      return this;
    }

    public ReadBuilder reuseContainers() {
      this.reuseContainers = true;
      return this;
    }

    @SuppressWarnings("unchecked")
    public <D> CloseableIterable<D> build() {
      if (readerFunc != null) {
        ParquetReadOptions.Builder optionsBuilder;
        if (file instanceof HadoopInputFile) {
          // remove read properties already set that may conflict with this read
          Configuration conf = new Configuration(((HadoopInputFile) file).getConf());
          for (String property : READ_PROPERTIES_TO_REMOVE) {
            conf.unset(property);
          }
          optionsBuilder = HadoopReadOptions.builder(conf);
        } else {
          optionsBuilder = ParquetReadOptions.builder();
        }

        for (Map.Entry<String, String> entry : properties.entrySet()) {
          optionsBuilder.set(entry.getKey(), entry.getValue());
        }

        if (start != null) {
          optionsBuilder.withRange(start, start + length);
        }

        ParquetReadOptions options = optionsBuilder.build();

        return new org.apache.iceberg.parquet.ParquetReader<>(
            file, schema, options, readerFunc, filter, reuseContainers, caseSensitive);
      }

      ParquetReadBuilder<D> builder = new ParquetReadBuilder<>(ParquetIO.file(file));

      builder.project(schema);

      if (readSupport != null) {
        builder.readSupport((ReadSupport<D>) readSupport);
      } else {
        builder.readSupport(new AvroReadSupport<>(ParquetAvro.DEFAULT_MODEL));
      }

      // default options for readers
      builder.set("parquet.strict.typing", "false") // allow type promotion
          .set("parquet.avro.compatible", "false") // use the new RecordReader with Utf8 support
          .set("parquet.avro.add-list-element-records", "false"); // assume that lists use a 3-level schema

      for (Map.Entry<String, String> entry : properties.entrySet()) {
        builder.set(entry.getKey(), entry.getValue());
      }

      if (filter != null) {
        // TODO: should not need to get the schema to push down before opening the file.
        // Parquet should allow setting a filter inside its read support
        MessageType type;
        try (ParquetFileReader schemaReader = ParquetFileReader.open(ParquetIO.file(file))) {
          type = schemaReader.getFileMetaData().getSchema();
        } catch (IOException e) {
          throw new RuntimeIOException(e);
        }
        Schema fileSchema = ParquetSchemaUtil.convert(type);
        builder.useStatsFilter()
            .useDictionaryFilter()
            .useRecordFilter(filterRecords)
            .withFilter(ParquetFilters.convert(fileSchema, filter, caseSensitive));
      } else {
        // turn off filtering
        builder.useStatsFilter(false)
            .useDictionaryFilter(false)
            .useRecordFilter(false);
      }

      if (callInit) {
        builder.callInit();
      }

      if (start != null) {
        builder.withFileRange(start, start + length);
      }

      return new ParquetIterable<>(builder);
    }
  }

  private static class ParquetReadBuilder<T> extends ParquetReader.Builder<T> {
    private Schema schema = null;
    private ReadSupport<T> readSupport = null;
    private boolean callInit = false;

    private ParquetReadBuilder(org.apache.parquet.io.InputFile file) {
      super(file);
    }

    public ParquetReadBuilder<T> project(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    public ParquetReadBuilder<T> readSupport(ReadSupport<T> newReadSupport) {
      this.readSupport = newReadSupport;
      return this;
    }

    public ParquetReadBuilder<T> callInit() {
      this.callInit = true;
      return this;
    }

    @Override
    protected ReadSupport<T> getReadSupport() {
      return new ParquetReadSupport<>(schema, readSupport, callInit);
    }
  }
}
