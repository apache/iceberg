/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.parquet;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.SchemaParser;
import com.netflix.iceberg.avro.AvroSchemaUtil;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.io.FileAppender;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.io.OutputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static com.netflix.iceberg.TableProperties.PARQUET_COMPRESSION;
import static com.netflix.iceberg.TableProperties.PARQUET_COMPRESSION_DEFAULT;
import static com.netflix.iceberg.TableProperties.PARQUET_DICT_SIZE_BYTES;
import static com.netflix.iceberg.TableProperties.PARQUET_DICT_SIZE_BYTES_DEFAULT;
import static com.netflix.iceberg.TableProperties.PARQUET_PAGE_SIZE_BYTES;
import static com.netflix.iceberg.TableProperties.PARQUET_PAGE_SIZE_BYTES_DEFAULT;
import static com.netflix.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static com.netflix.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT;

public class Parquet {
  private Parquet() {
  }

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

    private WriteBuilder(OutputFile file) {
      this.file = file;
    }

    public WriteBuilder schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public WriteBuilder named(String name) {
      this.name = name;
      return this;
    }

    public WriteBuilder writeSupport(WriteSupport<?> writeSupport) {
      this.writeSupport = writeSupport;
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

    void forwardConfig(String parquetProperty, String icebergProperty, String defaultValue) {
      String value = config.getOrDefault(icebergProperty, defaultValue);
      if (value != null) {
        set(parquetProperty, value);
      }
    }

    public <D> FileAppender<D> build() throws IOException {
      Preconditions.checkNotNull(schema, "Schema is required");
      Preconditions.checkNotNull(name, "Table name is required and cannot be null");

      // add the Iceberg schema to keyValueMetadata
      meta("iceberg.schema", SchemaParser.toJson(schema));

      // add Parquet configuration
      forwardConfig("parquet.block.size",
          PARQUET_ROW_GROUP_SIZE_BYTES, PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT);
      forwardConfig("parquet.page.size",
          PARQUET_PAGE_SIZE_BYTES, PARQUET_PAGE_SIZE_BYTES_DEFAULT);
      forwardConfig("parquet.dictionary.page.size",
          PARQUET_DICT_SIZE_BYTES, PARQUET_DICT_SIZE_BYTES_DEFAULT);

      set("parquet.avro.write-old-list-structure", "false");
      MessageType type = ParquetSchemaUtil.convert(schema, name);

      return new ParquetWriteAdapter<>(new ParquetWriteBuilder<D>(ParquetIO.file(file))
          .setType(type)
          .setConfig(config)
          .setKeyValueMetadata(metadata)
          .setWriteSupport(getWriteSupport(type))
          .withCompressionCodec(codec()) // TODO: support codecs
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE) // TODO: support modes
          .build());
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
    private boolean filterRecords = true;
    private Map<String, String> properties = Maps.newHashMap();
    private boolean callInit = false;

    private ReadBuilder(InputFile file) {
      this.file = file;
    }

    /**
     * Restricts the read to the given range: [start, start + length).
     *
     * @param start the start position for this read
     * @param length the length of the range this read should scan
     * @return this builder for method chaining
     */
    public ReadBuilder split(long start, long length) {
      this.start = start;
      this.length = length;
      return this;
    }

    public ReadBuilder project(Schema schema) {
      this.schema = schema;
      return this;
    }

    public ReadBuilder filterRecords(boolean filterRecords) {
      this.filterRecords = filterRecords;
      return this;
    }

    public ReadBuilder filter(Expression filter) {
      this.filter = filter;
      return this;
    }

    public ReadBuilder readSupport(ReadSupport<?> readSupport) {
      this.readSupport = readSupport;
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

    @SuppressWarnings("unchecked")
    public <D> ParquetIterable<D> build() {
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
            .withFilter(ParquetFilters.convert(fileSchema, filter));
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

    public ParquetReadBuilder<T> project(Schema schema) {
      this.schema = schema;
      return this;
    }

    public ParquetReadBuilder<T> readSupport(ReadSupport<T> readSupport) {
      this.readSupport = readSupport;
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
