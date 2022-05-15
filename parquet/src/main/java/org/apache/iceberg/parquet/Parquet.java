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

import static org.apache.iceberg.TableProperties.DELETE_PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.DELETE_PARQUET_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.DELETE_PARQUET_DICT_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.DELETE_PARQUET_PAGE_ROW_LIMIT;
import static org.apache.iceberg.TableProperties.DELETE_PARQUET_PAGE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.DELETE_PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT;
import static org.apache.iceberg.TableProperties.DELETE_PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT;
import static org.apache.iceberg.TableProperties.DELETE_PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX;
import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_MAX_BYTES;
import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_MAX_BYTES_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_LEVEL_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_DICT_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.PARQUET_DICT_SIZE_BYTES_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_PAGE_ROW_LIMIT;
import static org.apache.iceberg.TableProperties.PARQUET_PAGE_ROW_LIMIT_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_PAGE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.PARQUET_PAGE_SIZE_BYTES_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
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
import org.apache.iceberg.parquet.ParquetValueWriters.PositionDeleteStructWriter;
import org.apache.iceberg.parquet.ParquetValueWriters.StructWriter;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

public class Parquet {
  private Parquet() {}

  private static final Collection<String> READ_PROPERTIES_TO_REMOVE =
      Sets.newHashSet(
          "parquet.read.filter",
          "parquet.private.read.filter.predicate",
          "parquet.read.support.class",
          "parquet.crypto.factory.class");

  public static WriteBuilder write(OutputFile file) {
    return new WriteBuilder(file);
  }

  public static class WriteBuilder {
    private final OutputFile file;
    private final Configuration conf;
    private final Map<String, String> metadata = Maps.newLinkedHashMap();
    private final Map<String, String> config = Maps.newLinkedHashMap();
    private Schema schema = null;
    private String name = "table";
    private WriteSupport<?> writeSupport = null;
    private Function<MessageType, ParquetValueWriter<?>> createWriterFunc = null;
    private MetricsConfig metricsConfig = MetricsConfig.getDefault();
    private ParquetFileWriter.Mode writeMode = ParquetFileWriter.Mode.CREATE;
    private WriterVersion writerVersion = WriterVersion.PARQUET_1_0;
    private Function<Map<String, String>, Context> createContextFunc = Context::dataContext;
    private ByteBuffer fileEncryptionKey = null;
    private ByteBuffer fileAADPrefix = null;

    private WriteBuilder(OutputFile file) {
      this.file = file;
      if (file instanceof HadoopOutputFile) {
        this.conf = new Configuration(((HadoopOutputFile) file).getConf());
      } else {
        this.conf = new Configuration();
      }
    }

    public WriteBuilder forTable(Table table) {
      schema(table.schema());
      setAll(table.properties());
      metricsConfig(MetricsConfig.forTable(table));
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

    public WriteBuilder createWriterFunc(
        Function<MessageType, ParquetValueWriter<?>> newCreateWriterFunc) {
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

    public WriteBuilder writerVersion(WriterVersion version) {
      this.writerVersion = version;
      return this;
    }

    public WriteBuilder withFileEncryptionKey(ByteBuffer encryptionKey) {
      this.fileEncryptionKey = encryptionKey;
      return this;
    }

    public WriteBuilder withAADPrefix(ByteBuffer aadPrefix) {
      this.fileAADPrefix = aadPrefix;
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

    /*
     * Sets the writer version. Default value is PARQUET_1_0 (v1).
     */
    @VisibleForTesting
    WriteBuilder withWriterVersion(WriterVersion version) {
      this.writerVersion = version;
      return this;
    }

    // supposed to always be a private method used strictly by data and delete write builders
    private WriteBuilder createContextFunc(
        Function<Map<String, String>, Context> newCreateContextFunc) {
      this.createContextFunc = newCreateContextFunc;
      return this;
    }

    public <D> FileAppender<D> build() throws IOException {
      Preconditions.checkNotNull(schema, "Schema is required");
      Preconditions.checkNotNull(name, "Table name is required and cannot be null");

      // add the Iceberg schema to keyValueMetadata
      meta("iceberg.schema", SchemaParser.toJson(schema));

      // Map Iceberg properties to pass down to the Parquet writer
      Context context = createContextFunc.apply(config);

      int rowGroupSize = context.rowGroupSize();
      int pageSize = context.pageSize();
      int pageRowLimit = context.pageRowLimit();
      int dictionaryPageSize = context.dictionaryPageSize();
      String compressionLevel = context.compressionLevel();
      CompressionCodecName codec = context.codec();
      int rowGroupCheckMinRecordCount = context.rowGroupCheckMinRecordCount();
      int rowGroupCheckMaxRecordCount = context.rowGroupCheckMaxRecordCount();
      int bloomFilterMaxBytes = context.bloomFilterMaxBytes();
      Map<String, String> columnBloomFilterEnabled = context.columnBloomFilterEnabled();
      boolean dictionaryEnabled = context.dictionaryEnabled();

      if (compressionLevel != null) {
        switch (codec) {
          case GZIP:
            config.put("zlib.compress.level", compressionLevel);
            break;
          case BROTLI:
            config.put("compression.brotli.quality", compressionLevel);
            break;
          case ZSTD:
            // keep "io.compression.codec.zstd.level" for backwards compatibility
            config.put("io.compression.codec.zstd.level", compressionLevel);
            config.put("parquet.compression.codec.zstd.level", compressionLevel);
            break;
          default:
            // compression level is not supported; ignore it
        }
      }

      set("parquet.avro.write-old-list-structure", "false");
      MessageType type = ParquetSchemaUtil.convert(schema, name);

      FileEncryptionProperties fileEncryptionProperties = null;
      if (fileEncryptionKey != null) {
        byte[] encryptionKeyArray = ByteBuffers.toByteArray(fileEncryptionKey);
        byte[] aadPrefixArray = ByteBuffers.toByteArray(fileAADPrefix);

        fileEncryptionProperties =
            FileEncryptionProperties.builder(encryptionKeyArray)
                .withAADPrefix(aadPrefixArray)
                .withoutAADPrefixStorage()
                .build();
      } else {
        Preconditions.checkState(fileAADPrefix == null, "AAD prefix set with null encryption key");
      }

      if (createWriterFunc != null) {
        Preconditions.checkArgument(
            writeSupport == null, "Cannot write with both write support and Parquet value writer");

        for (Map.Entry<String, String> entry : config.entrySet()) {
          conf.set(entry.getKey(), entry.getValue());
        }

        ParquetProperties.Builder propsBuilder =
            ParquetProperties.builder()
                .withWriterVersion(writerVersion)
                .withPageSize(pageSize)
                .withPageRowCountLimit(pageRowLimit)
                .withDictionaryEncoding(dictionaryEnabled)
                .withDictionaryPageSize(dictionaryPageSize)
                .withMinRowCountForPageSizeCheck(rowGroupCheckMinRecordCount)
                .withMaxRowCountForPageSizeCheck(rowGroupCheckMaxRecordCount)
                .withMaxBloomFilterBytes(bloomFilterMaxBytes);

        for (Map.Entry<String, String> entry : columnBloomFilterEnabled.entrySet()) {
          String colPath = entry.getKey();
          String bloomEnabled = entry.getValue();
          propsBuilder.withBloomFilterEnabled(colPath, Boolean.valueOf(bloomEnabled));
        }

        ParquetProperties parquetProperties = propsBuilder.build();

        return new org.apache.iceberg.parquet.ParquetWriter<>(
            conf,
            file,
            schema,
            rowGroupSize,
            metadata,
            createWriterFunc,
            codec,
            parquetProperties,
            metricsConfig,
            writeMode,
            fileEncryptionProperties);
      } else {
        ParquetWriteBuilder<D> parquetWriteBuilder =
            new ParquetWriteBuilder<D>(ParquetIO.file(file))
                .withWriterVersion(writerVersion)
                .setType(type)
                .setConfig(config)
                .setKeyValueMetadata(metadata)
                .setWriteSupport(getWriteSupport(type))
                .withCompressionCodec(codec)
                .withWriteMode(writeMode)
                .withRowGroupSize(rowGroupSize)
                .withPageSize(pageSize)
                .withPageRowCountLimit(pageRowLimit)
                .withDictionaryEncoding(dictionaryEnabled)
                .withDictionaryPageSize(dictionaryPageSize)
                .withEncryption(fileEncryptionProperties);

        for (Map.Entry<String, String> entry : columnBloomFilterEnabled.entrySet()) {
          String colPath = entry.getKey();
          String bloomEnabled = entry.getValue();
          parquetWriteBuilder.withBloomFilterEnabled(colPath, Boolean.valueOf(bloomEnabled));
        }

        return new ParquetWriteAdapter<>(parquetWriteBuilder.build(), metricsConfig);
      }
    }

    private static class Context {
      private final int rowGroupSize;
      private final int pageSize;
      private final int pageRowLimit;
      private final int dictionaryPageSize;
      private final CompressionCodecName codec;
      private final String compressionLevel;
      private final int rowGroupCheckMinRecordCount;
      private final int rowGroupCheckMaxRecordCount;
      private final int bloomFilterMaxBytes;
      private final Map<String, String> columnBloomFilterEnabled;
      private final boolean dictionaryEnabled;

      private Context(
          int rowGroupSize,
          int pageSize,
          int pageRowLimit,
          int dictionaryPageSize,
          CompressionCodecName codec,
          String compressionLevel,
          int rowGroupCheckMinRecordCount,
          int rowGroupCheckMaxRecordCount,
          int bloomFilterMaxBytes,
          Map<String, String> columnBloomFilterEnabled,
          boolean dictionaryEnabled) {
        this.rowGroupSize = rowGroupSize;
        this.pageSize = pageSize;
        this.pageRowLimit = pageRowLimit;
        this.dictionaryPageSize = dictionaryPageSize;
        this.codec = codec;
        this.compressionLevel = compressionLevel;
        this.rowGroupCheckMinRecordCount = rowGroupCheckMinRecordCount;
        this.rowGroupCheckMaxRecordCount = rowGroupCheckMaxRecordCount;
        this.bloomFilterMaxBytes = bloomFilterMaxBytes;
        this.columnBloomFilterEnabled = columnBloomFilterEnabled;
        this.dictionaryEnabled = dictionaryEnabled;
      }

      static Context dataContext(Map<String, String> config) {
        int rowGroupSize =
            PropertyUtil.propertyAsInt(
                config, PARQUET_ROW_GROUP_SIZE_BYTES, PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT);
        Preconditions.checkArgument(rowGroupSize > 0, "Row group size must be > 0");

        int pageSize =
            PropertyUtil.propertyAsInt(
                config, PARQUET_PAGE_SIZE_BYTES, PARQUET_PAGE_SIZE_BYTES_DEFAULT);
        Preconditions.checkArgument(pageSize > 0, "Page size must be > 0");

        int pageRowLimit =
            PropertyUtil.propertyAsInt(
                config, PARQUET_PAGE_ROW_LIMIT, PARQUET_PAGE_ROW_LIMIT_DEFAULT);
        Preconditions.checkArgument(pageRowLimit > 0, "Page row count limit must be > 0");

        int dictionaryPageSize =
            PropertyUtil.propertyAsInt(
                config, PARQUET_DICT_SIZE_BYTES, PARQUET_DICT_SIZE_BYTES_DEFAULT);
        Preconditions.checkArgument(dictionaryPageSize > 0, "Dictionary page size must be > 0");

        String codecAsString =
            config.getOrDefault(PARQUET_COMPRESSION, PARQUET_COMPRESSION_DEFAULT);
        CompressionCodecName codec = toCodec(codecAsString);

        String compressionLevel =
            config.getOrDefault(PARQUET_COMPRESSION_LEVEL, PARQUET_COMPRESSION_LEVEL_DEFAULT);

        int rowGroupCheckMinRecordCount =
            PropertyUtil.propertyAsInt(
                config,
                PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT,
                PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT_DEFAULT);
        Preconditions.checkArgument(
            rowGroupCheckMinRecordCount > 0, "Row group check minimal record count must be > 0");

        int rowGroupCheckMaxRecordCount =
            PropertyUtil.propertyAsInt(
                config,
                PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT,
                PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT_DEFAULT);
        Preconditions.checkArgument(
            rowGroupCheckMaxRecordCount > 0, "Row group check maximum record count must be > 0");
        Preconditions.checkArgument(
            rowGroupCheckMaxRecordCount >= rowGroupCheckMinRecordCount,
            "Row group check maximum record count must be >= minimal record count");

        int bloomFilterMaxBytes =
            PropertyUtil.propertyAsInt(
                config, PARQUET_BLOOM_FILTER_MAX_BYTES, PARQUET_BLOOM_FILTER_MAX_BYTES_DEFAULT);
        Preconditions.checkArgument(bloomFilterMaxBytes > 0, "bloom Filter Max Bytes must be > 0");

        Map<String, String> columnBloomFilterEnabled =
            PropertyUtil.propertiesWithPrefix(config, PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX);

        boolean dictionaryEnabled =
            PropertyUtil.propertyAsBoolean(config, ParquetOutputFormat.ENABLE_DICTIONARY, true);

        return new Context(
            rowGroupSize,
            pageSize,
            pageRowLimit,
            dictionaryPageSize,
            codec,
            compressionLevel,
            rowGroupCheckMinRecordCount,
            rowGroupCheckMaxRecordCount,
            bloomFilterMaxBytes,
            columnBloomFilterEnabled,
            dictionaryEnabled);
      }

      static Context deleteContext(Map<String, String> config) {
        // default delete config using data config
        Context dataContext = dataContext(config);

        int rowGroupSize =
            PropertyUtil.propertyAsInt(
                config, DELETE_PARQUET_ROW_GROUP_SIZE_BYTES, dataContext.rowGroupSize());
        Preconditions.checkArgument(rowGroupSize > 0, "Row group size must be > 0");

        int pageSize =
            PropertyUtil.propertyAsInt(
                config, DELETE_PARQUET_PAGE_SIZE_BYTES, dataContext.pageSize());
        Preconditions.checkArgument(pageSize > 0, "Page size must be > 0");

        int pageRowLimit =
            PropertyUtil.propertyAsInt(
                config, DELETE_PARQUET_PAGE_ROW_LIMIT, dataContext.pageRowLimit());
        Preconditions.checkArgument(pageRowLimit > 0, "Page row count limit must be > 0");

        int dictionaryPageSize =
            PropertyUtil.propertyAsInt(
                config, DELETE_PARQUET_DICT_SIZE_BYTES, dataContext.dictionaryPageSize());
        Preconditions.checkArgument(dictionaryPageSize > 0, "Dictionary page size must be > 0");

        String codecAsString = config.get(DELETE_PARQUET_COMPRESSION);
        CompressionCodecName codec =
            codecAsString != null ? toCodec(codecAsString) : dataContext.codec();

        String compressionLevel =
            config.getOrDefault(DELETE_PARQUET_COMPRESSION_LEVEL, dataContext.compressionLevel());

        int rowGroupCheckMinRecordCount =
            PropertyUtil.propertyAsInt(
                config,
                DELETE_PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT,
                dataContext.rowGroupCheckMinRecordCount());
        Preconditions.checkArgument(
            rowGroupCheckMinRecordCount > 0, "Row group check minimal record count must be > 0");

        int rowGroupCheckMaxRecordCount =
            PropertyUtil.propertyAsInt(
                config,
                DELETE_PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT,
                dataContext.rowGroupCheckMaxRecordCount());
        Preconditions.checkArgument(
            rowGroupCheckMaxRecordCount > 0, "Row group check maximum record count must be > 0");
        Preconditions.checkArgument(
            rowGroupCheckMaxRecordCount >= rowGroupCheckMinRecordCount,
            "Row group check maximum record count must be >= minimal record count");

        boolean dictionaryEnabled =
            PropertyUtil.propertyAsBoolean(config, ParquetOutputFormat.ENABLE_DICTIONARY, true);

        return new Context(
            rowGroupSize,
            pageSize,
            pageRowLimit,
            dictionaryPageSize,
            codec,
            compressionLevel,
            rowGroupCheckMinRecordCount,
            rowGroupCheckMaxRecordCount,
            PARQUET_BLOOM_FILTER_MAX_BYTES_DEFAULT,
            ImmutableMap.of(),
            dictionaryEnabled);
      }

      private static CompressionCodecName toCodec(String codecAsString) {
        try {
          return CompressionCodecName.valueOf(codecAsString.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("Unsupported compression codec: " + codecAsString);
        }
      }

      int rowGroupSize() {
        return rowGroupSize;
      }

      int pageSize() {
        return pageSize;
      }

      int pageRowLimit() {
        return pageRowLimit;
      }

      int dictionaryPageSize() {
        return dictionaryPageSize;
      }

      CompressionCodecName codec() {
        return codec;
      }

      String compressionLevel() {
        return compressionLevel;
      }

      int rowGroupCheckMinRecordCount() {
        return rowGroupCheckMinRecordCount;
      }

      int rowGroupCheckMaxRecordCount() {
        return rowGroupCheckMaxRecordCount;
      }

      int bloomFilterMaxBytes() {
        return bloomFilterMaxBytes;
      }

      Map<String, String> columnBloomFilterEnabled() {
        return columnBloomFilterEnabled;
      }

      boolean dictionaryEnabled() {
        return dictionaryEnabled;
      }
    }
  }

  public static DataWriteBuilder writeData(OutputFile file) {
    return new DataWriteBuilder(file);
  }

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

    public DataWriteBuilder createWriterFunc(
        Function<MessageType, ParquetValueWriter<?>> newCreateWriterFunc) {
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

    public DataWriteBuilder withFileEncryptionKey(ByteBuffer fileEncryptionKey) {
      appenderBuilder.withFileEncryptionKey(fileEncryptionKey);
      return this;
    }

    public DataWriteBuilder withAADPrefix(ByteBuffer aadPrefix) {
      appenderBuilder.withAADPrefix(aadPrefix);
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
          fileAppender, FileFormat.PARQUET, location, spec, partition, keyMetadata, sortOrder);
    }
  }

  public static DeleteWriteBuilder writeDeletes(OutputFile file) {
    return new DeleteWriteBuilder(file);
  }

  public static class DeleteWriteBuilder {
    private final WriteBuilder appenderBuilder;
    private final String location;
    private Function<MessageType, ParquetValueWriter<?>> createWriterFunc = null;
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
      appenderBuilder.meta(property, value);
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
        Function<MessageType, ParquetValueWriter<?>> newCreateWriterFunc) {
      this.createWriterFunc = newCreateWriterFunc;
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

    public DeleteWriteBuilder withFileEncryptionKey(ByteBuffer fileEncryptionKey) {
      appenderBuilder.withFileEncryptionKey(fileEncryptionKey);
      return this;
    }

    public DeleteWriteBuilder withAADPrefix(ByteBuffer aadPrefix) {
      appenderBuilder.withAADPrefix(aadPrefix);
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
      appenderBuilder.createContextFunc(WriteBuilder.Context::deleteContext);

      return new EqualityDeleteWriter<>(
          appenderBuilder.build(),
          FileFormat.PARQUET,
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
            parquetSchema -> {
              ParquetValueWriter<?> writer = createWriterFunc.apply(parquetSchema);
              if (writer instanceof StructWriter) {
                return new PositionDeleteStructWriter<T>(
                    (StructWriter<?>) writer, pathTransformFunc);
              } else {
                throw new UnsupportedOperationException(
                    "Cannot wrap writer for position deletes: " + writer.getClass());
              }
            });

      } else {
        appenderBuilder.schema(DeleteSchemaUtil.pathPosSchema());

        // We ignore the 'createWriterFunc' and 'rowSchema' even if is provided, since we do not
        // write row data itself
        appenderBuilder.createWriterFunc(
            parquetSchema ->
                new PositionDeleteStructWriter<T>(
                    (StructWriter<?>) GenericParquetWriter.buildWriter(parquetSchema),
                    Function.identity()));
      }

      appenderBuilder.createContextFunc(WriteBuilder.Context::deleteContext);

      return new PositionDeleteWriter<>(
          appenderBuilder.build(), FileFormat.PARQUET, location, spec, partition, keyMetadata);
    }
  }

  private static class ParquetWriteBuilder<T>
      extends ParquetWriter.Builder<T, ParquetWriteBuilder<T>> {
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
    private final Map<String, String> properties = Maps.newHashMap();
    private Long start = null;
    private Long length = null;
    private Schema schema = null;
    private Expression filter = null;
    private ReadSupport<?> readSupport = null;
    private Function<MessageType, VectorizedReader<?>> batchedReaderFunc = null;
    private Function<MessageType, ParquetValueReader<?>> readerFunc = null;
    private boolean filterRecords = true;
    private boolean caseSensitive = true;
    private boolean callInit = false;
    private boolean reuseContainers = false;
    private int maxRecordsPerBatch = 10000;
    private NameMapping nameMapping = null;
    private ByteBuffer fileEncryptionKey = null;
    private ByteBuffer fileAADPrefix = null;

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

    public ReadBuilder createReaderFunc(
        Function<MessageType, ParquetValueReader<?>> newReaderFunction) {
      Preconditions.checkArgument(
          this.batchedReaderFunc == null,
          "Reader function cannot be set since the batched version is already set");
      this.readerFunc = newReaderFunction;
      return this;
    }

    public ReadBuilder createBatchedReaderFunc(Function<MessageType, VectorizedReader<?>> func) {
      Preconditions.checkArgument(
          this.readerFunc == null,
          "Batched reader function cannot be set since the non-batched version is already set");
      this.batchedReaderFunc = func;
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

    public ReadBuilder recordsPerBatch(int numRowsPerBatch) {
      this.maxRecordsPerBatch = numRowsPerBatch;
      return this;
    }

    public ReadBuilder withNameMapping(NameMapping newNameMapping) {
      this.nameMapping = newNameMapping;
      return this;
    }

    public ReadBuilder withFileEncryptionKey(ByteBuffer encryptionKey) {
      this.fileEncryptionKey = encryptionKey;
      return this;
    }

    public ReadBuilder withAADPrefix(ByteBuffer aadPrefix) {
      this.fileAADPrefix = aadPrefix;
      return this;
    }

    @SuppressWarnings({"unchecked", "checkstyle:CyclomaticComplexity"})
    public <D> CloseableIterable<D> build() {
      FileDecryptionProperties fileDecryptionProperties = null;
      if (fileEncryptionKey != null) {
        byte[] encryptionKeyArray = ByteBuffers.toByteArray(fileEncryptionKey);
        byte[] aadPrefixArray = ByteBuffers.toByteArray(fileAADPrefix);
        fileDecryptionProperties =
            FileDecryptionProperties.builder()
                .withFooterKey(encryptionKeyArray)
                .withAADPrefix(aadPrefixArray)
                .build();
      } else {
        Preconditions.checkState(fileAADPrefix == null, "AAD prefix set with null encryption key");
      }

      if (readerFunc != null || batchedReaderFunc != null) {
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

        if (fileDecryptionProperties != null) {
          optionsBuilder.withDecryption(fileDecryptionProperties);
        }

        ParquetReadOptions options = optionsBuilder.build();

        if (batchedReaderFunc != null) {
          return new VectorizedParquetReader<>(
              file,
              schema,
              options,
              batchedReaderFunc,
              nameMapping,
              filter,
              reuseContainers,
              caseSensitive,
              maxRecordsPerBatch);
        } else {
          return new org.apache.iceberg.parquet.ParquetReader<>(
              file,
              schema,
              options,
              readerFunc,
              nameMapping,
              filter,
              reuseContainers,
              caseSensitive);
        }
      }

      ParquetReadBuilder<D> builder = new ParquetReadBuilder<>(ParquetIO.file(file));

      builder.project(schema);

      if (readSupport != null) {
        builder.readSupport((ReadSupport<D>) readSupport);
      } else {
        builder.readSupport(new AvroReadSupport<>(ParquetAvro.DEFAULT_MODEL));
      }

      // default options for readers
      builder
          .set("parquet.strict.typing", "false") // allow type promotion
          .set("parquet.avro.compatible", "false") // use the new RecordReader with Utf8 support
          .set(
              "parquet.avro.add-list-element-records",
              "false"); // assume that lists use a 3-level schema

      for (Map.Entry<String, String> entry : properties.entrySet()) {
        builder.set(entry.getKey(), entry.getValue());
      }

      if (filter != null) {
        // TODO: should not need to get the schema to push down before opening the file.
        // Parquet should allow setting a filter inside its read support
        ParquetReadOptions decryptOptions =
            ParquetReadOptions.builder().withDecryption(fileDecryptionProperties).build();
        MessageType type;
        try (ParquetFileReader schemaReader =
            ParquetFileReader.open(ParquetIO.file(file), decryptOptions)) {
          type = schemaReader.getFileMetaData().getSchema();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        Schema fileSchema = ParquetSchemaUtil.convert(type);
        builder
            .useStatsFilter()
            .useDictionaryFilter()
            .useRecordFilter(filterRecords)
            .useBloomFilter()
            .withFilter(ParquetFilters.convert(fileSchema, filter, caseSensitive));
      } else {
        // turn off filtering
        builder
            .useStatsFilter(false)
            .useDictionaryFilter(false)
            .useBloomFilter(false)
            .useRecordFilter(false);
      }

      if (callInit) {
        builder.callInit();
      }

      if (start != null) {
        builder.withFileRange(start, start + length);
      }

      if (nameMapping != null) {
        builder.withNameMapping(nameMapping);
      }

      if (fileDecryptionProperties != null) {
        builder.withDecryption(fileDecryptionProperties);
      }

      return new ParquetIterable<>(builder);
    }
  }

  private static class ParquetReadBuilder<T> extends ParquetReader.Builder<T> {
    private Schema schema = null;
    private ReadSupport<T> readSupport = null;
    private boolean callInit = false;
    private NameMapping nameMapping = null;

    private ParquetReadBuilder(org.apache.parquet.io.InputFile file) {
      super(file);
    }

    public ParquetReadBuilder<T> project(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    public ParquetReadBuilder<T> withNameMapping(NameMapping newNameMapping) {
      this.nameMapping = newNameMapping;
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
      return new ParquetReadSupport<>(schema, readSupport, callInit, nameMapping);
    }
  }

  /**
   * Combines several files into one
   *
   * @param inputFiles an {@link Iterable} of parquet files. The order of iteration determines the
   *     order in which content of files are read and written to the {@code outputFile}
   * @param outputFile the output parquet file containing all the data from {@code inputFiles}
   * @param rowGroupSize the row group size to use when writing the {@code outputFile}
   * @param schema the schema of the data
   * @param metadata extraMetadata to write at the footer of the {@code outputFile}
   */
  public static void concat(
      Iterable<File> inputFiles,
      File outputFile,
      int rowGroupSize,
      Schema schema,
      Map<String, String> metadata)
      throws IOException {
    OutputFile file = Files.localOutput(outputFile);
    ParquetFileWriter writer =
        new ParquetFileWriter(
            ParquetIO.file(file),
            ParquetSchemaUtil.convert(schema, "table"),
            ParquetFileWriter.Mode.CREATE,
            rowGroupSize,
            0);
    writer.start();
    for (File inputFile : inputFiles) {
      writer.appendFile(ParquetIO.file(Files.localInput(inputFile)));
    }
    writer.end(metadata);
  }
}
