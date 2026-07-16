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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_ADAPTIVE_ENABLED;
import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX;
import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_MAX_BYTES;
import static org.apache.iceberg.TableProperties.PARQUET_COLUMN_STATS_ENABLED_PREFIX;
import static org.apache.iceberg.TableProperties.PARQUET_DICT_ENCODING_ENABLED_COLUMN_PREFIX;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.parquet.ParquetWritingTestUtils.createTempFile;
import static org.apache.iceberg.parquet.ParquetWritingTestUtils.write;
import static org.apache.iceberg.relocated.com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.variants.Variant;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestParquet {

  @TempDir private Path temp;

  @Test
  public void testRowGroupSizeConfigurable() throws IOException {
    // Without an explicit writer function doesn't support PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT
    // PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT configs.
    // Even though row group size is 16 bytes, we still have to write 101 records
    // as default PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT is 100.
    File parquetFile = generateFile(null, 101, 4 * Integer.BYTES, null, null).first();

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(localInput(parquetFile)))) {
      assertThat(reader.getRowGroups()).hasSize(2);
    }
  }

  @Test
  public void testRowGroupSizeConfigurableWithWriter() throws IOException {
    // Explicit writer function supports PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT
    // and PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT configs.
    // We should just need to write 5 integers (20 bytes)
    // to create two row groups with row group size configured at 16 bytes.
    File parquetFile =
        generateFile(ParquetAvroWriter::buildWriter, 5, 4 * Integer.BYTES, 1, 2).first();

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(localInput(parquetFile)))) {
      assertThat(reader.getRowGroups()).hasSize(2);
    }
  }

  @Test
  public void testMetricsMissingColumnStatisticsInRowGroups() throws IOException {
    Schema schema = new Schema(optional(1, "stringCol", Types.StringType.get()));

    File file = createTempFile(temp);

    List<GenericData.Record> records = Lists.newArrayListWithCapacity(1);
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());

    GenericData.Record smallRecord = new GenericData.Record(avroSchema);
    smallRecord.put("stringCol", "test");
    records.add(smallRecord);

    GenericData.Record largeRecord = new GenericData.Record(avroSchema);
    largeRecord.put("stringCol", Strings.repeat("a", 2048));
    records.add(largeRecord);

    write(
        file,
        schema,
        ImmutableMap.<String, String>builder()
            .put(PARQUET_ROW_GROUP_SIZE_BYTES, "1")
            .put(PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, "1")
            .put(PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, "1")
            .buildOrThrow(),
        ParquetAvroWriter::buildWriter,
        records.toArray(new GenericData.Record[] {}));

    InputFile inputFile = Files.localInput(file);
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile))) {
      assertThat(reader.getRowGroups()).hasSize(2);
      List<BlockMetaData> blocks = reader.getFooter().getBlocks();
      assertThat(blocks).hasSize(2);

      Statistics<?> smallStatistics = getOnlyElement(blocks.get(0).getColumns()).getStatistics();
      assertThat(smallStatistics.hasNonNullValue()).isTrue();
      assertThat(smallStatistics.getMinBytes()).isEqualTo("test".getBytes(UTF_8));
      assertThat(smallStatistics.getMaxBytes()).isEqualTo("test".getBytes(UTF_8));

      // parquet-mr doesn't write stats larger than the max size rather than truncating
      Statistics<?> largeStatistics = getOnlyElement(blocks.get(1).getColumns()).getStatistics();
      assertThat(largeStatistics.hasNonNullValue()).isFalse();
      assertThat(largeStatistics.getMinBytes()).isNull();
      assertThat(largeStatistics.getMaxBytes()).isNull();
    }

    // Null count, lower and upper bounds should be empty because
    // one of the statistics in row groups is missing
    Metrics metrics = ParquetUtil.fileMetrics(inputFile, MetricsConfig.getDefault());
    assertThat(metrics.nullValueCounts()).isEmpty();
    assertThat(metrics.lowerBounds()).isEmpty();
    assertThat(metrics.upperBounds()).isEmpty();
  }

  @Test
  public void testNumberOfBytesWritten() throws IOException {
    Schema schema = new Schema(optional(1, "intCol", IntegerType.get()));

    // this value was specifically derived to reproduce iss1980
    // record count grow factor is 10000 (hardcoded)
    // total 10 checkSize method calls
    // for the 10th time (the last call of the checkSize method) nextCheckRecordCount == 100100
    // 100099 + 1 >= 100100
    int recordCount = 100099;
    File file = createTempFile(temp);

    List<GenericData.Record> records = Lists.newArrayListWithCapacity(recordCount);
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
    for (int i = 1; i <= recordCount; i++) {
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put("intCol", i);
      records.add(record);
    }

    long actualSize =
        write(
            file,
            schema,
            Collections.emptyMap(),
            ParquetAvroWriter::buildWriter,
            records.toArray(new GenericData.Record[] {}));

    long expectedSize = ParquetIO.file(localInput(file)).getLength();
    assertThat(actualSize).isEqualTo(expectedSize);
  }

  @Test
  public void testTwoLevelList() throws IOException {
    Schema schema =
        new Schema(
            optional(1, "arraybytes", Types.ListType.ofRequired(3, Types.BinaryType.get())),
            optional(2, "topbytes", Types.BinaryType.get()));
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());

    File testFile = new File(temp.toFile(), "test" + System.nanoTime() + ".parquet");

    ParquetWriter<GenericRecord> writer =
        AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(testFile.toPath()))
            .withDataModel(GenericData.get())
            .withSchema(avroSchema)
            .config("parquet.avro.add-list-element-records", "true")
            .config("parquet.avro.write-old-list-structure", "true")
            .build();

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);
    List<ByteBuffer> expectedByteList = Lists.newArrayList();
    byte[] expectedByte = {0x00, 0x01};
    ByteBuffer expectedBinary = ByteBuffer.wrap(expectedByte);
    expectedByteList.add(expectedBinary);
    recordBuilder.set("arraybytes", expectedByteList);
    recordBuilder.set("topbytes", expectedBinary);
    GenericData.Record expectedRecord = recordBuilder.build();

    writer.write(expectedRecord);
    writer.close();

    GenericData.Record recordRead =
        Iterables.getOnlyElement(
            Parquet.read(Files.localInput(testFile)).project(schema).callInit().build());

    assertThat(recordRead.get("arraybytes")).isEqualTo(expectedByteList);
    assertThat(recordRead.get("topbytes")).isEqualTo(expectedBinary);
  }

  @Test
  public void testColumnStatisticsEnabled() throws Exception {
    Schema schema =
        new Schema(
            optional(1, "int_field", IntegerType.get()),
            optional(2, "string_field", Types.StringType.get()));

    File file = createTempFile(temp);

    List<GenericData.Record> records = Lists.newArrayListWithCapacity(5);
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
    for (int i = 1; i <= 5; i++) {
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put("int_field", i);
      record.put("string_field", "test");
      records.add(record);
    }

    write(
        file,
        schema,
        ImmutableMap.<String, String>builder()
            .put(PARQUET_COLUMN_STATS_ENABLED_PREFIX + "int_field", "true")
            .put(PARQUET_COLUMN_STATS_ENABLED_PREFIX + "string_field", "false")
            .buildOrThrow(),
        ParquetAvroWriter::buildWriter,
        records.toArray(new GenericData.Record[] {}));

    InputFile inputFile = Files.localInput(file);

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile))) {
      for (BlockMetaData block : reader.getFooter().getBlocks()) {
        for (ColumnChunkMetaData column : block.getColumns()) {
          boolean emptyStats = column.getStatistics().isEmpty();
          if (column.getPath().toDotString().equals("int_field")) {
            assertThat(emptyStats).as("int_field has statistics").isEqualTo(false);
          } else if (column.getPath().toDotString().equals("string_field")) {
            assertThat(emptyStats).as("string_field has statistics").isEqualTo(true);
          }
        }
      }
    }
  }

  @Test
  public void testGeospatialFooterMetricsSkipParquetBounds() throws IOException {
    Schema binarySchema = new Schema(optional(1, "geom", Types.BinaryType.get()));
    Schema geometrySchema = new Schema(optional(1, "geom", Types.GeometryType.crs84()));
    Schema geographySchema = new Schema(optional(1, "geom", Types.GeographyType.crs84()));

    File file = createTempFile(temp);
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(binarySchema.asStruct());
    GenericData.Record first = new GenericData.Record(avroSchema);
    first.put("geom", ByteBuffer.wrap(new byte[] {0x01, 0x02, 0x03}));
    GenericData.Record second = new GenericData.Record(avroSchema);
    second.put("geom", ByteBuffer.wrap(new byte[] {0x04, 0x05, 0x06}));

    write(
        file, binarySchema, Collections.emptyMap(), ParquetAvroWriter::buildWriter, first, second);

    InputFile inputFile = Files.localInput(file);
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile))) {
      Metrics geometryMetrics =
          ParquetMetrics.metrics(
              geometrySchema,
              reader.getFooter().getFileMetaData().getSchema(),
              MetricsConfig.getDefault(),
              reader.getFooter(),
              Stream.empty());

      Metrics geographyMetrics =
          ParquetMetrics.metrics(
              geographySchema,
              reader.getFooter().getFileMetaData().getSchema(),
              MetricsConfig.getDefault(),
              reader.getFooter(),
              Stream.empty());

      assertThat(geometryMetrics.valueCounts()).containsEntry(1, 2L);
      assertThat(geometryMetrics.nullValueCounts()).containsEntry(1, 0L);
      assertThat(geometryMetrics.lowerBounds()).doesNotContainKey(1);
      assertThat(geometryMetrics.upperBounds()).doesNotContainKey(1);

      assertThat(geographyMetrics.valueCounts()).containsEntry(1, 2L);
      assertThat(geographyMetrics.nullValueCounts()).containsEntry(1, 0L);
      assertThat(geographyMetrics.lowerBounds()).doesNotContainKey(1);
      assertThat(geographyMetrics.upperBounds()).doesNotContainKey(1);
    }
  }

  @Test
  public void testPerColumnDictionaryEncoding() throws Exception {
    Schema schema =
        new Schema(
            optional(1, "category", Types.StringType.get()),
            optional(2, "region", Types.StringType.get()));

    File file = createTempFile(temp);

    List<GenericData.Record> records = Lists.newArrayListWithCapacity(100);
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
    for (int i = 0; i < 100; i++) {
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put("category", "cat_" + (i % 5));
      record.put("region", "region_" + (i % 3));
      records.add(record);
    }

    write(
        file,
        schema,
        ImmutableMap.<String, String>builder()
            .put(PARQUET_DICT_ENCODING_ENABLED_COLUMN_PREFIX + "category", "false")
            .buildOrThrow(),
        ParquetAvroWriter::buildWriter,
        records.toArray(new GenericData.Record[] {}));

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file)))) {
      for (BlockMetaData block : reader.getFooter().getBlocks()) {
        for (ColumnChunkMetaData column : block.getColumns()) {
          boolean usesDictionary =
              column.getEncodings().stream().anyMatch(Encoding::usesDictionary);
          if (column.getPath().toDotString().equals("category")) {
            assertThat(usesDictionary).as("category dictionary disabled").isFalse();
          } else if (column.getPath().toDotString().equals("region")) {
            assertThat(usesDictionary).as("region uses global default").isTrue();
          }
        }
      }
    }
  }

  @Test
  public void testPerColumnDictionaryEncodingNestedField() throws Exception {
    // Iceberg names list elements as "<list>.element", but Parquet's 3-level encoding
    // names the same column "<list>.list.element". The per-column setter must accept the
    // Iceberg name and resolve it to the Parquet path internally.
    Schema schema =
        new Schema(
            optional(1, "id", Types.IntegerType.get()),
            optional(2, "tags", Types.ListType.ofRequired(3, Types.StringType.get())));

    File file = createTempFile(temp);

    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
    try (FileAppender<GenericData.Record> writer =
        Parquet.write(Files.localOutput(file))
            .schema(schema)
            .withDictionaryEncoding("tags.element", false)
            .createWriterFunc(ParquetAvroWriter::buildWriter)
            .build()) {
      for (int i = 0; i < 100; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put("id", i % 5);
        record.put("tags", Collections.singletonList("tag_" + (i % 5)));
        writer.add(record);
      }
    }

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file)))) {
      for (BlockMetaData block : reader.getFooter().getBlocks()) {
        for (ColumnChunkMetaData column : block.getColumns()) {
          boolean usesDictionary =
              column.getEncodings().stream().anyMatch(Encoding::usesDictionary);
          if (column.getPath().toDotString().equals("tags.list.element")) {
            assertThat(usesDictionary).as("tags element dictionary disabled").isFalse();
          } else if (column.getPath().toDotString().equals("id")) {
            assertThat(usesDictionary).as("id uses global default").isTrue();
          }
        }
      }
    }
  }

  @Test
  public void testPerColumnDictionaryEncodingViaBuilder() throws Exception {
    Schema schema =
        new Schema(
            optional(1, "category", Types.StringType.get()),
            optional(2, "region", Types.StringType.get()));

    File file = createTempFile(temp);

    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
    try (FileAppender<GenericData.Record> writer =
        Parquet.write(Files.localOutput(file))
            .schema(schema)
            .withDictionaryEncoding("category", false)
            .createWriterFunc(ParquetAvroWriter::buildWriter)
            .build()) {
      for (int i = 0; i < 100; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put("category", "cat_" + (i % 5));
        record.put("region", "region_" + (i % 3));
        writer.add(record);
      }
    }

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file)))) {
      for (BlockMetaData block : reader.getFooter().getBlocks()) {
        for (ColumnChunkMetaData column : block.getColumns()) {
          boolean usesDictionary =
              column.getEncodings().stream().anyMatch(Encoding::usesDictionary);
          if (column.getPath().toDotString().equals("category")) {
            assertThat(usesDictionary).as("category dictionary disabled").isFalse();
          } else if (column.getPath().toDotString().equals("region")) {
            assertThat(usesDictionary).as("region uses global default").isTrue();
          }
        }
      }
    }
  }

  @Test
  public void testPerColumnDictionaryEncodingForNonExistentColumnIsIgnored() throws Exception {
    Schema schema =
        new Schema(
            optional(1, "category", Types.StringType.get()),
            optional(2, "region", Types.StringType.get()));

    File file = createTempFile(temp);

    List<GenericData.Record> records = Lists.newArrayListWithCapacity(100);
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
    for (int i = 0; i < 100; i++) {
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put("category", "cat_" + (i % 5));
      record.put("region", "region_" + (i % 3));
      records.add(record);
    }

    write(
        file,
        schema,
        ImmutableMap.<String, String>builder()
            .put(PARQUET_DICT_ENCODING_ENABLED_COLUMN_PREFIX + "non_existent_field", "false")
            .buildOrThrow(),
        ParquetAvroWriter::buildWriter,
        records.toArray(new GenericData.Record[] {}));

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file)))) {
      for (BlockMetaData block : reader.getFooter().getBlocks()) {
        for (ColumnChunkMetaData column : block.getColumns()) {
          boolean usesDictionary =
              column.getEncodings().stream().anyMatch(Encoding::usesDictionary);
          assertThat(usesDictionary)
              .as("column %s uses global default", column.getPath().toDotString())
              .isTrue();
        }
      }
    }
  }

  @Test
  public void testFooterMetricsWithNameMappingForFileWithoutIds() throws IOException {
    Schema schemaWithIds =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

    NameMapping nameMapping = MappingUtil.create(schemaWithIds);

    File file = createTempFile(temp);

    // Write a Parquet file WITHOUT field IDs using plain Avro schema
    org.apache.avro.Schema avroSchemaWithoutIds =
        org.apache.avro.SchemaBuilder.record("test")
            .fields()
            .requiredLong("id")
            .optionalString("data")
            .endRecord();

    try (ParquetWriter<GenericData.Record> writer =
        AvroParquetWriter.<GenericData.Record>builder(ParquetIO.file(Files.localOutput(file)))
            .withDataModel(GenericData.get())
            .withSchema(avroSchemaWithoutIds)
            .build()) {

      GenericData.Record record = new GenericData.Record(avroSchemaWithoutIds);
      record.put("id", 1L);
      record.put("data", "a");
      writer.write(record);
    }

    InputFile inputFile = Files.localInput(file);

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile))) {
      MessageType parquetSchema = reader.getFooter().getFileMetaData().getSchema();
      assertThat(ParquetSchemaUtil.hasIds(parquetSchema)).isFalse();

      Metrics metrics =
          ParquetUtil.footerMetrics(
              reader.getFooter(), Stream.empty(), MetricsConfig.getDefault(), nameMapping);

      // The key assertion: column sizes should be keyed by field IDs from NameMapping
      assertThat(metrics.columnSizes()).containsOnlyKeys(1, 2);
    }
  }

  @Test
  public void testAvroWriterRejectsVariantType() {
    MessageType schema =
        org.apache.parquet.schema.Types.buildMessage()
            .optional(PrimitiveTypeName.INT32)
            .named("id")
            .optionalGroup()
            .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
            .required(PrimitiveTypeName.BINARY)
            .named("metadata")
            .required(PrimitiveTypeName.BINARY)
            .named("value")
            .named("v")
            .named("table");

    assertThatThrownBy(() -> ParquetAvroWriter.buildWriter(schema))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Avro writer does not support variant types");
  }

  @Test
  public void adaptiveBloomFilterSizingShrinksFile() throws IOException {
    // when PARQUET_BLOOM_FILTER_ADAPTIVE_ENABLED is not set (the default), the writer
    // allocates the full PARQUET_BLOOM_FILTER_MAX_BYTES buffer (4 MiB here) regardless
    // of the number of distinct values written
    long sizeWithoutAdaptive = writeWithBloomFilter(null);
    assertThat(sizeWithoutAdaptive)
        .as("non-adaptive file should pad the bloom filter to PARQUET_BLOOM_FILTER_MAX_BYTES")
        .isGreaterThan(3_500_000L);

    // with PARQUET_BLOOM_FILTER_ADAPTIVE_ENABLED, the writer picks the smallest candidate
    // bloom filter that satisfies the actual number of distinct values (5) at the
    // configured FPP
    long sizeWithAdaptive = writeWithBloomFilter(true);
    assertThat(sizeWithAdaptive)
        .as("adaptive file should be at least 2x smaller than the non-adaptive file")
        .isLessThan(sizeWithoutAdaptive / 2);
  }

  private long writeWithBloomFilter(Boolean adaptiveEnabled) throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()));

    ImmutableMap.Builder<String, String> propsBuilder =
        ImmutableMap.<String, String>builder()
            .put(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id", "true")
            .put(PARQUET_BLOOM_FILTER_MAX_BYTES, "4194304");
    if (adaptiveEnabled != null) {
      propsBuilder.put(PARQUET_BLOOM_FILTER_ADAPTIVE_ENABLED, adaptiveEnabled.toString());
    }

    List<GenericData.Record> records = Lists.newArrayListWithCapacity(5);
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
    for (long i = 0; i < 5; i++) {
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put("id", i);
      records.add(record);
    }

    return write(
        createTempFile(temp),
        schema,
        propsBuilder.buildOrThrow(),
        ParquetAvroWriter::buildWriter,
        records.toArray(new GenericData.Record[] {}));
  }

  private Pair<File, Long> generateFile(
      Function<MessageType, ParquetValueWriter<?>> createWriterFunc,
      int desiredRecordCount,
      Integer rowGroupSizeBytes,
      Integer minCheckRecordCount,
      Integer maxCheckRecordCount)
      throws IOException {
    Schema schema = new Schema(optional(1, "intCol", IntegerType.get()));

    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
    if (rowGroupSizeBytes != null) {
      propsBuilder.put(PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(rowGroupSizeBytes));
    }
    if (minCheckRecordCount != null) {
      propsBuilder.put(
          PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, Integer.toString(minCheckRecordCount));
    }
    if (maxCheckRecordCount != null) {
      propsBuilder.put(
          PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, Integer.toString(maxCheckRecordCount));
    }

    List<GenericData.Record> records = Lists.newArrayListWithCapacity(desiredRecordCount);
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
    for (int i = 1; i <= desiredRecordCount; i++) {
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put("intCol", i);
      records.add(record);
    }

    File file = createTempFile(temp);
    long size =
        write(
            file,
            schema,
            propsBuilder.build(),
            createWriterFunc,
            records.toArray(new GenericData.Record[] {}));
    return Pair.of(file, size);
  }

  @Test
  public void timestampNanoFilterRespectsNanoseconds() throws IOException {
    // Predicate pushdown on timestamp_ns must filter at full nanosecond resolution. The five rows
    // differ only by sub-microsecond nanoseconds, so a micros-truncating push down could not
    // separate id 2 (250 ns) from id 3 (750 ns) and would return the wrong rows.
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "ts", Types.TimestampNanoType.withoutZone()));

    Record template = org.apache.iceberg.data.GenericRecord.create(schema);
    List<Record> records =
        Lists.newArrayList(
            template.copy(
                ImmutableMap.of(
                    "id", 1L, "ts", LocalDateTime.parse("2024-01-01T00:00:00.000000000"))),
            template.copy(
                ImmutableMap.of(
                    "id", 2L, "ts", LocalDateTime.parse("2024-01-01T00:00:00.000000250"))),
            template.copy(
                ImmutableMap.of(
                    "id", 3L, "ts", LocalDateTime.parse("2024-01-01T00:00:00.000000750"))),
            template.copy(
                ImmutableMap.of(
                    "id", 4L, "ts", LocalDateTime.parse("2024-01-01T00:00:00.000001500"))),
            template.copy(
                ImmutableMap.of(
                    "id", 5L, "ts", LocalDateTime.parse("2024-01-01T00:00:00.000003000"))));

    File file = writeNanoRecords(schema, records);

    // Boundary at 500 ns: only ids 3 (750 ns), 4 (1500 ns), 5 (3000 ns) qualify.
    List<Long> ids =
        filterIds(schema, file, greaterThanOrEqual("ts", "2024-01-01T00:00:00.000000500"));
    assertThat(ids).containsExactlyInAnyOrder(3L, 4L, 5L);
  }

  @Test
  public void timestamptzNanoFilterAcrossTimezones() throws IOException {
    // Each row is written in a different zone offset; instants are 0/500/750/1500/3000 ns past the
    // same UTC second. id2 lands exactly on the filter boundary but in +05:00, so a strict
    // greaterThan must exclude it by instant, not by wall-clock (its 05:00 vs the boundary's
    // 04:00).
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "ts", Types.TimestampNanoType.withZone()));

    Record template = org.apache.iceberg.data.GenericRecord.create(schema);
    List<Record> records =
        Lists.newArrayList(
            template.copy(
                ImmutableMap.of(
                    "id", 1L, "ts", OffsetDateTime.parse("2024-01-01T00:00:00.000000000+00:00"))),
            template.copy(
                ImmutableMap.of(
                    "id", 2L, "ts", OffsetDateTime.parse("2024-01-01T05:00:00.000000500+05:00"))),
            template.copy(
                ImmutableMap.of(
                    "id", 3L, "ts", OffsetDateTime.parse("2023-12-31T16:00:00.000000750-08:00"))),
            template.copy(
                ImmutableMap.of(
                    "id", 4L, "ts", OffsetDateTime.parse("2024-01-01T05:30:00.000001500+05:30"))),
            template.copy(
                ImmutableMap.of(
                    "id", 5L, "ts", OffsetDateTime.parse("2024-01-01T00:00:00.000003000+00:00"))));

    File file = writeNanoRecords(schema, records);

    // Boundary == 2024-01-01T00:00:00.000000500Z, expressed in +04:00. id2 is that same instant
    // (written in +05:00); a strict greaterThan excludes it, leaving ids 3/4/5.
    List<Long> ids =
        filterIds(schema, file, greaterThan("ts", "2024-01-01T04:00:00.000000500+04:00"));
    assertThat(ids).containsExactlyInAnyOrder(3L, 4L, 5L);
  }

  private File writeNanoRecords(Schema schema, List<Record> records) throws IOException {
    File file = createTempFile(temp);
    try (FileAppender<Record> appender =
        Parquet.write(Files.localOutput(file))
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      for (Record record : records) {
        appender.add(record);
      }
    }

    return file;
  }

  private List<Long> filterIds(Schema schema, File file, Expression filter) throws IOException {
    List<Long> ids = Lists.newArrayList();
    // callInit() drives the parquet-mr ReadSupport path, the only read path that runs
    // ParquetFilters.
    try (CloseableIterable<GenericData.Record> reader =
        Parquet.read(localInput(file)).project(schema).callInit().filter(filter).build()) {
      for (GenericData.Record record : reader) {
        ids.add((Long) record.get("id"));
      }
    }

    return ids;
  }
}
