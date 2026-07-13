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
package org.apache.iceberg.vortex;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.data.vortex.GenericVortexWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@SuppressWarnings("deprecation")
public class TestVortexMetrics {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "name", Types.StringType.get()),
          optional(3, "salary", Types.LongType.get()),
          optional(4, "rating", Types.DoubleType.get()));

  @TempDir private Path temp;

  @Test
  public void testFieldMetricsFromWriter() {
    VortexValueWriter<Record> writer = GenericVortexWriter.buildWriter(SCHEMA);

    addRecord(writer, 1L, "Alice", 1000L, 4.5);
    addRecord(writer, 2L, "Bob", null, Double.NaN);
    addRecord(writer, 3L, "Carol", 3000L, 3.2);

    Map<Integer, FieldMetrics<?>> metricsMap = collectMetricsById(writer);

    // id: required, all non-null
    FieldMetrics<?> idMetrics = metricsMap.get(1);
    assertThat(idMetrics.valueCount()).isEqualTo(3);
    assertThat(idMetrics.nullValueCount()).isEqualTo(0);
    assertThat(idMetrics.lowerBound()).isEqualTo(1L);
    assertThat(idMetrics.upperBound()).isEqualTo(3L);

    // name: optional, all present
    FieldMetrics<?> nameMetrics = metricsMap.get(2);
    assertThat(nameMetrics.valueCount()).isEqualTo(3);
    assertThat(nameMetrics.nullValueCount()).isEqualTo(0);
    assertThat(nameMetrics.lowerBound()).isEqualTo("Alice");
    assertThat(nameMetrics.upperBound()).isEqualTo("Carol");

    // salary: optional, one null
    FieldMetrics<?> salaryMetrics = metricsMap.get(3);
    assertThat(salaryMetrics.valueCount()).isEqualTo(3);
    assertThat(salaryMetrics.nullValueCount()).isEqualTo(1);
    assertThat(salaryMetrics.lowerBound()).isEqualTo(1000L);
    assertThat(salaryMetrics.upperBound()).isEqualTo(3000L);

    // rating: double with NaN
    FieldMetrics<?> ratingMetrics = metricsMap.get(4);
    assertThat(ratingMetrics.valueCount()).isEqualTo(3);
    assertThat(ratingMetrics.nullValueCount()).isEqualTo(0);
    assertThat(ratingMetrics.nanValueCount()).isEqualTo(1);
    assertThat(ratingMetrics.lowerBound()).isEqualTo(3.2);
    assertThat(ratingMetrics.upperBound()).isEqualTo(4.5);
  }

  @Test
  public void testBuildMetricsFullMode() {
    VortexValueWriter<Record> writer = GenericVortexWriter.buildWriter(SCHEMA);

    addRecord(writer, 1L, "Alice", 1000L, 4.5);
    addRecord(writer, 2L, "Bob", null, 3.2);

    Metrics metrics =
        VortexMetrics.buildMetrics(2L, SCHEMA, MetricsConfig.getDefault(), writer.metrics());

    assertThat(metrics.recordCount()).isEqualTo(2L);

    // value counts
    assertThat(metrics.valueCounts()).containsEntry(1, 2L);
    assertThat(metrics.valueCounts()).containsEntry(2, 2L);
    assertThat(metrics.valueCounts()).containsEntry(3, 2L);

    // null counts
    assertThat(metrics.nullValueCounts()).containsEntry(1, 0L);
    assertThat(metrics.nullValueCounts()).containsEntry(3, 1L);

    // bounds are ByteBuffers
    assertThat(
            (Object) Conversions.fromByteBuffer(Types.LongType.get(), metrics.lowerBounds().get(1)))
        .isEqualTo(1L);
    assertThat(
            (Object) Conversions.fromByteBuffer(Types.LongType.get(), metrics.upperBounds().get(1)))
        .isEqualTo(2L);

    assertThat(
            Conversions.fromByteBuffer(Types.StringType.get(), metrics.lowerBounds().get(2))
                .toString())
        .isEqualTo("Alice");
    assertThat(
            Conversions.fromByteBuffer(Types.StringType.get(), metrics.upperBounds().get(2))
                .toString())
        .isEqualTo("Bob");
  }

  @Test
  public void testMetricsCountsMode() {
    MetricsConfig countsConfig =
        MetricsConfig.fromProperties(
            ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "counts"));

    VortexValueWriter<Record> writer = GenericVortexWriter.buildWriter(SCHEMA);
    addRecord(writer, 1L, "Alice", 1000L, 4.5);

    Metrics metrics = VortexMetrics.buildMetrics(1L, SCHEMA, countsConfig, writer.metrics());

    assertThat(metrics.recordCount()).isEqualTo(1L);
    assertThat(metrics.valueCounts()).containsEntry(1, 1L);
    assertThat(metrics.nullValueCounts()).containsEntry(1, 0L);

    // no bounds in counts mode
    assertThat(metrics.lowerBounds()).isNull();
    assertThat(metrics.upperBounds()).isNull();
  }

  @Test
  public void testVariantColumnReportsRowCountWithoutBounds() {
    Schema variantSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "payload", Types.VariantType.get()));
    FieldMetrics<Long> idMetrics = new FieldMetrics<>(1, 3, 0, 10L, 12L);

    Metrics metrics =
        VortexMetrics.buildMetrics(
            3L, variantSchema, MetricsConfig.getDefault(), Stream.of(idMetrics));

    assertThat(metrics.valueCounts()).containsEntry(2, 3L);
    assertThat(metrics.nullValueCounts()).doesNotContainKey(2);
    assertThat(metrics.lowerBounds()).doesNotContainKey(2);
    assertThat(metrics.upperBounds()).doesNotContainKey(2);
  }

  @Test
  public void testMetricsNoneMode() {
    MetricsConfig noneConfig =
        MetricsConfig.fromProperties(
            ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none"));

    VortexValueWriter<Record> writer = GenericVortexWriter.buildWriter(SCHEMA);
    addRecord(writer, 1L, "Alice", 1000L, 4.5);

    Metrics metrics = VortexMetrics.buildMetrics(1L, SCHEMA, noneConfig, writer.metrics());

    assertThat(metrics.recordCount()).isEqualTo(1L);
    assertThat(metrics.valueCounts()).isEmpty();
    assertThat(metrics.nullValueCounts()).isEmpty();
    assertThat(metrics.lowerBounds()).isNull();
    assertThat(metrics.upperBounds()).isNull();
  }

  @Test
  public void testMetricsTruncateMode() {
    MetricsConfig truncateConfig =
        MetricsConfig.fromProperties(
            ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "truncate(2)"));

    Schema stringSchema = new Schema(required(1, "name", Types.StringType.get()));

    FieldMetrics<String> fieldMetrics = new FieldMetrics<>(1, 1, 0, "abcdef", "abcdef");

    Metrics metrics =
        VortexMetrics.buildMetrics(1L, stringSchema, truncateConfig, Stream.of(fieldMetrics));

    // lower bound should be truncated to "ab"
    ByteBuffer lowerBound = metrics.lowerBounds().get(1);
    assertThat(Conversions.fromByteBuffer(Types.StringType.get(), lowerBound).toString())
        .isEqualTo("ab");

    // upper bound should be truncated to "ac" (truncateStringMax increments last char)
    ByteBuffer upperBound = metrics.upperBounds().get(1);
    assertThat(Conversions.fromByteBuffer(Types.StringType.get(), upperBound).toString())
        .isEqualTo("ac");
  }

  @Test
  public void testAllNullColumn() {
    VortexValueWriter<Record> writer = GenericVortexWriter.buildWriter(SCHEMA);
    addRecord(writer, 1L, null, null, null);
    addRecord(writer, 2L, null, null, null);

    Map<Integer, FieldMetrics<?>> metricsMap = collectMetricsById(writer);

    FieldMetrics<?> nameMetrics = metricsMap.get(2);
    assertThat(nameMetrics.valueCount()).isEqualTo(2);
    assertThat(nameMetrics.nullValueCount()).isEqualTo(2);
    assertThat(nameMetrics.hasBounds()).isFalse();
  }

  @Test
  public void testAllNaNDoubleColumn() {
    Schema doubleSchema = new Schema(required(1, "val", Types.DoubleType.get()));
    VortexValueWriter<Record> writer = GenericVortexWriter.buildWriter(doubleSchema);

    GenericRecord record = GenericRecord.create(doubleSchema);
    record.set(0, Double.NaN);
    // write directly through tracker, not through VectorSchemaRoot
    // Just test FieldMetrics directly
    FieldMetrics<Double> fieldMetrics = new FieldMetrics<>(1, 2, 0, 2L, null, null);

    Metrics metrics =
        VortexMetrics.buildMetrics(
            2L, doubleSchema, MetricsConfig.getDefault(), Stream.of(fieldMetrics));

    assertThat(metrics.nanValueCounts()).containsEntry(1, 2L);
    assertThat(metrics.lowerBounds()).isNull();
    assertThat(metrics.upperBounds()).isNull();
  }

  @Test
  void metricsFromArrowBatchIncludeNestedFields() {
    Schema schema =
        new Schema(
            optional(
                1, "nested", Types.StructType.of(optional(2, "value", Types.IntegerType.get()))),
            optional(3, "data", Types.BinaryType.get()),
            optional(4, "score", Types.FloatType.get()));
    VortexValueWriter<Record> writer = GenericVortexWriter.buildWriter(schema);
    VortexMetrics.Accumulator accumulator =
        new VortexMetrics.Accumulator(schema, MetricsConfig.getDefault());

    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root =
            VectorSchemaRoot.create(VortexSchemas.toArrowSchema(schema), allocator)) {
      root.allocateNew();

      Record nested = GenericRecord.create(schema.findType("nested").asStructType());
      nested.setField("value", 10);
      Record first = GenericRecord.create(schema);
      first.setField("nested", nested);
      first.setField("data", ByteBuffer.wrap(new byte[] {(byte) 0x80}));
      first.setField("score", Float.NaN);
      writer.write(first, root, 0);

      Record second = GenericRecord.create(schema);
      second.setField("nested", null);
      second.setField("data", ByteBuffer.wrap(new byte[] {0x7F}));
      second.setField("score", 2.5F);
      writer.write(second, root, 1);

      root.setRowCount(2);
      accumulator.add(root, 2);
    }

    Metrics metrics = accumulator.metrics(2);
    assertThat(metrics.valueCounts()).containsEntry(2, 2L);
    assertThat(metrics.nullValueCounts()).containsEntry(2, 1L);
    assertThat(metrics.valueCounts()).doesNotContainKey(1);

    assertThat(metrics.nanValueCounts()).containsEntry(4, 1L);
    assertThat(
            Conversions.<Float>fromByteBuffer(Types.FloatType.get(), metrics.lowerBounds().get(4)))
        .isEqualTo(2.5F);
    assertThat(
            Conversions.<Float>fromByteBuffer(Types.FloatType.get(), metrics.upperBounds().get(4)))
        .isEqualTo(2.5F);

    assertThat(metrics.lowerBounds().get(3)).isEqualTo(ByteBuffer.wrap(new byte[] {0x7F}));
    assertThat(metrics.upperBounds().get(3)).isEqualTo(ByteBuffer.wrap(new byte[] {(byte) 0x80}));
  }

  @Test
  void metricsFromArrowBatchForAllPrimitiveTypes() {
    Schema schema =
        new Schema(
            optional(1, "boolean", Types.BooleanType.get()),
            optional(2, "integer", Types.IntegerType.get()),
            optional(3, "long", Types.LongType.get()),
            optional(4, "float", Types.FloatType.get()),
            optional(5, "double", Types.DoubleType.get()),
            optional(6, "string", Types.StringType.get()),
            optional(7, "binary", Types.BinaryType.get()),
            optional(8, "decimal", Types.DecimalType.of(9, 2)),
            optional(9, "date", Types.DateType.get()),
            optional(10, "time", Types.TimeType.get()),
            optional(11, "timestamp", Types.TimestampType.withoutZone()),
            optional(12, "timestamp_tz", Types.TimestampType.withZone()),
            optional(13, "timestamp_ns", Types.TimestampNanoType.withoutZone()),
            optional(14, "timestamp_ns_tz", Types.TimestampNanoType.withZone()),
            optional(15, "uuid", Types.UUIDType.get()));
    VortexValueWriter<Record> writer = GenericVortexWriter.buildWriter(schema);
    VortexMetrics.Accumulator accumulator =
        new VortexMetrics.Accumulator(schema, MetricsConfig.getDefault());
    Record record = GenericRecord.create(schema);
    record.setField("boolean", true);
    record.setField("integer", 34);
    record.setField("long", 35L);
    record.setField("float", 1.25F);
    record.setField("double", 2.5D);
    record.setField("string", "abc");
    record.setField("binary", ByteBuffer.wrap(new byte[] {1, 2, 3}));
    record.setField("decimal", new BigDecimal("12.34"));
    record.setField("date", LocalDate.of(2026, 7, 12));
    record.setField("time", LocalTime.of(12, 34, 56));
    record.setField("timestamp", LocalDateTime.of(2026, 7, 12, 12, 34, 56));
    record.setField("timestamp_tz", OffsetDateTime.of(2026, 7, 12, 12, 34, 56, 0, ZoneOffset.UTC));
    record.setField("timestamp_ns", LocalDateTime.of(2026, 7, 12, 12, 34, 56, 123));
    record.setField(
        "timestamp_ns_tz", OffsetDateTime.of(2026, 7, 12, 12, 34, 56, 123, ZoneOffset.UTC));
    record.setField("uuid", UUID.fromString("123e4567-e89b-12d3-a456-426614174000"));

    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root =
            VectorSchemaRoot.create(VortexSchemas.toArrowSchema(schema), allocator)) {
      root.allocateNew();
      writer.write(record, root, 0);
      root.setRowCount(1);
      accumulator.add(root, 1);
    }

    Metrics metrics = accumulator.metrics(1);
    assertThat(metrics.valueCounts())
        .hasSize(15)
        .allSatisfy((id, count) -> assertThat(count).isOne());
    assertThat(metrics.nullValueCounts())
        .hasSize(15)
        .allSatisfy((id, count) -> assertThat(count).isZero());
    assertThat(metrics.lowerBounds()).hasSize(15);
    assertThat(metrics.upperBounds()).hasSize(15);
    assertThat(metrics.nanValueCounts()).containsEntry(4, 0L).containsEntry(5, 0L);
  }

  @Test
  void metricsFromArrowBatchForVariant() {
    Schema schema = new Schema(optional(1, "variant", Types.VariantType.get()));
    VortexValueWriter<Record> writer = GenericVortexWriter.buildWriter(schema);
    VortexMetrics.Accumulator accumulator =
        new VortexMetrics.Accumulator(schema, MetricsConfig.getDefault());
    Record first = GenericRecord.create(schema);
    first.setField("variant", Variant.of(VariantMetadata.empty(), Variants.of("abc")));
    Record second = GenericRecord.create(schema);
    second.setField("variant", null);

    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root =
            VectorSchemaRoot.create(VortexSchemas.toArrowSchema(schema), allocator)) {
      root.allocateNew();
      writer.write(first, root, 0);
      writer.write(second, root, 1);
      root.setRowCount(2);
      accumulator.add(root, 2);
    }

    Metrics metrics = accumulator.metrics(2);
    assertThat(metrics.valueCounts()).containsEntry(1, 2L);
    assertThat(metrics.nullValueCounts()).containsEntry(1, 1L);
    assertThat(metrics.lowerBounds()).isNull();
    assertThat(metrics.upperBounds()).isNull();
  }

  @Test
  void appenderCollectsMetricsIndependentlyOfValueWriter() throws Exception {
    Schema schema = new Schema(optional(1, "value", Types.IntegerType.get()));
    VortexValueWriter<Record> delegate = GenericVortexWriter.buildWriter(schema);
    VortexValueWriter<Record> writerWithoutMetrics = delegate::write;
    VortexFormatModel<Record, Void, VortexRowReader<?>> model =
        VortexFormatModel.create(
            Record.class,
            Void.class,
            (icebergSchema, fileSchema, engineSchema) -> writerWithoutMetrics,
            (VortexFormatModel.ReaderFunction<Record>) GenericVortexReader::buildReader);
    OutputFile outputFile = Files.localOutput(temp.resolve("metrics.vortex").toFile());

    FileAppender<Record> appender =
        model
            .writeBuilder(EncryptedFiles.plainAsEncryptedOutput(outputFile))
            .schema(schema)
            .content(FileContent.DATA)
            .build();
    Record first = GenericRecord.create(schema);
    first.setField("value", 10);
    appender.add(first);
    Record second = GenericRecord.create(schema);
    second.setField("value", null);
    appender.add(second);
    appender.close();

    assertThat(writerWithoutMetrics.metrics()).isEmpty();
    assertThat(appender.metrics().valueCounts()).containsEntry(1, 2L);
    assertThat(appender.metrics().nullValueCounts()).containsEntry(1, 1L);
    assertThat(
            Conversions.<Integer>fromByteBuffer(
                Types.IntegerType.get(), appender.metrics().lowerBounds().get(1)))
        .isEqualTo(10);
    assertThat(
            Conversions.<Integer>fromByteBuffer(
                Types.IntegerType.get(), appender.metrics().upperBounds().get(1)))
        .isEqualTo(10);
  }

  @Test
  void positionDeleteAppenderMetrics() throws Exception {
    OutputFile outputFile = Files.localOutput(temp.resolve("position-deletes.vortex").toFile());
    VortexFormatModel<PositionDelete<Void>, Void, VortexRowReader<?>> model =
        VortexFormatModel.forPositionDeletes();
    FileAppender<PositionDelete<Void>> appender =
        model
            .writeBuilder(EncryptedFiles.plainAsEncryptedOutput(outputFile))
            .content(FileContent.POSITION_DELETES)
            .build();

    PositionDelete<Void> delete = PositionDelete.create();
    appender.add(delete.set("file-a.parquet", 1L, null));
    appender.add(delete.set("file-a.parquet", 3L, null));
    appender.close();

    Metrics metrics = appender.metrics();
    int pathId = MetadataColumns.DELETE_FILE_PATH.fieldId();
    int positionId = MetadataColumns.DELETE_FILE_POS.fieldId();
    assertThat(metrics.valueCounts()).containsEntry(pathId, 2L).containsEntry(positionId, 2L);
    assertThat(metrics.nullValueCounts()).containsEntry(pathId, 0L).containsEntry(positionId, 0L);
    assertThat(
            Conversions.fromByteBuffer(Types.StringType.get(), metrics.lowerBounds().get(pathId))
                .toString())
        .isEqualTo("file-a.parquet");
    assertThat(
            Conversions.<Long>fromByteBuffer(
                Types.LongType.get(), metrics.lowerBounds().get(positionId)))
        .isEqualTo(1L);
    assertThat(
            Conversions.<Long>fromByteBuffer(
                Types.LongType.get(), metrics.upperBounds().get(positionId)))
        .isEqualTo(3L);
  }

  @Test
  void positionDeleteFilePreservesBounds() throws Exception {
    OutputFile outputFile = Files.localOutput(temp.resolve("position-delete-file.vortex").toFile());
    PositionDeleteWriter<Void> writer =
        FormatModelRegistry.<Void>positionDeleteWriteBuilder(
                FileFormat.VORTEX, EncryptedFiles.plainAsEncryptedOutput(outputFile))
            .spec(PartitionSpec.unpartitioned())
            .build();
    PositionDelete<Void> delete = PositionDelete.create();
    writer.write(delete.set("file-a.parquet", 1L, null));
    writer.write(delete.set("file-a.parquet", 3L, null));
    writer.close();

    DeleteFile deleteFile = writer.toDeleteFile();
    int pathId = MetadataColumns.DELETE_FILE_PATH.fieldId();
    int positionId = MetadataColumns.DELETE_FILE_POS.fieldId();
    assertThat(deleteFile.valueCounts()).isNull();
    assertThat(deleteFile.nullValueCounts()).isNull();
    assertThat(deleteFile.lowerBounds()).containsKeys(pathId, positionId);
    assertThat(deleteFile.upperBounds()).containsKeys(pathId, positionId);
    assertThat(
            Conversions.<Long>fromByteBuffer(
                Types.LongType.get(), deleteFile.lowerBounds().get(positionId)))
        .isEqualTo(1L);
    assertThat(
            Conversions.<Long>fromByteBuffer(
                Types.LongType.get(), deleteFile.upperBounds().get(positionId)))
        .isEqualTo(3L);
  }

  private static void addRecord(
      VortexValueWriter<Record> writer, Long id, String name, Long salary, Double rating) {
    // We can't write through VectorSchemaRoot in unit tests (needs Arrow allocation),
    // but GenericVortexWriter tracks metrics before writing to vectors.
    // Use reflection-free approach: call the writer's metrics tracking directly.
    // Actually, the GenericVortexWriter.write() will throw on null root vectors,
    // so let's use the tracker-level testing approach via metrics() directly.
    // For integration testing we'd need the full Arrow stack.
    //
    // Instead, let's test the tracker directly through GenericVortexWriter internals.
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.set(0, id);
    record.set(1, name);
    record.set(2, salary);
    record.set(3, rating);

    // We need to create an actual VectorSchemaRoot for the write call.
    // Use a shared helper that creates an Arrow root.
    try (org.apache.arrow.memory.BufferAllocator allocator =
            new org.apache.arrow.memory.RootAllocator();
        org.apache.arrow.vector.VectorSchemaRoot root =
            org.apache.arrow.vector.VectorSchemaRoot.create(
                VortexSchemas.toArrowSchema(SCHEMA), allocator)) {
      root.allocateNew();
      writer.write(record, root, 0);
    }
  }

  private static Map<Integer, FieldMetrics<?>> collectMetricsById(
      VortexValueWriter<Record> writer) {
    Map<Integer, FieldMetrics<?>> map = new java.util.HashMap<>();
    writer.metrics().forEach(m -> map.put(m.id(), m));
    return map;
  }
}
