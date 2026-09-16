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
import java.util.UUID;
import org.apache.iceberg.DeleteFile;
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
import org.apache.iceberg.util.ContentFileUtil;
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
  void summaryMetricsInCountsMode() throws Exception {
    MetricsConfig countsConfig =
        MetricsConfig.fromProperties(
            ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "counts"));
    Schema schema = new Schema(optional(1, "name", Types.StringType.get()));
    FileAppender<Record> appender = buildAppender(schema, "counts.vortex", countsConfig);
    appender.add(stringRecord(schema, "Alice"));
    appender.close();

    Metrics metrics = appender.metrics();
    assertThat(metrics.valueCounts()).containsEntry(1, 1L);
    assertThat(metrics.nullValueCounts()).containsEntry(1, 0L);
    assertThat(metrics.lowerBounds()).isNull();
    assertThat(metrics.upperBounds()).isNull();
  }

  @Test
  void summaryMetricsInNoneMode() throws Exception {
    MetricsConfig noneConfig =
        MetricsConfig.fromProperties(
            ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none"));
    Schema schema = new Schema(optional(1, "name", Types.StringType.get()));
    FileAppender<Record> appender = buildAppender(schema, "none.vortex", noneConfig);
    appender.add(stringRecord(schema, "Alice"));
    appender.close();

    Metrics metrics = appender.metrics();
    assertThat(metrics.recordCount()).isEqualTo(1L);
    assertThat(metrics.valueCounts()).isEmpty();
    assertThat(metrics.nullValueCounts()).isEmpty();
    assertThat(metrics.lowerBounds()).isNull();
    assertThat(metrics.upperBounds()).isNull();
  }

  @Test
  void summaryMetricsRespectTruncateMode() throws Exception {
    // Iceberg truncation applies on top of whatever bounds Vortex reports.
    MetricsConfig truncateConfig =
        MetricsConfig.fromProperties(
            ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "truncate(3)"));
    Schema schema = new Schema(optional(1, "name", Types.StringType.get()));
    FileAppender<Record> appender = buildAppender(schema, "truncate.vortex", truncateConfig);
    appender.add(stringRecord(schema, "abcdef"));
    appender.close();

    Metrics metrics = appender.metrics();
    assertThat(
            Conversions.fromByteBuffer(Types.StringType.get(), metrics.lowerBounds().get(1))
                .toString())
        .isEqualTo("abc");
    assertThat(
            Conversions.fromByteBuffer(Types.StringType.get(), metrics.upperBounds().get(1))
                .toString())
        .isEqualTo("abd");
  }

  private static Record stringRecord(Schema schema, String value) {
    Record record = GenericRecord.create(schema);
    record.setField("name", value);
    return record;
  }

  @Test
  void summaryMetricsCoverTopLevelColumns() throws Exception {
    Schema schema =
        new Schema(
            optional(
                1, "nested", Types.StructType.of(optional(2, "value", Types.IntegerType.get()))),
            optional(3, "data", Types.BinaryType.get()),
            optional(4, "score", Types.FloatType.get()));
    FileAppender<Record> appender = buildAppender(schema, "nested.vortex");

    Record nested = GenericRecord.create(schema.findType("nested").asStructType());
    nested.setField("value", 10);
    Record first = GenericRecord.create(schema);
    first.setField("nested", nested);
    first.setField("data", ByteBuffer.wrap(new byte[] {(byte) 0x80}));
    first.setField("score", Float.NaN);
    appender.add(first);

    Record second = GenericRecord.create(schema);
    second.setField("nested", null);
    second.setField("data", ByteBuffer.wrap(new byte[] {0x7F}));
    second.setField("score", 2.5F);
    appender.add(second);
    appender.close();

    Metrics metrics = appender.metrics();
    assertThat(metrics.recordCount()).isEqualTo(2L);

    // Vortex reports statistics per top-level column, so struct columns and their nested fields
    // carry no counts or bounds.
    assertThat(metrics.valueCounts()).doesNotContainKey(1).doesNotContainKey(2);
    assertThat(metrics.lowerBounds()).doesNotContainKeys(1, 2);

    // Column sizes are physical and cover every top-level column, including structs.
    assertThat(metrics.columnSizes()).containsKeys(1, 3, 4);

    assertThat(metrics.valueCounts()).containsEntry(3, 2L).containsEntry(4, 2L);
    assertThat(metrics.nullValueCounts()).containsEntry(3, 0L).containsEntry(4, 0L);
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
  void summaryMetricsForAllPrimitiveTypes() throws Exception {
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

    FileAppender<Record> appender = buildAppender(schema, "all-types.vortex");
    appender.add(record);
    appender.close();

    Metrics metrics = appender.metrics();
    assertThat(metrics.recordCount()).isEqualTo(1L);
    assertThat(metrics.columnSizes()).hasSize(15);
    assertThat(metrics.valueCounts())
        .hasSize(15)
        .allSatisfy((id, count) -> assertThat(count).isOne());
    assertThat(metrics.nullValueCounts())
        .hasSize(15)
        .allSatisfy((id, count) -> assertThat(count).isZero());
    // Vortex does not compute min/max for the arrow.uuid extension dtype, so the UUID column (15)
    // has counts but no bounds.
    assertThat(metrics.lowerBounds()).hasSize(14).doesNotContainKey(15);
    assertThat(metrics.upperBounds()).hasSize(14).doesNotContainKey(15);
    assertThat(metrics.nanValueCounts()).containsEntry(4, 0L).containsEntry(5, 0L);

    assertThat(
            Conversions.<Integer>fromByteBuffer(Types.DateType.get(), metrics.lowerBounds().get(9)))
        .isEqualTo((int) LocalDate.of(2026, 7, 12).toEpochDay());
    assertThat(
            Conversions.<BigDecimal>fromByteBuffer(
                Types.DecimalType.of(9, 2), metrics.lowerBounds().get(8)))
        .isEqualTo(new BigDecimal("12.34"));
    assertThat(
            Conversions.fromByteBuffer(Types.StringType.get(), metrics.lowerBounds().get(6))
                .toString())
        .isEqualTo("abc");
  }

  @Test
  void summaryMetricsForVariant() throws Exception {
    Schema schema = new Schema(optional(1, "variant", Types.VariantType.get()));
    Record first = GenericRecord.create(schema);
    first.setField("variant", Variant.of(VariantMetadata.empty(), Variants.of("abc")));
    Record second = GenericRecord.create(schema);
    second.setField("variant", null);

    FileAppender<Record> appender = buildAppender(schema, "variant.vortex");
    appender.add(first);
    appender.add(second);
    appender.close();

    Metrics metrics = appender.metrics();
    assertThat(metrics.recordCount()).isEqualTo(2L);
    assertThat(metrics.valueCounts()).containsEntry(1, 2L);
    assertThat(metrics.lowerBounds()).isNull();
    assertThat(metrics.upperBounds()).isNull();
  }

  @Test
  void summaryMetricsForMapReportCountsWithoutBounds() throws Exception {
    // Vortex only computes statistics for top-level columns, so a map column carries no bounds.
    Schema schema =
        new Schema(
            optional(
                1,
                "props",
                Types.MapType.ofOptional(2, 3, Types.StringType.get(), Types.IntegerType.get())));

    Record first = GenericRecord.create(schema);
    first.setField("props", ImmutableMap.of("a", 1, "b", 2));
    Record second = GenericRecord.create(schema);
    second.setField("props", null);

    FileAppender<Record> appender = buildAppender(schema, "map.vortex");
    appender.add(first);
    appender.add(second);
    appender.close();

    Metrics metrics = appender.metrics();
    assertThat(metrics.recordCount()).isEqualTo(2L);
    assertThat(metrics.columnSizes()).containsKey(1);
    assertThat(metrics.lowerBounds()).isNull();
    assertThat(metrics.upperBounds()).isNull();
  }

  private FileAppender<Record> buildAppender(Schema schema, String fileName) throws Exception {
    return buildAppender(schema, fileName, MetricsConfig.getDefault());
  }

  private FileAppender<Record> buildAppender(
      Schema schema, String fileName, MetricsConfig metricsConfig) throws Exception {
    VortexFormatModel<Record, Void, VortexRowReader<?>> model =
        VortexFormatModel.create(
            Record.class,
            Void.class,
            (icebergSchema, fileSchema, engineSchema) ->
                GenericVortexWriter.buildWriter(icebergSchema),
            (VortexFormatModel.ReaderFunction<Record>) GenericVortexReader::buildReader);
    OutputFile outputFile = Files.localOutput(temp.resolve(fileName).toFile());
    return model
        .writeBuilder(EncryptedFiles.plainAsEncryptedOutput(outputFile))
        .schema(schema)
        .metricsConfig(metricsConfig)
        .content(FileContent.DATA)
        .build();
  }

  @Test
  void appenderCollectsMetricsFromTheWrittenFile() throws Exception {
    Schema schema = new Schema(optional(1, "value", Types.IntegerType.get()));
    FileAppender<Record> appender = buildAppender(schema, "metrics.vortex");
    Record first = GenericRecord.create(schema);
    first.setField("value", 10);
    appender.add(first);
    Record second = GenericRecord.create(schema);
    second.setField("value", null);
    appender.add(second);
    appender.close();

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
  void positionDeleteFileIsFileScopedForLongPaths() throws Exception {
    // Iceberg infers that a delete file covers a single data file from an equal lower and upper
    // bound on file_path, and only rewrites deletes it can attribute to one data file. Vortex's
    // native string statistics report a truncated prefix range, so a realistic path would compare
    // unequal and the delete file would read as partition scoped.
    String longPath =
        "/warehouse/default/table/data/00000-0-abcdef01-2345-6789-abcd-ef0123456789-00001.parquet";
    assertThat(longPath.length()).isGreaterThan(64);

    OutputFile outputFile = Files.localOutput(temp.resolve("long-path-deletes.vortex").toFile());
    PositionDeleteWriter<Void> writer =
        FormatModelRegistry.<Void>positionDeleteWriteBuilder(
                FileFormat.VORTEX, EncryptedFiles.plainAsEncryptedOutput(outputFile))
            .metricsConfig(MetricsConfig.forPositionDelete())
            .spec(PartitionSpec.unpartitioned())
            .build();
    PositionDelete<Void> delete = PositionDelete.create();
    writer.write(delete.set(longPath, 1L, null));
    writer.write(delete.set(longPath, 3L, null));
    writer.close();

    DeleteFile deleteFile = writer.toDeleteFile();
    assertThat(ContentFileUtil.referencedDataFile(deleteFile)).hasToString(longPath);
    assertThat(ContentFileUtil.isFileScoped(deleteFile)).isTrue();
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
}
