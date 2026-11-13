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
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.RandomUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestRepairMetrics {

  private static final List<Type> STORED_BOUND_TYPES =
      List.of(
          Types.FloatType.get(),
          Types.DoubleType.get(),
          Types.GeometryType.crs84(),
          Types.GeographyType.crs84());
  private static final List<Type> RECOMPUTED_BOUND_TYPES =
      List.of(Types.IntegerType.get(), Types.StringType.get());
  private static final long FILE_SIZE = 100;
  private static final Metrics STORED_METRICS =
      new Metrics(
          2L,
          Map.of(1, 8L),
          Map.of(1, 2L),
          Map.of(1, 0L),
          Map.of(2, 1L),
          Map.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1)),
          Map.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), 3)),
          Map.of(3, 21L),
          null);

  @TempDir private Path temp;

  @ParameterizedTest
  @FieldSource("STORED_BOUND_TYPES")
  void keepsStoredBounds(Type type) throws IOException {
    Schema schema = new Schema(optional(1, "value", type));
    Object value =
        switch (type.typeId()) {
          case FLOAT -> 1.0F;
          case DOUBLE -> 1.0D;
          case GEOMETRY, GEOGRAPHY -> ByteBuffer.wrap(RandomUtil.wkbPoint(30, 10));
          default -> throw new IllegalArgumentException("Unexpected type: " + type);
        };
    ByteBuffer bound =
        switch (type.typeId()) {
          case GEOMETRY, GEOGRAPHY -> GeospatialBound.createXY(30, 10).toByteBuffer();
          default -> Conversions.toByteBuffer(type, value);
        };
    DataFile written =
        writeDataFile(schema, List.of(GenericRecord.create(schema).copy("value", value)));
    // set the stored bounds explicitly, since the Parquet writer records none for geography, and
    // leave out NaN counts, which keep a column's bounds whatever its type
    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .copy(written)
            .withMetrics(
                new Metrics(
                    written.recordCount(),
                    written.columnSizes(),
                    written.valueCounts(),
                    written.nullValueCounts(),
                    null,
                    Map.of(1, bound),
                    Map.of(1, bound)))
            .build();
    MetricsConfig noMetrics =
        MetricsConfig.from(
            Map.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none"),
            schema,
            SortOrder.unsorted());

    Metrics metrics =
        RepairMetrics.readMetrics(Files.localInput(file.location()), file, noMetrics, null, schema);

    assertThat(metrics.lowerBounds()).isEqualTo(file.lowerBounds());
    assertThat(metrics.upperBounds()).isEqualTo(file.upperBounds());
  }

  @ParameterizedTest
  @FieldSource("RECOMPUTED_BOUND_TYPES")
  void replacesRecomputedBounds(Type type) throws IOException {
    Schema schema = new Schema(optional(1, "value", type));
    Record template = GenericRecord.create(schema);
    Object lower = type.typeId() == Type.TypeID.INTEGER ? 1 : "a";
    Object upper = type.typeId() == Type.TypeID.INTEGER ? 3 : "z";
    Object stored = type.typeId() == Type.TypeID.INTEGER ? 2 : "m";
    DataFile original =
        writeDataFile(
            schema, List.of(template.copy("value", lower), template.copy("value", upper)));
    DataFile corrupt =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .copy(original)
            .withMetrics(
                new Metrics(
                    original.recordCount(),
                    original.columnSizes(),
                    original.valueCounts(),
                    original.nullValueCounts(),
                    original.nanValueCounts(),
                    Map.of(1, Conversions.toByteBuffer(type, stored)),
                    Map.of(1, Conversions.toByteBuffer(type, stored))))
            .build();

    Metrics metrics =
        RepairMetrics.readMetrics(
            Files.localInput(corrupt.location()),
            corrupt,
            MetricsConfig.getDefault(),
            null,
            schema);

    assertThat(metrics.lowerBounds()).isEqualTo(original.lowerBounds());
    assertThat(metrics.upperBounds()).isEqualTo(original.upperBounds());
  }

  @Test
  void keepsNaNCounts() throws IOException {
    Schema schema =
        new Schema(
            optional(1, "f", Types.FloatType.get()), optional(2, "d", Types.DoubleType.get()));
    Record template = GenericRecord.create(schema);
    DataFile file =
        writeDataFile(
            schema,
            List.of(
                template.copy("f", Float.NaN, "d", Double.NaN),
                template.copy("f", 1.0F, "d", 2.0D)));

    Metrics metrics =
        RepairMetrics.readMetrics(
            Files.localInput(file.location()),
            file,
            MetricsConfig.getDefault(),
            null,
            schema.select("d"));

    assertThat(metrics.nanValueCounts()).isNotEmpty().isEqualTo(file.nanValueCounts());
  }

  @Test
  void positionDeleteBoundsForSingleReferencedFile() throws IOException {
    DeleteFile single = writePositionDeletes("one.parquet");
    DeleteFile multiple = writePositionDeletes("one.parquet", "two.parquet");
    Schema schema = new Schema(MetadataColumns.DELETE_FILE_PATH, MetadataColumns.DELETE_FILE_POS);

    Metrics singleMetrics =
        RepairMetrics.readMetrics(
            Files.localInput(single.location()),
            single,
            MetricsConfig.forPositionDelete(),
            null,
            schema);
    Metrics multipleMetrics =
        RepairMetrics.readMetrics(
            Files.localInput(multiple.location()),
            multiple,
            MetricsConfig.forPositionDelete(),
            null,
            schema);

    assertThat(singleMetrics.lowerBounds())
        .containsKey(MetadataColumns.DELETE_FILE_PATH.fieldId())
        .isEqualTo(single.lowerBounds());
    assertThat(singleMetrics.upperBounds()).isEqualTo(single.upperBounds());
    assertThat(multipleMetrics.lowerBounds()).isNullOrEmpty();
    assertThat(multipleMetrics.upperBounds()).isNullOrEmpty();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "unchanged",
        "file-size",
        "record-count",
        "column-sizes",
        "value-counts",
        "null-value-counts",
        "lower-bounds",
        "upper-bounds"
      })
  void statsAreIncorrect(String difference) {
    DataFile file = dataFile(STORED_METRICS);
    Map<Integer, Long> changedCounts = Map.of(1, 100L);
    Map<Integer, ByteBuffer> changedBounds =
        Map.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), 2));
    Metrics metrics =
        new Metrics(
            difference.equals("record-count") ? file.recordCount() + 1 : file.recordCount(),
            difference.equals("column-sizes") ? changedCounts : file.columnSizes(),
            difference.equals("value-counts") ? changedCounts : file.valueCounts(),
            difference.equals("null-value-counts") ? changedCounts : file.nullValueCounts(),
            file.nanValueCounts(),
            difference.equals("lower-bounds") ? changedBounds : file.lowerBounds(),
            difference.equals("upper-bounds") ? changedBounds : file.upperBounds());
    long fileSize = difference.equals("file-size") ? FILE_SIZE + 1 : FILE_SIZE;

    assertThat(RepairMetrics.statsAreIncorrect(file, metrics, fileSize, true))
        .isEqualTo(!difference.equals("unchanged"));
  }

  @Test
  void statsAreIncorrectIgnoresColumnMetricsWhenDisabled() {
    DataFile file = dataFile(STORED_METRICS);
    Metrics metrics = new Metrics(file.recordCount());

    assertThat(RepairMetrics.statsAreIncorrect(file, metrics, FILE_SIZE, false)).isFalse();
  }

  @Test
  void missingAndEmptyMapsMatch() {
    Metrics missing = new Metrics(STORED_METRICS.recordCount());
    Metrics empty =
        new Metrics(
            missing.recordCount(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of());

    assertThat(RepairMetrics.statsAreIncorrect(dataFile(missing), empty, FILE_SIZE, true))
        .isFalse();
    assertThat(RepairMetrics.statsAreIncorrect(dataFile(empty), missing, FILE_SIZE, true))
        .isFalse();
  }

  @Test
  void recordCountOnlyKeepsColumnStats() {
    DataFile file = dataFile(STORED_METRICS);
    Metrics recomputed = new Metrics(file.recordCount() + 1);

    Metrics metrics = RepairMetrics.recordCountOnly(file, recomputed);

    assertThat(metrics.recordCount()).isEqualTo(recomputed.recordCount());
    assertThat(metrics.columnSizes()).isEqualTo(file.columnSizes());
    assertThat(metrics.valueCounts()).isEqualTo(file.valueCounts());
    assertThat(metrics.nullValueCounts()).isEqualTo(file.nullValueCounts());
    assertThat(metrics.nanValueCounts()).isEqualTo(file.nanValueCounts());
    assertThat(metrics.lowerBounds()).isEqualTo(file.lowerBounds());
    assertThat(metrics.upperBounds()).isEqualTo(file.upperBounds());
    assertThat(metrics.totalBytes()).isEqualTo(file.totalBytes());
  }

  @Test
  void withStatsKeepsEqualityFieldIds() {
    DeleteFile file =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofEqualityDeletes(1, 2)
            .withPath("deletes.parquet")
            .withFileSizeInBytes(FILE_SIZE)
            .withMetrics(STORED_METRICS)
            .build();

    DeleteFile repaired =
        (DeleteFile)
            RepairMetrics.withStats(
                file,
                PartitionSpec.unpartitioned(),
                new Metrics(file.recordCount() + 1),
                FILE_SIZE + 1);

    assertThat(repaired.equalityFieldIds()).isEqualTo(file.equalityFieldIds());
  }

  private DataFile dataFile(Metrics metrics) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("data.parquet")
        .withFileSizeInBytes(FILE_SIZE)
        .withMetrics(metrics)
        .build();
  }

  private DataFile writeDataFile(Schema schema, List<Record> rows) throws IOException {
    DataWriter<Record> writer =
        Parquet.writeData(Files.localOutput(temp.resolve("data.parquet").toFile()))
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::create)
            .withSpec(PartitionSpec.unpartitioned())
            .build();
    try (writer) {
      writer.write(rows);
    }

    return writer.toDataFile();
  }

  private DeleteFile writePositionDeletes(String... paths) throws IOException {
    PositionDeleteWriter<Record> writer =
        Parquet.writeDeletes(
                Files.localOutput(temp.resolve("deletes-" + paths.length + ".parquet").toFile()))
            .createWriterFunc(GenericParquetWriter::create)
            .withSpec(PartitionSpec.unpartitioned())
            .buildPositionWriter();
    PositionDelete<Record> delete = PositionDelete.create();
    try (writer) {
      for (String path : paths) {
        writer.write(delete.set(path, 0L));
      }
    }

    return writer.toDeleteFile();
  }
}
