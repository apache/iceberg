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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestMetrics;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.GeometryType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/** Test Metrics for Parquet. */
@ExtendWith(ParameterizedTestExtension.class)
public class TestParquetMetrics extends TestMetrics {
  private static final Map<String, String> SMALL_ROW_GROUP_CONFIG =
      ImmutableMap.of(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1600");

  @Override
  public FileFormat fileFormat() {
    return FileFormat.PARQUET;
  }

  @Override
  protected OutputFile createOutputFile() throws IOException {
    File tmpFolder = java.nio.file.Files.createTempDirectory(temp, "parquet").toFile();
    String filename = UUID.randomUUID().toString();
    return Files.localOutput(new File(tmpFolder, FileFormat.PARQUET.addExtension(filename)));
  }

  @Override
  public Metrics getMetrics(Schema schema, Record... records) throws IOException {
    return getMetrics(schema, MetricsConfig.getDefault(), records);
  }

  @Override
  public Metrics getMetrics(Schema schema, MetricsConfig metricsConfig, Record... records)
      throws IOException {
    return getMetrics(schema, createOutputFile(), ImmutableMap.of(), metricsConfig, records);
  }

  private Metrics getMetrics(
      Schema schema,
      OutputFile file,
      Map<String, String> properties,
      MetricsConfig metricsConfig,
      Record... records)
      throws IOException {
    FileAppender<Record> writer =
        Parquet.write(file)
            .schema(schema)
            .setAll(properties)
            .createWriterFunc(GenericParquetWriter::create)
            .metricsConfig(metricsConfig)
            .build();
    try (FileAppender<Record> appender = writer) {
      appender.addAll(Lists.newArrayList(records));
    }
    return writer.metrics();
  }

  @Override
  protected Metrics getMetricsForRecordsWithSmallRowGroups(
      Schema schema, OutputFile outputFile, Record... records) throws IOException {
    return getMetrics(
        schema, outputFile, SMALL_ROW_GROUP_CONFIG, MetricsConfig.getDefault(), records);
  }

  @Override
  public int splitCount(InputFile inputFile) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile))) {
      return reader.getRowGroups().size();
    }
  }

  @Override
  public boolean supportsSmallRowGroups() {
    return true;
  }

  @TestTemplate
  public void testMetricsForNullStructWithFloatingAndGeoLeaves() throws IOException {
    // float, double, geometry and geography are the only types whose writers report metrics, so
    // they are the ones whose nested null counts could be dropped when a struct is null. Null
    // counts are only tracked for optional fields.
    StructType struct =
        StructType.of(
            optional(2, "optDouble", DoubleType.get()),
            optional(3, "optFloat", FloatType.get()),
            optional(4, "geom", GeometryType.crs84()),
            optional(5, "optLong", LongType.get()));
    Schema schema = new Schema(optional(1, "struct", struct));

    Record inner = GenericRecord.create(struct);
    inner.setField("optDouble", 1.5D);
    inner.setField("optFloat", 2.5F);
    inner.setField("geom", wkbPoint(30, 10));
    inner.setField("optLong", 10L);
    Record withStruct = GenericRecord.create(schema);
    withStruct.setField("struct", inner);
    Record nullStruct = GenericRecord.create(schema);
    nullStruct.setField("struct", null);

    Metrics metrics = getMetrics(schema, withStruct, nullStruct, nullStruct);

    assertThat(metrics.recordCount()).isEqualTo(3L);
    // each leaf has one value from the populated struct and two nulls from the null structs
    assertCounts(2, 3L, 2L, 0L, metrics);
    assertCounts(3, 3L, 2L, 0L, metrics);
    assertCounts(4, 3L, 2L, metrics);
    // a type without writer metrics was already correct via the footer; included as a control
    assertCounts(5, 3L, 2L, metrics);

    // the counts also reach a data file built from these metrics
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/file.parquet")
            .withFileSizeInBytes(1024)
            .withFormat(FileFormat.PARQUET)
            .withMetrics(metrics)
            .build();
    assertThat(dataFile.nullValueCounts()).containsEntry(2, 2L).containsEntry(3, 2L);
  }

  @TestTemplate
  public void testMetricsForRequiredNestedFieldInNullStruct() throws IOException {
    // null counts are only tracked for optional fields, so a required float/double leaf keeps its
    // prior behavior when the struct is null: the null count is not recorded.
    StructType struct =
        StructType.of(
            required(2, "reqDouble", DoubleType.get()), required(3, "reqLong", LongType.get()));
    Schema schema = new Schema(optional(1, "struct", struct));

    Record inner = GenericRecord.create(struct);
    inner.setField("reqDouble", 1.5D);
    inner.setField("reqLong", 10L);
    Record withStruct = GenericRecord.create(schema);
    withStruct.setField("struct", inner);
    Record nullStruct = GenericRecord.create(schema);
    nullStruct.setField("struct", null);

    Metrics metrics = getMetrics(schema, withStruct, nullStruct, nullStruct);

    assertThat(metrics.recordCount()).isEqualTo(3L);
    // reqDouble uses writer metrics, which do not count nulls for required fields: only the
    // populated struct's value is seen
    assertCounts(2, 1L, 0L, 0L, metrics);
    // reqLong uses footer stats, so its counts are unaffected
    assertCounts(3, 3L, 2L, metrics);

    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/file.parquet")
            .withFileSizeInBytes(1024)
            .withFormat(FileFormat.PARQUET)
            .withMetrics(metrics)
            .build();
    assertThat(dataFile.nullValueCounts()).containsEntry(2, 0L).containsEntry(3, 2L);
  }

  private static ByteBuffer wkbPoint(double xCoord, double yCoord) {
    // little-endian WKB encoding of a point
    return ByteBuffer.wrap(
        ByteBuffer.allocate(21)
            .order(ByteOrder.LITTLE_ENDIAN)
            .put((byte) 1) // byte order: little endian
            .putInt(1) // WKB geometry type: Point
            .putDouble(xCoord)
            .putDouble(yCoord)
            .array());
  }
}
