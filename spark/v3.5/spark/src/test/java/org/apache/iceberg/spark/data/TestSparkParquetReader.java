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
package org.apache.iceberg.spark.data;

import static org.apache.iceberg.spark.data.TestHelpers.assertEqualsUnsafe;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class TestSparkParquetReader extends AvroDataTest {
  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    assumeThat(
            TypeUtil.find(
                schema,
                type -> type.isMapType() && type.asMapType().keyType() != Types.StringType.get()))
        .as("Parquet Avro cannot write non-string map keys")
        .isNull();

    List<GenericData.Record> expected = RandomData.generateList(schema, 100, 0L);

    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).as("Delete should succeed").isTrue();

    try (FileAppender<GenericData.Record> writer =
        Parquet.write(Files.localOutput(testFile)).schema(schema).named("test").build()) {
      writer.addAll(expected);
    }

    try (CloseableIterable<InternalRow> reader =
        Parquet.read(Files.localInput(testFile))
            .project(schema)
            .createReaderFunc(type -> SparkParquetReaders.buildReader(schema, type))
            .build()) {
      Iterator<InternalRow> rows = reader.iterator();
      for (GenericData.Record record : expected) {
        assertThat(rows).as("Should have expected number of rows").hasNext();
        assertEqualsUnsafe(schema.asStruct(), record, rows.next());
      }
      assertThat(rows).as("Should not have extra rows").isExhausted();
    }
  }

  protected List<InternalRow> rowsFromFile(InputFile inputFile, Schema schema) throws IOException {
    try (CloseableIterable<InternalRow> reader =
        Parquet.read(inputFile)
            .project(schema)
            .createReaderFunc(type -> SparkParquetReaders.buildReader(schema, type))
            .build()) {
      return Lists.newArrayList(reader);
    }
  }

  protected Table tableFromInputFile(InputFile inputFile, Schema schema) throws IOException {
    HadoopTables tables = new HadoopTables();
    Table table =
        tables.create(
            schema,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(),
            java.nio.file.Files.createTempDirectory(temp, null).toFile().getCanonicalPath());

    table
        .newAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withFormat(FileFormat.PARQUET)
                .withInputFile(inputFile)
                .withMetrics(ParquetUtil.fileMetrics(inputFile, MetricsConfig.getDefault()))
                .withFileSizeInBytes(inputFile.getLength())
                .build())
        .commit();

    return table;
  }

  @Test
  public void testInt96TimestampProducedBySparkIsReadCorrectly() throws IOException {
    String outputFilePath = String.format("%s/%s", temp.toAbsolutePath(), "parquet_int96.parquet");
    HadoopOutputFile outputFile =
        HadoopOutputFile.fromPath(
            new org.apache.hadoop.fs.Path(outputFilePath), new Configuration());
    Schema schema = new Schema(required(1, "ts", Types.TimestampType.withZone()));
    StructType sparkSchema =
        new StructType(
            new StructField[] {
              new StructField("ts", DataTypes.TimestampType, true, Metadata.empty())
            });
    List<InternalRow> rows = Lists.newArrayList(RandomData.generateSpark(schema, 10, 0L));

    try (ParquetWriter<InternalRow> writer =
        new NativeSparkWriterBuilder(outputFile)
            .set("org.apache.spark.sql.parquet.row.attributes", sparkSchema.json())
            .set("spark.sql.parquet.writeLegacyFormat", "false")
            .set("spark.sql.parquet.outputTimestampType", "INT96")
            .set("spark.sql.parquet.fieldId.write.enabled", "true")
            .build()) {
      for (InternalRow row : rows) {
        writer.write(row);
      }
    }

    InputFile parquetInputFile = Files.localInput(outputFilePath);
    List<InternalRow> readRows = rowsFromFile(parquetInputFile, schema);

    assertThat(readRows).hasSameSizeAs(rows);
    assertThat(readRows).isEqualTo(rows);

    // Now we try to import that file as an Iceberg table to make sure Iceberg can read
    // Int96 end to end.
    Table int96Table = tableFromInputFile(parquetInputFile, schema);
    List<Record> tableRecords = Lists.newArrayList(IcebergGenerics.read(int96Table).build());

    assertThat(tableRecords).hasSameSizeAs(rows);

    for (int i = 0; i < tableRecords.size(); i++) {
      GenericsHelpers.assertEqualsUnsafe(schema.asStruct(), tableRecords.get(i), rows.get(i));
    }
  }

  @Test
  public void testVariant() throws IOException {
    String outputFilePath = String.format("%s/%s", temp.toAbsolutePath(), "variant.parquet");
    HadoopOutputFile outputFile =
        HadoopOutputFile.fromPath(
            new org.apache.hadoop.fs.Path(outputFilePath), new Configuration());
    Schema schema = new Schema(required(1, "v", Types.VariantType.get()));
    StructType sparkSchema =
        new StructType(
            new StructField[] {
              new StructField("v", DataTypes.VariantType, true, Metadata.empty())
            });
    List<InternalRow> rows = Lists.newArrayList(RandomData.generateSpark(schema, 10, 0L));

    try (ParquetWriter<InternalRow> writer =
        new NativeSparkWriterBuilder(outputFile)
            .set("org.apache.spark.sql.parquet.row.attributes", sparkSchema.json())
            .set("spark.sql.parquet.writeLegacyFormat", "false")
            .set("spark.sql.parquet.outputTimestampType", "INT96")
            .set("spark.sql.parquet.fieldId.write.enabled", "true")
            .build()) {
      for (InternalRow row : rows) {
        writer.write(row);
      }
    }

    InputFile parquetInputFile = Files.localInput(outputFilePath);
    List<InternalRow> readRows = rowsFromFile(parquetInputFile, schema);

    assertThat(readRows).hasSameSizeAs(rows);
    assertThat(readRows).isEqualTo(rows);

    // Now we try to import that file as an Iceberg table to make sure Iceberg can read
    // Variant end to end.
    Table variantTable = tableFromInputFile(parquetInputFile, schema);
    List<Record> tableRecords = Lists.newArrayList(IcebergGenerics.read(variantTable).build());

    assertThat(tableRecords).hasSameSizeAs(rows);

    for (int i = 0; i < tableRecords.size(); i++) {
      GenericsHelpers.assertEqualsUnsafe(schema.asStruct(), tableRecords.get(i), rows.get(i));
    }
  }

  /**
   * Native Spark ParquetWriter.Builder implementation so that we can write timestamps using Spark's
   * native ParquetWriteSupport.
   */
  private static class NativeSparkWriterBuilder
      extends ParquetWriter.Builder<InternalRow, NativeSparkWriterBuilder> {
    private final Map<String, String> config = Maps.newHashMap();

    NativeSparkWriterBuilder(org.apache.parquet.io.OutputFile path) {
      super(path);
    }

    public NativeSparkWriterBuilder set(String property, String value) {
      this.config.put(property, value);
      return self();
    }

    @Override
    protected NativeSparkWriterBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<InternalRow> getWriteSupport(Configuration configuration) {
      for (Map.Entry<String, String> entry : config.entrySet()) {
        configuration.set(entry.getKey(), entry.getValue());
      }

      return new org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport();
    }
  }
}
