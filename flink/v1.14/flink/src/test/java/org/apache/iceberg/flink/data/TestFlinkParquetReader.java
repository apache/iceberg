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
package org.apache.iceberg.flink.data;

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.DataTest;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetWriteAdapter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.RandomUtil;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestFlinkParquetReader extends DataTest {
  private static final int NUM_RECORDS = 100;

  @Test
  public void testTwoLevelList() throws IOException {
    Schema schema =
        new Schema(
            optional(1, "arraybytes", Types.ListType.ofRequired(3, Types.BinaryType.get())),
            optional(2, "topbytes", Types.BinaryType.get()));
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());

    File testFile = temp.newFile();
    Assert.assertTrue(testFile.delete());

    ParquetWriter<GenericRecord> writer =
        AvroParquetWriter.<GenericRecord>builder(new Path(testFile.toURI()))
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

    try (CloseableIterable<RowData> reader =
        Parquet.read(Files.localInput(testFile))
            .project(schema)
            .createReaderFunc(type -> FlinkParquetReaders.buildReader(schema, type))
            .build()) {
      Iterator<RowData> rows = reader.iterator();
      Assert.assertTrue("Should have at least one row", rows.hasNext());
      RowData rowData = rows.next();
      Assert.assertArrayEquals(rowData.getArray(0).getBinary(0), expectedByte);
      Assert.assertArrayEquals(rowData.getBinary(1), expectedByte);
      Assert.assertFalse("Should not have more than one row", rows.hasNext());
    }
  }

  private void writeAndValidate(Iterable<Record> iterable, Schema schema) throws IOException {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<Record> writer =
        Parquet.write(Files.localOutput(testFile))
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .build()) {
      writer.addAll(iterable);
    }

    try (CloseableIterable<RowData> reader =
        Parquet.read(Files.localInput(testFile))
            .project(schema)
            .createReaderFunc(type -> FlinkParquetReaders.buildReader(schema, type))
            .build()) {
      Iterator<Record> expected = iterable.iterator();
      Iterator<RowData> rows = reader.iterator();
      LogicalType rowType = FlinkSchemaUtil.convert(schema);
      for (int i = 0; i < NUM_RECORDS; i += 1) {
        Assert.assertTrue("Should have expected number of rows", rows.hasNext());
        TestHelpers.assertRowData(schema.asStruct(), rowType, expected.next(), rows.next());
      }
      Assert.assertFalse("Should not have extra rows", rows.hasNext());
    }
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(RandomGenericData.generate(schema, NUM_RECORDS, 19981), schema);
    writeAndValidate(
        RandomGenericData.generateDictionaryEncodableRecords(schema, NUM_RECORDS, 21124), schema);
    writeAndValidate(
        RandomGenericData.generateFallbackRecords(schema, NUM_RECORDS, 21124, NUM_RECORDS / 20),
        schema);
  }

  protected List<RowData> flinkReadRowDataFromFile(InputFile inputFile, Schema schema) throws IOException {
    try (CloseableIterable<RowData> reader =
        Parquet.read(inputFile)
            .project(schema)
            .createReaderFunc(type -> FlinkParquetReaders.buildReader(schema, type))
            .build()) {
      return Lists.newArrayList(reader);
    }
  }

  protected List<org.apache.iceberg.data.GenericRecord> genericReadRowDataFromFile(InputFile inputFile,
      Schema schema) throws IOException {
    try (CloseableIterable<org.apache.iceberg.data.GenericRecord> reader =
                 Parquet.read(inputFile)
                         .project(schema)
                         .createReaderFunc(type -> GenericParquetReaders.buildReader(schema, type))
                         .build()) {
      return Lists.newArrayList(reader);
    }
  }

  @Test
  public void testInt96TimestampProducedBySparkIsReadCorrectly() throws IOException {
    String outputFilePath = String.format("%s/%s", temp.getRoot().getAbsolutePath(), "parquet_int96.parquet");
    HadoopOutputFile outputFile =
        HadoopOutputFile.fromPath(
            new org.apache.hadoop.fs.Path(outputFilePath), new Configuration());
    Schema schema = new Schema(required(1, "ts", Types.TimestampType.withoutZone()));
    StructType sparkSchema =
        new StructType(
            new StructField[] {
                new StructField("ts", DataTypes.TimestampType, true, Metadata.empty())
            });

    final Random random = new Random(0L);
    List<InternalRow> rows = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      rows.add(new GenericInternalRow(new Object[] {
          RandomUtil.generatePrimitive(schema.asStruct().fieldType("ts").asPrimitiveType(), random)}));
    }

    try (FileAppender<InternalRow> writer =
        new ParquetWriteAdapter<>(
            new NativeSparkWriterBuilder(outputFile)
                .set("org.apache.spark.sql.parquet.row.attributes", sparkSchema.json())
                .set("spark.sql.parquet.writeLegacyFormat", "false")
                .set("spark.sql.parquet.outputTimestampType", "INT96")
                .build(),
            MetricsConfig.getDefault())) {
      writer.addAll(rows);
    }

    InputFile parquetInputFile = Files.localInput(outputFilePath);
    List<RowData> flinkReadDataRows = flinkReadRowDataFromFile(parquetInputFile, schema);
    List<org.apache.iceberg.data.GenericRecord> genericReadDataRows = genericReadRowDataFromFile(parquetInputFile,
        schema);

    Assert.assertEquals(rows.size(), flinkReadDataRows.size());
    Assert.assertEquals(rows.size(), genericReadDataRows.size());
    for (int i = 0; i < rows.size(); i += 1) {
      TimestampData actual = ((TimestampData) ((GenericRowData) flinkReadDataRows.get(i)).getField(0));
      Assert.assertEquals(
          rows.get(i).getLong(0),
          actual.getMillisecond() * 1000 + actual.getNanoOfMillisecond() / 1000);

      OffsetDateTime expect = ((OffsetDateTime) genericReadDataRows.get(i).getField("ts"));
      Assert.assertTrue(expect.toLocalDateTime().equals(actual.toLocalDateTime()));
    }
  }

  /**
   * Native Spark ParquetWriter.Builder implementation so that we can write timestamps using Spark's native
   * ParquetWriteSupport.
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
