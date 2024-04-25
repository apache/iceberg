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
import static org.apache.parquet.schema.Types.primitive;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.DataTest;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

public class TestFlinkParquetReader extends DataTest {
  private static final int NUM_RECORDS = 100;

  @Test
  public void testBuildReader() {
    MessageType fileSchema =
        new MessageType(
            "test",
            // 0: required(100, "id", LongType.get())
            primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
                .id(100)
                .named("id"),
            // 1: optional(101, "data", Types.StringType.get())
            primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
                .id(101)
                .named("data"),
            // 2: required(102, "b", Types.BooleanType.get())
            primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.REQUIRED)
                .id(102)
                .named("b"),
            // 3: optional(103, "i", Types.IntegerType.get())
            primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                .id(103)
                .named("i"),
            // 4: optional(105, "f", Types.FloatType.get())
            primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
                .id(104)
                .named("l"),
            // 5: required(106, "d", Types.DoubleType.get())
            primitive(PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.OPTIONAL)
                .id(105)
                .named("f"),
            // 6: required(106, "d", Types.DoubleType.get())
            primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                .id(106)
                .named("d"),
            // 7: optional(107, "date", Types.DateType.get())
            primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                .id(107)
                .as(LogicalTypeAnnotation.dateType())
                .named("date"),
            // 8: required(108, "ts_tz", Types.TimestampType.withZone())
            primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
                .id(108)
                .as(
                    LogicalTypeAnnotation.timestampType(
                        true, LogicalTypeAnnotation.TimeUnit.MICROS))
                .named("ts_tz"),
            // 9: required(109, "ts", Types.TimestampType.withoutZone())
            primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
                .id(109)
                .as(
                    LogicalTypeAnnotation.timestampType(
                        false, LogicalTypeAnnotation.TimeUnit.MICROS))
                .named("ts"),
            // 10: required(110, "s", Types.StringType.get())
            primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                .id(110)
                .as(LogicalTypeAnnotation.stringType())
                .named("s"),
            // 11: required(112, "fixed", Types.FixedType.ofLength(7))
            primitive(
                    PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Type.Repetition.REQUIRED)
                .id(112)
                .length(7)
                .named("f"),
            // 12: optional(113, "bytes", Types.BinaryType.get())
            primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
                .id(113)
                .named("bytes"),
            // 13: required(114, "dec_9_0", Types.DecimalType.of(9, 0))
            primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
                .id(114)
                .as(LogicalTypeAnnotation.decimalType(0, 9))
                .named("dec_9_0"),
            // 14: required(115, "dec_11_2", Types.DecimalType.of(11, 2))
            primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
                .id(115)
                .as(LogicalTypeAnnotation.decimalType(2, 11))
                .named("dec_11_2"),
            // 15: required(116, "dec_38_10", Types.DecimalType.of(38, 10)) // maximum precision
            primitive(
                    PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Type.Repetition.REQUIRED)
                .id(116)
                .length(16)
                .as(LogicalTypeAnnotation.decimalType(10, 38))
                .named("dec_38_10"),
            // 16: required(117, "time", Types.TimeType.get())
            primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
                .id(117)
                .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                .named("time"));
    ParquetValueReader<RowData> reader =
        FlinkParquetReaders.buildReader(new Schema(SUPPORTED_PRIMITIVES.fields()), fileSchema);

    assertThat(reader.columns().size()).isEqualTo(SUPPORTED_PRIMITIVES.fields().size());
  }

  @Test
  public void testTwoLevelList() throws IOException {
    Schema schema =
        new Schema(
            optional(1, "arraybytes", Types.ListType.ofRequired(3, Types.BinaryType.get())),
            optional(2, "topbytes", Types.BinaryType.get()));
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());

    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).isTrue();

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
      assertThat(rows).hasNext();
      RowData rowData = rows.next();
      assertThat(rowData.getArray(0).getBinary(0)).isEqualTo(expectedByte);
      assertThat(rowData.getBinary(1)).isEqualTo(expectedByte);
      assertThat(rows).isExhausted();
    }
  }

  private void writeAndValidate(Iterable<Record> iterable, Schema schema) throws IOException {
    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).isTrue();

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
        assertThat(rows).hasNext();
        TestHelpers.assertRowData(schema.asStruct(), rowType, expected.next(), rows.next());
      }
      assertThat(rows).isExhausted();
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
}
