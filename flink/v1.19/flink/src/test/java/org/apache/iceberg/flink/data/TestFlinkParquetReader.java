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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;

public class TestFlinkParquetReader extends DataTest {
  private static final int NUM_RECORDS = 100;

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
