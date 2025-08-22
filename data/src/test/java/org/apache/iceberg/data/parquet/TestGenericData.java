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
package org.apache.iceberg.data.parquet;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.DataTestBase;
import org.apache.iceberg.data.DataTestHelpers;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;

public class TestGenericData extends DataTestBase {
  @Override
  protected boolean supportsDefaultValues() {
    return true;
  }

  @Override
  protected boolean supportsUnknown() {
    return true;
  }

  @Override
  protected boolean supportsTimestampNanos() {
    return true;
  }

  @Override
  protected boolean supportsRowLineage() {
    return true;
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(schema, schema);
  }

  @Override
  protected void writeAndValidate(Schema schema, List<Record> expected) throws IOException {
    writeAndValidate(schema, schema, expected);
  }

  @Override
  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema) throws IOException {
    List<Record> data = RandomGenericData.generate(writeSchema, 100, 12228L);
    writeAndValidate(writeSchema, expectedSchema, data);
  }

  private void writeAndValidate(Schema writeSchema, Schema expectedSchema, List<Record> expected)
      throws IOException {

    OutputFile output = new InMemoryOutputFile();

    try (FileAppender<Record> appender =
        Parquet.write(output)
            .schema(writeSchema)
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      appender.addAll(expected);
    }

    List<Record> rows;
    try (CloseableIterable<Record> reader =
        Parquet.read(output.toInputFile())
            .project(expectedSchema)
            .createReaderFunc(
                fileSchema ->
                    GenericParquetReaders.buildReader(expectedSchema, fileSchema, ID_TO_CONSTANT))
            .build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int pos = 0; pos < expected.size(); pos += 1) {
      DataTestHelpers.assertEquals(
          expectedSchema.asStruct(), expected.get(pos), rows.get(pos), ID_TO_CONSTANT, pos);
    }

    // test reuseContainers
    try (CloseableIterable<Record> reader =
        Parquet.read(output.toInputFile())
            .project(expectedSchema)
            .reuseContainers()
            .createReaderFunc(
                fileSchema ->
                    GenericParquetReaders.buildReader(expectedSchema, fileSchema, ID_TO_CONSTANT))
            .build()) {
      int pos = 0;
      for (Record actualRecord : reader) {
        DataTestHelpers.assertEquals(
            expectedSchema.asStruct(), expected.get(pos), actualRecord, ID_TO_CONSTANT, pos);
        pos += 1;
      }
    }
  }

  @Test
  public void testTwoLevelList() throws IOException {
    Schema schema =
        new Schema(
            optional(1, "arraybytes", Types.ListType.ofRequired(3, Types.BinaryType.get())),
            optional(2, "topbytes", Types.BinaryType.get()));
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());

    File testFile = temp.resolve("test-file" + System.nanoTime()).toFile();

    ParquetWriter<org.apache.avro.generic.GenericRecord> writer =
        AvroParquetWriter.<org.apache.avro.generic.GenericRecord>builder(new Path(testFile.toURI()))
            .withDataModel(GenericData.get())
            .withSchema(avroSchema)
            .config("parquet.avro.add-list-element-records", "true")
            .config("parquet.avro.write-old-list-structure", "true")
            .build();

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);
    List<ByteBuffer> expectedByteList = new ArrayList();
    byte[] expectedByte = {0x00, 0x01};
    ByteBuffer expectedBinary = ByteBuffer.wrap(expectedByte);
    expectedByteList.add(expectedBinary);
    recordBuilder.set("arraybytes", expectedByteList);
    recordBuilder.set("topbytes", expectedBinary);
    GenericData.Record expectedRecord = recordBuilder.build();

    writer.write(expectedRecord);
    writer.close();

    // test reuseContainers
    try (CloseableIterable<Record> reader =
        Parquet.read(Files.localInput(testFile))
            .project(schema)
            .reuseContainers()
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build()) {
      for (Record actualRecord : reader) {
        assertThat(actualRecord.get(0, ArrayList.class)).first().isEqualTo(expectedBinary);
        assertThat(actualRecord.get(1, ByteBuffer.class)).isEqualTo(expectedBinary);
      }

      assertThat(Lists.newArrayList(reader)).hasSize(1);
    }
  }
}
