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
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.DataTestBase;
import org.apache.iceberg.data.DataTestHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
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
  protected boolean supportsGeospatial() {
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
        AvroParquetWriter.<org.apache.avro.generic.GenericRecord>builder(
                new LocalOutputFile(testFile.toPath()))
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

  @Test
  public void testNestedInitialDefaultWhenAncestorStructIsNull() throws IOException {
    // one row has an inner value, the other has a null nested struct
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("nested")
                .withId(2)
                .ofType(
                    Types.StructType.of(
                        Types.NestedField.required(3, "inner", Types.StringType.get())))
                .build());

    Record present = GenericRecord.create(writeSchema);
    present.setField("id", 1L);
    Record presentNested =
        GenericRecord.create(writeSchema.findField("nested").type().asStructType());
    presentNested.setField("inner", "a");
    present.setField("nested", presentNested);

    Record nullNested = GenericRecord.create(writeSchema);
    nullNested.setField("id", 2L);
    nullNested.setField("nested", null);

    OutputFile output = new InMemoryOutputFile();
    try (FileAppender<Record> appender =
        Parquet.write(output)
            .schema(writeSchema)
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      appender.add(present);
      appender.add(nullNested);
    }

    // project only the added field with the default; inner is not read
    Schema readSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("nested")
                .withId(2)
                .ofType(
                    Types.StructType.of(
                        Types.NestedField.optional("added")
                            .withId(4)
                            .ofType(Types.StringType.get())
                            .withInitialDefault(Literal.of("US"))
                            .build()))
                .build());

    List<Record> rows;
    try (CloseableIterable<Record> reader =
        Parquet.read(output.toInputFile())
            .project(readSchema)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(readSchema, fileSchema))
            .build()) {
      rows = Lists.newArrayList(reader);
    }

    assertThat(rows).hasSize(2);

    // present struct reads the default
    Record row1Nested = (Record) rows.get(0).getField("nested");
    assertThat(row1Nested).isNotNull();
    assertThat(row1Nested.getField("added")).isEqualTo("US");

    // null struct reads as null
    assertThat(rows.get(1).getField("nested")).isNull();
  }

  @Test
  public void testNestedInitialDefaultWithInterleavedNullStructs() throws IOException {
    // alternate present and null nested structs across rows
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("nested")
                .withId(2)
                .ofType(
                    Types.StructType.of(
                        Types.NestedField.required(3, "inner", Types.StringType.get())))
                .build());

    Types.StructType writeNestedType = writeSchema.findField("nested").type().asStructType();

    // present, null, present, null, present
    List<Record> records = Lists.newArrayList();
    boolean[] present = {true, false, true, false, true};
    for (int i = 0; i < present.length; i += 1) {
      Record record = GenericRecord.create(writeSchema);
      record.setField("id", (long) i);
      if (present[i]) {
        Record nested = GenericRecord.create(writeNestedType);
        nested.setField("inner", "inner-" + i);
        record.setField("nested", nested);
      } else {
        record.setField("nested", null);
      }

      records.add(record);
    }

    OutputFile output = new InMemoryOutputFile();
    try (FileAppender<Record> appender =
        Parquet.write(output)
            .schema(writeSchema)
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      appender.addAll(records);
    }

    // project only the added field with the default; inner is not read
    Schema readSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("nested")
                .withId(2)
                .ofType(
                    Types.StructType.of(
                        Types.NestedField.optional("added")
                            .withId(4)
                            .ofType(Types.StringType.get())
                            .withInitialDefault(Literal.of("US"))
                            .build()))
                .build());

    List<Record> rows;
    try (CloseableIterable<Record> reader =
        Parquet.read(output.toInputFile())
            .project(readSchema)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(readSchema, fileSchema))
            .build()) {
      rows = Lists.newArrayList(reader);
    }

    assertThat(rows).hasSize(present.length);
    for (int i = 0; i < present.length; i += 1) {
      Record nested = (Record) rows.get(i).getField("nested");
      if (present[i]) {
        // present struct reads the default
        assertThat(nested).as("row %s nested struct should be present", i).isNotNull();
        assertThat(nested.getField("added")).isEqualTo("US");
      } else {
        // null struct reads as null
        assertThat(nested).as("row %s nested struct should be null", i).isNull();
      }
    }
  }
}
