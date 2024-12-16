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
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import org.apache.iceberg.data.DataTest;
import org.apache.iceberg.data.DataTestHelpers;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;

public class TestGenericData extends DataTest {
  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(schema, schema);
  }

  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema) throws IOException {
    List<Record> expected = RandomGenericData.generate(writeSchema, 100, 12228L);

    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).isTrue();

    try (FileAppender<Record> appender =
        Parquet.write(Files.localOutput(testFile))
            .schema(writeSchema)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .build()) {
      appender.addAll(expected);
    }

    List<Record> rows;
    try (CloseableIterable<Record> reader =
        Parquet.read(Files.localInput(testFile))
            .project(expectedSchema)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(expectedSchema, fileSchema))
            .build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      DataTestHelpers.assertEquals(expectedSchema.asStruct(), expected.get(i), rows.get(i));
    }

    // test reuseContainers
    try (CloseableIterable<Record> reader =
        Parquet.read(Files.localInput(testFile))
            .project(expectedSchema)
            .reuseContainers()
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(expectedSchema, fileSchema))
            .build()) {
      int index = 0;
      for (Record actualRecord : reader) {
        DataTestHelpers.assertEquals(expectedSchema.asStruct(), expected.get(index), actualRecord);
        index += 1;
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

    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).isTrue();

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

      assertThat(Lists.newArrayList(reader).size()).isEqualTo(1);
    }
  }

  @Test
  public void testMissingRequiredWithoutDefault() {
    Schema writeSchema = new Schema(required(1, "id", Types.LongType.get()));

    Schema expectedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            NestedField.required("missing_str")
                .withId(6)
                .ofType(Types.StringType.get())
                .withDoc("Missing required field with no default")
                .build());

    assertThatThrownBy(() -> writeAndValidate(writeSchema, expectedSchema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required field: missing_str");
  }

  @Test
  public void testDefaultValues() throws IOException {
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .withDoc("Should not produce default value")
                .build());

    Schema expectedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .build(),
            NestedField.required("missing_str")
                .withId(6)
                .ofType(Types.StringType.get())
                .withInitialDefault("orange")
                .build(),
            NestedField.optional("missing_int")
                .withId(7)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(34)
                .build());

    writeAndValidate(writeSchema, expectedSchema);
  }

  @Test
  public void testNullDefaultValue() throws IOException {
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .withDoc("Should not produce default value")
                .build());

    Schema expectedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .build(),
            NestedField.optional("missing_date").withId(3).ofType(Types.DateType.get()).build());

    writeAndValidate(writeSchema, expectedSchema);
  }

  @Test
  public void testNestedDefaultValue() throws IOException {
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .withDoc("Should not produce default value")
                .build(),
            NestedField.optional("nested")
                .withId(3)
                .ofType(Types.StructType.of(required(4, "inner", Types.StringType.get())))
                .withDoc("Used to test nested field defaults")
                .build());

    Schema expectedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .build(),
            NestedField.optional("nested")
                .withId(3)
                .ofType(
                    Types.StructType.of(
                        required(4, "inner", Types.StringType.get()),
                        NestedField.optional("missing_inner_float")
                            .withId(5)
                            .ofType(Types.FloatType.get())
                            .withInitialDefault(-0.0F)
                            .build()))
                .withDoc("Used to test nested field defaults")
                .build());

    writeAndValidate(writeSchema, expectedSchema);
  }

  @Test
  public void testMapNestedDefaultValue() throws IOException {
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .withDoc("Should not produce default value")
                .build(),
            NestedField.optional("nested_map")
                .withId(3)
                .ofType(
                    Types.MapType.ofOptional(
                        4,
                        5,
                        Types.StringType.get(),
                        Types.StructType.of(required(6, "value_str", Types.StringType.get()))))
                .withDoc("Used to test nested map value field defaults")
                .build());

    Schema expectedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .build(),
            NestedField.optional("nested_map")
                .withId(3)
                .ofType(
                    Types.MapType.ofOptional(
                        4,
                        5,
                        Types.StringType.get(),
                        Types.StructType.of(
                            required(6, "value_str", Types.StringType.get()),
                            Types.NestedField.optional("value_int")
                                .withId(7)
                                .ofType(Types.IntegerType.get())
                                .withInitialDefault(34)
                                .build())))
                .withDoc("Used to test nested field defaults")
                .build());

    writeAndValidate(writeSchema, expectedSchema);
  }

  @Test
  public void testListNestedDefaultValue() throws IOException {
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .withDoc("Should not produce default value")
                .build(),
            NestedField.optional("nested_list")
                .withId(3)
                .ofType(
                    Types.ListType.ofOptional(
                        4, Types.StructType.of(required(5, "element_str", Types.StringType.get()))))
                .withDoc("Used to test nested field defaults")
                .build());

    Schema expectedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .build(),
            NestedField.optional("nested_list")
                .withId(3)
                .ofType(
                    Types.ListType.ofOptional(
                        4,
                        Types.StructType.of(
                            required(5, "element_str", Types.StringType.get()),
                            Types.NestedField.optional("element_int")
                                .withId(7)
                                .ofType(Types.IntegerType.get())
                                .withInitialDefault(34)
                                .build())))
                .withDoc("Used to test nested field defaults")
                .build());

    writeAndValidate(writeSchema, expectedSchema);
  }
}
