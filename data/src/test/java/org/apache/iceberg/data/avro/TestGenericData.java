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
package org.apache.iceberg.data.avro;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.data.DataTest;
import org.apache.iceberg.data.DataTestHelpers;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestGenericData extends DataTest {
  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(schema, schema);
  }

  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema) throws IOException {
    List<Record> expected = RandomGenericData.generate(writeSchema, 100, 0L);

    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).isTrue();

    try (FileAppender<Record> writer =
        Avro.write(Files.localOutput(testFile))
            .schema(writeSchema)
            .createWriterFunc(DataWriter::create)
            .named("test")
            .build()) {
      for (Record rec : expected) {
        writer.add(rec);
      }
    }

    List<Record> rows;
    try (AvroIterable<Record> reader =
        Avro.read(Files.localInput(testFile))
            .project(expectedSchema)
            .createResolvingReader(PlannedDataReader::create)
            .build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      DataTestHelpers.assertEquals(expectedSchema.asStruct(), expected.get(i), rows.get(i));
    }
  }

  @Test
  public void testMissingRequiredWithoutDefault() {
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .withDoc("Should not produce default value")
                .build());

    Schema expectedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .build(),
            Types.NestedField.required("missing_str")
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
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .withDoc("Should not produce default value")
                .build());

    Schema expectedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .build(),
            Types.NestedField.required("missing_str")
                .withId(6)
                .ofType(Types.StringType.get())
                .withInitialDefault("orange")
                .build(),
            Types.NestedField.optional("missing_int")
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
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .withDoc("Should not produce default value")
                .build());

    Schema expectedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .build(),
            Types.NestedField.optional("missing_date")
                .withId(3)
                .ofType(Types.DateType.get())
                .build());

    writeAndValidate(writeSchema, expectedSchema);
  }

  @Test
  public void testNestedDefaultValue() throws IOException {
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .withDoc("Should not produce default value")
                .build(),
            Types.NestedField.optional("nested")
                .withId(3)
                .ofType(Types.StructType.of(required(4, "inner", Types.StringType.get())))
                .withDoc("Used to test nested field defaults")
                .build());

    Schema expectedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .build(),
            Types.NestedField.optional("nested")
                .withId(3)
                .ofType(
                    Types.StructType.of(
                        required(4, "inner", Types.StringType.get()),
                        Types.NestedField.optional("missing_inner_float")
                            .withId(5)
                            .ofType(Types.FloatType.get())
                            .withInitialDefault(-0.0F)
                            .build()))
                .withDoc("Used to test nested field defaults")
                .build());

    writeAndValidate(writeSchema, expectedSchema);
  }
}
