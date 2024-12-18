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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;
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
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
    Schema writeSchema = new Schema(required(1, "id", Types.LongType.get()));

    Schema expectedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
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

  @Test
  public void testMapNestedDefaultValue() throws IOException {
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .withDoc("Should not produce default value")
                .build(),
            Types.NestedField.optional("nested_map")
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
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .build(),
            Types.NestedField.optional("nested_map")
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
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault("wrong!")
                .withDoc("Should not produce default value")
                .build(),
            Types.NestedField.optional("nested_list")
                .withId(3)
                .ofType(
                    Types.ListType.ofOptional(
                        4, Types.StructType.of(required(5, "element_str", Types.StringType.get()))))
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
            Types.NestedField.optional("nested_list")
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

  private static Stream<Arguments> primitiveTypesAndDefaults() {
    return Stream.of(
        Arguments.of(Types.BooleanType.get(), false),
        Arguments.of(Types.IntegerType.get(), 34),
        Arguments.of(Types.LongType.get(), 4900000000L),
        Arguments.of(Types.FloatType.get(), 12.21F),
        Arguments.of(Types.DoubleType.get(), -0.0D),
        Arguments.of(Types.DateType.get(), DateTimeUtil.isoDateToDays("2024-12-17")),
        Arguments.of(Types.TimeType.get(), DateTimeUtil.isoTimeToMicros("23:59:59.999999")),
        Arguments.of(
            Types.TimestampType.withZone(),
            DateTimeUtil.isoTimestamptzToMicros("2024-12-17T23:59:59.999999+00:00")),
        Arguments.of(
            Types.TimestampType.withoutZone(),
            DateTimeUtil.isoTimestampToMicros("2024-12-17T23:59:59.999999")),
        Arguments.of(Types.StringType.get(), "iceberg"),
        Arguments.of(
            Types.FixedType.ofLength(4), ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0x0c, 0x0d})),
        Arguments.of(Types.BinaryType.get(), ByteBuffer.wrap(new byte[] {0x0a, 0x0b})),
        Arguments.of(Types.DecimalType.of(9, 2), new BigDecimal("12.34")));
  }

  @ParameterizedTest
  @MethodSource("primitiveTypesAndDefaults")
  public void testPrimitiveTypeDefaultValues(Type.PrimitiveType type, Object defaultValue)
      throws IOException {
    Schema writeSchema = new Schema(required(1, "id", Types.LongType.get()));

    Schema readSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("col_with_default")
                .withId(2)
                .ofType(type)
                .withInitialDefault(defaultValue)
                .build());

    writeAndValidate(writeSchema, readSchema);
  }
}
