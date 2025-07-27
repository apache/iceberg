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
package org.apache.iceberg.data;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.EdgeAlgorithm;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;

public abstract class DataTestBase {

  private static final long FIRST_ROW_ID = 2_000L;
  protected static final Map<Integer, Object> ID_TO_CONSTANT =
      Map.of(
          MetadataColumns.ROW_ID.fieldId(),
          FIRST_ROW_ID,
          MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(),
          34L);

  protected abstract void writeAndValidate(Schema schema) throws IOException;

  protected void writeAndValidate(Schema schema, List<Record> data) throws IOException {
    throw new UnsupportedEncodingException(
        "Cannot run test, writeAndValidate(Schema, List<Record>) is not implemented");
  }

  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema) throws IOException {
    throw new UnsupportedEncodingException(
        "Cannot run test, writeAndValidate(Schema, Schema) is not implemented");
  }

  protected boolean supportsDefaultValues() {
    return false;
  }

  protected boolean allowsWritingNullValuesForRequiredFields() {
    return false;
  }

  protected static final StructType SUPPORTED_PRIMITIVES =
      StructType.of(
          required(100, "id", LongType.get()),
          optional(101, "data", Types.StringType.get()),
          required(102, "b", Types.BooleanType.get()),
          optional(103, "i", Types.IntegerType.get()),
          required(104, "l", LongType.get()),
          optional(105, "f", Types.FloatType.get()),
          required(106, "d", Types.DoubleType.get()),
          optional(107, "date", Types.DateType.get()),
          required(108, "ts_tz", Types.TimestampType.withZone()),
          required(109, "ts", Types.TimestampType.withoutZone()),
          required(110, "s", Types.StringType.get()),
          required(112, "fixed", Types.FixedType.ofLength(7)),
          optional(113, "bytes", Types.BinaryType.get()),
          required(114, "dec_9_0", Types.DecimalType.of(9, 0)),
          required(115, "dec_11_2", Types.DecimalType.of(11, 2)),
          required(116, "dec_38_10", Types.DecimalType.of(38, 10)), // maximum precision
          required(117, "time", Types.TimeType.get()));

  @TempDir protected Path temp;

  private static final Type[] SIMPLE_TYPES =
      new Type[] {
        Types.UnknownType.get(),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withZone(),
        Types.TimestampType.withoutZone(),
        Types.TimestampNanoType.withZone(),
        Types.TimestampNanoType.withoutZone(),
        Types.StringType.get(),
        Types.FixedType.ofLength(7),
        Types.BinaryType.get(),
        Types.DecimalType.of(9, 0),
        Types.DecimalType.of(11, 2),
        Types.DecimalType.of(38, 10),
        Types.VariantType.get(),
        Types.GeometryType.crs84(),
        Types.GeometryType.of("srid:3857"),
        Types.GeographyType.crs84(),
        Types.GeographyType.of("srid:4269"),
        Types.GeographyType.of("srid:4269", EdgeAlgorithm.KARNEY),
      };

  protected boolean supportsUnknown() {
    return false;
  }

  protected boolean supportsTimestampNanos() {
    return false;
  }

  protected boolean supportsVariant() {
    return false;
  }

  protected boolean supportsGeospatial() {
    return false;
  }

  protected boolean supportsRowLineage() {
    return false;
  }

  @ParameterizedTest
  @FieldSource("SIMPLE_TYPES")
  public void testTypeSchema(Type type) throws IOException {
    assumeThat(
            supportsUnknown()
                || TypeUtil.find(type, t -> t.typeId() == Type.TypeID.UNKNOWN) == null)
        .as("unknown is not yet implemented")
        .isTrue();
    assumeThat(
            supportsTimestampNanos()
                || TypeUtil.find(type, t -> t.typeId() == Type.TypeID.TIMESTAMP_NANO) == null)
        .as("timestamp_ns is not yet implemented")
        .isTrue();
    assumeThat(
            supportsVariant()
                || TypeUtil.find(type, t -> t.typeId() == Type.TypeID.VARIANT) == null)
        .as("variant is not yet implemented")
        .isTrue();
    if (!supportsGeospatial()) {
      assumeThat(TypeUtil.find(type, t -> t.typeId() == Type.TypeID.GEOMETRY) == null)
          .as("geometry is not yet implemented")
          .isTrue();
      assumeThat(TypeUtil.find(type, t -> t.typeId() == Type.TypeID.GEOGRAPHY) == null)
          .as("geography is not yet implemented")
          .isTrue();
    }

    writeAndValidate(
        new Schema(
            required(1, "id", LongType.get()),
            optional(2, "test_type", type),
            required(3, "trailing_data", Types.StringType.get())));
  }

  @Test
  public void testSimpleStruct() throws IOException {
    writeAndValidate(new Schema(SUPPORTED_PRIMITIVES.fields()));
  }

  @Test
  public void testArray() throws IOException {
    Schema schema =
        new Schema(
            required(0, "id", LongType.get()),
            optional(1, "data", ListType.ofOptional(2, Types.StringType.get())));

    writeAndValidate(schema);
  }

  @Test
  public void testArrayOfStructs() throws IOException {
    Schema schema =
        new Schema(
            required(0, "id", LongType.get()),
            optional(1, "data", ListType.ofOptional(2, SUPPORTED_PRIMITIVES)));

    writeAndValidate(schema);
  }

  @Test
  public void testMap() throws IOException {
    Schema schema =
        new Schema(
            required(0, "id", LongType.get()),
            optional(
                1,
                "data",
                MapType.ofOptional(2, 3, Types.StringType.get(), Types.StringType.get())));

    writeAndValidate(schema);
  }

  @Test
  public void testNumericMapKey() throws IOException {
    Schema schema =
        new Schema(
            required(0, "id", LongType.get()),
            optional(1, "data", MapType.ofOptional(2, 3, LongType.get(), Types.StringType.get())));

    writeAndValidate(schema);
  }

  @Test
  public void testComplexMapKey() throws IOException {
    Schema schema =
        new Schema(
            required(0, "id", LongType.get()),
            optional(
                1,
                "data",
                MapType.ofOptional(
                    2,
                    3,
                    StructType.of(
                        required(4, "i", Types.IntegerType.get()),
                        optional(5, "s", Types.StringType.get())),
                    Types.StringType.get())));

    writeAndValidate(schema);
  }

  @Test
  public void testMapOfStructs() throws IOException {
    Schema schema =
        new Schema(
            required(0, "id", LongType.get()),
            optional(
                1, "data", MapType.ofOptional(2, 3, Types.StringType.get(), SUPPORTED_PRIMITIVES)));

    writeAndValidate(schema);
  }

  @Test
  public void testMixedTypes() throws IOException {
    StructType structType =
        StructType.of(
            required(0, "id", LongType.get()),
            optional(
                1,
                "list_of_maps",
                ListType.ofOptional(
                    2, MapType.ofOptional(3, 4, Types.StringType.get(), SUPPORTED_PRIMITIVES))),
            optional(
                5,
                "map_of_lists",
                MapType.ofOptional(
                    6, 7, Types.StringType.get(), ListType.ofOptional(8, SUPPORTED_PRIMITIVES))),
            required(
                9,
                "list_of_lists",
                ListType.ofOptional(10, ListType.ofOptional(11, SUPPORTED_PRIMITIVES))),
            required(
                12,
                "map_of_maps",
                MapType.ofOptional(
                    13,
                    14,
                    Types.StringType.get(),
                    MapType.ofOptional(15, 16, Types.StringType.get(), SUPPORTED_PRIMITIVES))),
            required(
                17,
                "list_of_struct_of_nested_types",
                ListType.ofOptional(
                    19,
                    StructType.of(
                        Types.NestedField.required(
                            20,
                            "m1",
                            MapType.ofOptional(
                                21, 22, Types.StringType.get(), SUPPORTED_PRIMITIVES)),
                        Types.NestedField.optional(
                            23, "l1", ListType.ofRequired(24, SUPPORTED_PRIMITIVES)),
                        Types.NestedField.required(
                            25, "l2", ListType.ofRequired(26, SUPPORTED_PRIMITIVES)),
                        Types.NestedField.optional(
                            27,
                            "m2",
                            MapType.ofOptional(
                                28, 29, Types.StringType.get(), SUPPORTED_PRIMITIVES))))));

    Schema schema =
        new Schema(
            TypeUtil.assignFreshIds(structType, new AtomicInteger(0)::incrementAndGet)
                .asStructType()
                .fields());

    writeAndValidate(schema);
  }

  @Test
  public void testMissingRequiredWithoutDefault() {
    assumeThat(supportsDefaultValues()).isTrue();

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
    assumeThat(supportsDefaultValues()).isTrue();

    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault(Literal.of("wrong!"))
                .withDoc("Should not produce default value")
                .build());

    Schema expectedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault(Literal.of("wrong!"))
                .build(),
            Types.NestedField.required("missing_str")
                .withId(6)
                .ofType(Types.StringType.get())
                .withInitialDefault(Literal.of("orange"))
                .build(),
            Types.NestedField.optional("missing_int")
                .withId(7)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(Literal.of(34))
                .build());

    writeAndValidate(writeSchema, expectedSchema);
  }

  @Test
  public void testNullDefaultValue() throws IOException {
    assumeThat(supportsDefaultValues()).isTrue();

    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault(Literal.of("wrong!"))
                .withDoc("Should not produce default value")
                .build());

    Schema expectedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault(Literal.of("wrong!"))
                .build(),
            Types.NestedField.optional("missing_date")
                .withId(3)
                .ofType(Types.DateType.get())
                .build());

    writeAndValidate(writeSchema, expectedSchema);
  }

  @Test
  public void testNestedDefaultValue() throws IOException {
    assumeThat(supportsDefaultValues()).isTrue();

    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault(Literal.of("wrong!"))
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
                .withInitialDefault(Literal.of("wrong!"))
                .build(),
            Types.NestedField.optional("nested")
                .withId(3)
                .ofType(
                    Types.StructType.of(
                        required(4, "inner", Types.StringType.get()),
                        Types.NestedField.optional("missing_inner_float")
                            .withId(5)
                            .ofType(Types.FloatType.get())
                            .withInitialDefault(Literal.of(-0.0F))
                            .build()))
                .withDoc("Used to test nested field defaults")
                .build());

    writeAndValidate(writeSchema, expectedSchema);
  }

  @Test
  public void testMapNestedDefaultValue() throws IOException {
    assumeThat(supportsDefaultValues()).isTrue();

    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault(Literal.of("wrong!"))
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
                .withInitialDefault(Literal.of("wrong!"))
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
                                .withInitialDefault(Literal.of(34))
                                .build())))
                .withDoc("Used to test nested field defaults")
                .build());

    writeAndValidate(writeSchema, expectedSchema);
  }

  @Test
  public void testListNestedDefaultValue() throws IOException {
    assumeThat(supportsDefaultValues()).isTrue();

    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault(Literal.of("wrong!"))
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
                .withInitialDefault(Literal.of("wrong!"))
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
                                .withInitialDefault(Literal.of(34))
                                .build())))
                .withDoc("Used to test nested field defaults")
                .build());

    writeAndValidate(writeSchema, expectedSchema);
  }

  private static Stream<Arguments> primitiveTypesAndDefaults() {
    return Stream.of(
        Arguments.of(Types.BooleanType.get(), Literal.of(false)),
        Arguments.of(Types.IntegerType.get(), Literal.of(34)),
        Arguments.of(Types.LongType.get(), Literal.of(4900000000L)),
        Arguments.of(Types.FloatType.get(), Literal.of(12.21F)),
        Arguments.of(Types.DoubleType.get(), Literal.of(-0.0D)),
        Arguments.of(Types.DateType.get(), Literal.of(DateTimeUtil.isoDateToDays("2024-12-17"))),
        // Arguments.of(Types.TimeType.get(), DateTimeUtil.isoTimeToMicros("23:59:59.999999")),
        Arguments.of(
            Types.TimestampType.withZone(),
            Literal.of(DateTimeUtil.isoTimestamptzToMicros("2024-12-17T23:59:59.999999+00:00"))),
        Arguments.of(
            Types.TimestampType.withoutZone(),
            Literal.of(DateTimeUtil.isoTimestampToMicros("2024-12-17T23:59:59.999999"))),
        Arguments.of(Types.StringType.get(), Literal.of("iceberg")),
        Arguments.of(Types.UUIDType.get(), Literal.of(UUID.randomUUID())),
        Arguments.of(
            Types.FixedType.ofLength(4),
            Literal.of(ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0x0c, 0x0d}))),
        Arguments.of(Types.BinaryType.get(), Literal.of(ByteBuffer.wrap(new byte[] {0x0a, 0x0b}))),
        Arguments.of(Types.DecimalType.of(9, 2), Literal.of(new BigDecimal("12.34"))));
  }

  @ParameterizedTest
  @MethodSource("primitiveTypesAndDefaults")
  public void testPrimitiveTypeDefaultValues(Type.PrimitiveType type, Literal<?> defaultValue)
      throws IOException {
    assumeThat(supportsDefaultValues()).isTrue();

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

  @Test
  public void testWriteNullValueForRequiredType() throws Exception {
    Schema schema =
        new Schema(
            required(0, "id", LongType.get()), required(1, "string", Types.StringType.get()));

    GenericRecord genericRecord = GenericRecord.create(schema);
    genericRecord.set(0, 42L);
    genericRecord.set(1, null);

    if (allowsWritingNullValuesForRequiredFields()) {
      writeAndValidate(schema, ImmutableList.of(genericRecord));
    } else {
      assertThatThrownBy(
          // The actual exception depends on the implementation, e.g.
          // NullPointerException or IllegalArgumentException.
          () -> writeAndValidate(schema, ImmutableList.of(genericRecord)));
    }
  }

  @Test
  public void testRowLineage() throws Exception {
    assumeThat(supportsRowLineage()).as("Row Lineage support is not implemented").isTrue();

    Schema schema =
        new Schema(
            required(1, "id", LongType.get()),
            required(2, "data", Types.StringType.get()),
            MetadataColumns.ROW_ID,
            MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);

    GenericRecord record = GenericRecord.create(schema);

    writeAndValidate(
        schema,
        List.of(
            record.copy(Map.of("id", 1L, "data", "a")),
            record.copy(Map.of("id", 2L, "data", "b")),
            record.copy(
                Map.of(
                    "id",
                    3L,
                    "data",
                    "c",
                    "_row_id",
                    1_000L,
                    "_last_updated_sequence_number",
                    33L)),
            record.copy(Map.of("id", 4L, "data", "d", "_row_id", 1_001L)),
            record.copy(Map.of("id", 5L, "data", "e"))));
  }
}
