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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FixedType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.types.Types.UUIDType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSchemaUnionByFieldName {

  private static List<? extends PrimitiveType> primitiveTypes() {
    return Lists.newArrayList(
        StringType.get(),
        TimeType.get(),
        TimestampType.withoutZone(),
        TimestampType.withZone(),
        UUIDType.get(),
        DateType.get(),
        BooleanType.get(),
        BinaryType.get(),
        DoubleType.get(),
        IntegerType.get(),
        FixedType.ofLength(10),
        DecimalType.of(10, 2),
        LongType.get(),
        FloatType.get());
  }

  private static NestedField[] primitiveFields(
      Integer initialValue, List<? extends PrimitiveType> primitiveTypes) {
    AtomicInteger atomicInteger = new AtomicInteger(initialValue);
    return primitiveTypes.stream()
        .map(
            type ->
                optional(
                    atomicInteger.incrementAndGet(),
                    type.toString(),
                    Types.fromPrimitiveString(type.toString())))
        .toArray(NestedField[]::new);
  }

  @Test
  public void testAddTopLevelPrimitives() {
    Schema newSchema = new Schema(primitiveFields(0, primitiveTypes()));
    Schema applied = new SchemaUpdate(new Schema(), 0).unionByNameWith(newSchema).apply();
    Assertions.assertThat(applied.asStruct()).isEqualTo(newSchema.asStruct());
  }

  @Test
  public void testAddTopLevelListOfPrimitives() {
    for (PrimitiveType primitiveType : primitiveTypes()) {
      Schema newSchema =
          new Schema(optional(1, "aList", Types.ListType.ofOptional(2, primitiveType)));
      Schema applied = new SchemaUpdate(new Schema(), 0).unionByNameWith(newSchema).apply();
      Assertions.assertThat(applied.asStruct()).isEqualTo(newSchema.asStruct());
    }
  }

  @Test
  public void testAddTopLevelMapOfPrimitives() {
    for (PrimitiveType primitiveType : primitiveTypes()) {
      Schema newSchema =
          new Schema(
              optional(1, "aMap", Types.MapType.ofOptional(2, 3, primitiveType, primitiveType)));
      Schema applied = new SchemaUpdate(new Schema(), 0).unionByNameWith(newSchema).apply();
      Assertions.assertThat(applied.asStruct()).isEqualTo(newSchema.asStruct());
    }
  }

  @Test
  public void testAddTopLevelStructOfPrimitives() {
    for (PrimitiveType primitiveType : primitiveTypes()) {
      Schema currentSchema =
          new Schema(
              optional(1, "aStruct", Types.StructType.of(optional(2, "primitive", primitiveType))));
      Schema applied = new SchemaUpdate(new Schema(), 0).unionByNameWith(currentSchema).apply();
      Assertions.assertThat(applied.asStruct()).isEqualTo(currentSchema.asStruct());
    }
  }

  @Test
  public void testAddNestedPrimitive() {
    for (PrimitiveType primitiveType : primitiveTypes()) {
      Schema currentSchema = new Schema(optional(1, "aStruct", Types.StructType.of()));
      Schema newSchema =
          new Schema(
              optional(1, "aStruct", Types.StructType.of(optional(2, "primitive", primitiveType))));
      Schema applied = new SchemaUpdate(currentSchema, 1).unionByNameWith(newSchema).apply();
      Assertions.assertThat(applied.asStruct()).isEqualTo(newSchema.asStruct());
    }
  }

  @Test
  public void testAddNestedPrimitives() {
    Schema currentSchema = new Schema(optional(1, "aStruct", Types.StructType.of()));
    Schema newSchema =
        new Schema(
            optional(1, "aStruct", Types.StructType.of(primitiveFields(1, primitiveTypes()))));
    Schema applied = new SchemaUpdate(currentSchema, 1).unionByNameWith(newSchema).apply();
    Assertions.assertThat(applied.asStruct()).isEqualTo(newSchema.asStruct());
  }

  @Test
  public void testAddNestedLists() {
    Schema newSchema =
        new Schema(
            optional(
                1,
                "aList",
                Types.ListType.ofOptional(
                    2,
                    Types.ListType.ofOptional(
                        3,
                        Types.ListType.ofOptional(
                            4,
                            Types.ListType.ofOptional(
                                5,
                                Types.ListType.ofOptional(
                                    6,
                                    Types.ListType.ofOptional(
                                        7,
                                        Types.ListType.ofOptional(
                                            8,
                                            Types.ListType.ofOptional(
                                                9,
                                                Types.ListType.ofOptional(
                                                    10, DecimalType.of(11, 20))))))))))));
    Schema applied = new SchemaUpdate(new Schema(), 0).unionByNameWith(newSchema).apply();
    Assertions.assertThat(applied.asStruct()).isEqualTo(newSchema.asStruct());
  }

  @Test
  public void testAddNestedStruct() {
    Schema newSchema =
        new Schema(
            optional(
                1,
                "struct1",
                Types.StructType.of(
                    optional(
                        2,
                        "struct2",
                        Types.StructType.of(
                            optional(
                                3,
                                "struct3",
                                Types.StructType.of(
                                    optional(
                                        4,
                                        "struct4",
                                        Types.StructType.of(
                                            optional(
                                                5,
                                                "struct5",
                                                Types.StructType.of(
                                                    optional(
                                                        6,
                                                        "struct6",
                                                        Types.StructType.of(
                                                            optional(
                                                                7,
                                                                "aString",
                                                                StringType.get()))))))))))))));
    Schema applied = new SchemaUpdate(new Schema(), 0).unionByNameWith(newSchema).apply();
    Assertions.assertThat(applied.asStruct()).isEqualTo(newSchema.asStruct());
  }

  @Test
  public void testAddNestedMaps() {
    Schema newSchema =
        new Schema(
            optional(
                1,
                "struct",
                Types.MapType.ofOptional(
                    2,
                    3,
                    StringType.get(),
                    Types.MapType.ofOptional(
                        4,
                        5,
                        StringType.get(),
                        Types.MapType.ofOptional(
                            6,
                            7,
                            StringType.get(),
                            Types.MapType.ofOptional(
                                8,
                                9,
                                StringType.get(),
                                Types.MapType.ofOptional(
                                    10,
                                    11,
                                    StringType.get(),
                                    Types.MapType.ofOptional(
                                        12, 13, StringType.get(), StringType.get()))))))));
    Schema applied = new SchemaUpdate(new Schema(), 0).unionByNameWith(newSchema).apply();
    Assertions.assertThat(applied.asStruct()).isEqualTo(newSchema.asStruct());
  }

  @Test
  public void testDetectInvalidTopLevelList() {
    Schema currentSchema =
        new Schema(optional(1, "aList", Types.ListType.ofOptional(2, StringType.get())));
    Schema newSchema =
        new Schema(optional(1, "aList", Types.ListType.ofOptional(2, LongType.get())));
    Assertions.assertThatThrownBy(
            () -> new SchemaUpdate(currentSchema, 2).unionByNameWith(newSchema).apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot change column type: aList.element: string -> long");
  }

  @Test
  public void testDetectInvalidTopLevelMapValue() {

    Schema currentSchema =
        new Schema(
            optional(
                1, "aMap", Types.MapType.ofOptional(2, 3, StringType.get(), StringType.get())));
    Schema newSchema =
        new Schema(
            optional(1, "aMap", Types.MapType.ofOptional(2, 3, StringType.get(), LongType.get())));

    Assertions.assertThatThrownBy(
            () -> new SchemaUpdate(currentSchema, 3).unionByNameWith(newSchema).apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot change column type: aMap.value: string -> long");
  }

  @Test
  public void testDetectInvalidTopLevelMapKey() {
    Schema currentSchema =
        new Schema(
            optional(
                1, "aMap", Types.MapType.ofOptional(2, 3, StringType.get(), StringType.get())));
    Schema newSchema =
        new Schema(
            optional(1, "aMap", Types.MapType.ofOptional(2, 3, UUIDType.get(), StringType.get())));
    Assertions.assertThatThrownBy(
            () -> new SchemaUpdate(currentSchema, 3).unionByNameWith(newSchema).apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot change column type: aMap.key: string -> uuid");
  }

  @Test
  // int 32-bit signed integers -> Can promote to long
  public void testTypePromoteIntegerToLong() {
    Schema currentSchema = new Schema(required(1, "aCol", IntegerType.get()));
    Schema newSchema = new Schema(required(1, "aCol", LongType.get()));

    Schema applied = new SchemaUpdate(currentSchema, 1).unionByNameWith(newSchema).apply();
    Assertions.assertThat(applied.asStruct().fields()).hasSize(1);
    Assertions.assertThat(applied.asStruct().fields().get(0).type()).isEqualTo(LongType.get());
  }

  @Test
  // float 32-bit IEEE 754 floating point -> Can promote to double
  public void testTypePromoteFloatToDouble() {
    Schema currentSchema = new Schema(required(1, "aCol", FloatType.get()));
    Schema newSchema = new Schema(required(1, "aCol", DoubleType.get()));

    Schema applied = new SchemaUpdate(currentSchema, 1).unionByNameWith(newSchema).apply();
    Assertions.assertThat(applied.asStruct()).isEqualTo(newSchema.asStruct());
    Assertions.assertThat(applied.asStruct().fields()).hasSize(1);
    Assertions.assertThat(applied.asStruct().fields().get(0).type()).isEqualTo(DoubleType.get());
  }

  @Test
  public void testInvalidTypePromoteDoubleToFloat() {
    Schema currentSchema = new Schema(required(1, "aCol", DoubleType.get()));
    Schema newSchema = new Schema(required(1, "aCol", FloatType.get()));

    Assertions.assertThatThrownBy(
            () -> new SchemaUpdate(currentSchema, 1).unionByNameWith(newSchema).apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot change column type: aCol: double -> float");
  }

  @Test
  // decimal(P,S) Fixed-point decimal; precision P, scale S -> Scale is fixed [1], precision must be
  // 38 or less
  public void testTypePromoteDecimalToFixedScaleWithWiderPrecision() {
    Schema currentSchema = new Schema(required(1, "aCol", DecimalType.of(20, 1)));
    Schema newSchema = new Schema(required(1, "aCol", DecimalType.of(22, 1)));

    Schema applied = new SchemaUpdate(currentSchema, 1).unionByNameWith(newSchema).apply();
    Assertions.assertThat(applied.asStruct()).isEqualTo(newSchema.asStruct());
  }

  @Test
  public void testAddPrimitiveToNestedStruct() {
    Schema schema =
        new Schema(
            required(
                1,
                "struct1",
                Types.StructType.of(
                    optional(
                        2,
                        "struct2",
                        Types.StructType.of(
                            optional(
                                3,
                                "list",
                                Types.ListType.ofOptional(
                                    4,
                                    Types.StructType.of(
                                        optional(5, "value", StringType.get())))))))));

    Schema newSchema =
        new Schema(
            required(
                1,
                "struct1",
                Types.StructType.of(
                    optional(
                        2,
                        "struct2",
                        Types.StructType.of(
                            optional(
                                3,
                                "list",
                                Types.ListType.ofOptional(
                                    4,
                                    Types.StructType.of(optional(5, "time", TimeType.get())))))))));

    Schema applied = new SchemaUpdate(schema, 5).unionByNameWith(newSchema).apply();

    Schema expected =
        new Schema(
            required(
                1,
                "struct1",
                Types.StructType.of(
                    optional(
                        2,
                        "struct2",
                        Types.StructType.of(
                            optional(
                                3,
                                "list",
                                Types.ListType.ofOptional(
                                    4,
                                    Types.StructType.of(
                                        optional(5, "value", StringType.get()),
                                        optional(6, "time", TimeType.get())))))))));

    Assertions.assertThat(applied.asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  public void testReplaceListWithPrimitive() {
    Schema currentSchema =
        new Schema(optional(1, "aColumn", Types.ListType.ofOptional(2, StringType.get())));
    Schema newSchema = new Schema(optional(1, "aColumn", StringType.get()));
    Assertions.assertThatThrownBy(
            () -> new SchemaUpdate(currentSchema, 2).unionByNameWith(newSchema).apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot change column type: aColumn: list<string> -> string");
  }

  @Test
  public void testMirroredSchemas() {
    Schema aSchema =
        new Schema(
            optional(9, "struct1", Types.StructType.of(optional(8, "string1", StringType.get()))),
            optional(6, "list1", Types.ListType.ofOptional(7, StringType.get())),
            optional(5, "string2", StringType.get()),
            optional(4, "string3", StringType.get()),
            optional(3, "string4", StringType.get()),
            optional(2, "string5", StringType.get()),
            optional(1, "string6", StringType.get()));

    // Same schema but the field indices are in reverse order, from lowest to highest
    Schema mirrored =
        new Schema(
            optional(1, "struct1", Types.StructType.of(optional(2, "string1", StringType.get()))),
            optional(3, "list1", Types.ListType.ofOptional(4, StringType.get())),
            optional(5, "string2", StringType.get()),
            optional(6, "string3", StringType.get()),
            optional(7, "string4", StringType.get()),
            optional(8, "string5", StringType.get()),
            optional(9, "string6", StringType.get()));

    Schema union = new SchemaUpdate(aSchema, 0).unionByNameWith(mirrored).apply();
    // We don't expect the original schema to have been altered.
    Assertions.assertThat(union.asStruct()).isEqualTo(aSchema.asStruct());
  }

  @Test
  public void addNewTopLevelStruct() {
    Schema schema =
        new Schema(
            optional(
                1,
                "map1",
                Types.MapType.ofOptional(
                    2,
                    3,
                    Types.StringType.get(),
                    Types.ListType.ofOptional(
                        4, Types.StructType.of(optional(5, "string1", Types.StringType.get()))))));

    Schema observed =
        new Schema(
            optional(
                1,
                "map1",
                Types.MapType.ofOptional(
                    2,
                    3,
                    Types.StringType.get(),
                    Types.ListType.ofOptional(
                        4, Types.StructType.of(optional(5, "string1", Types.StringType.get()))))),
            optional(
                6,
                "struct1",
                Types.StructType.of(
                    optional(
                        7, "d1", Types.StructType.of(optional(8, "d2", Types.StringType.get()))))));

    Schema union = new SchemaUpdate(schema, 5).unionByNameWith(observed).apply();
    Assertions.assertThat(union.asStruct()).isEqualTo(observed.asStruct());
  }

  @Test
  public void testAppendNestedStruct() {
    Schema schema =
        new Schema(
            required(
                1,
                "s1",
                StructType.of(
                    optional(
                        2,
                        "s2",
                        StructType.of(
                            optional(
                                3, "s3", StructType.of(optional(4, "s4", StringType.get()))))))));

    Schema observed =
        new Schema(
            required(
                1,
                "s1",
                StructType.of(
                    optional(
                        2,
                        "s2",
                        StructType.of(
                            optional(3, "s3", StructType.of(optional(4, "s4", StringType.get()))),
                            optional(
                                5,
                                "repeat",
                                StructType.of(
                                    optional(
                                        6,
                                        "s1",
                                        StructType.of(
                                            optional(
                                                7,
                                                "s2",
                                                StructType.of(
                                                    optional(
                                                        8,
                                                        "s3",
                                                        StructType.of(
                                                            optional(
                                                                9,
                                                                "s4",
                                                                StringType.get()))))))))))))));

    Schema applied = new SchemaUpdate(schema, 4).unionByNameWith(observed).apply();
    Assertions.assertThat(applied.asStruct()).isEqualTo(observed.asStruct());
  }

  @Test
  public void testAppendNestedLists() {
    Schema schema =
        new Schema(
            required(
                1,
                "s1",
                StructType.of(
                    optional(
                        2,
                        "s2",
                        StructType.of(
                            optional(
                                3,
                                "s3",
                                StructType.of(
                                    optional(
                                        4,
                                        "list1",
                                        ListType.ofOptional(5, StringType.get())))))))));

    Schema observed =
        new Schema(
            required(
                1,
                "s1",
                StructType.of(
                    optional(
                        2,
                        "s2",
                        StructType.of(
                            optional(
                                3,
                                "s3",
                                StructType.of(
                                    optional(
                                        4,
                                        "list2",
                                        ListType.ofOptional(5, StringType.get())))))))));

    Schema union = new SchemaUpdate(schema, 5).unionByNameWith(observed).apply();

    Schema expected =
        new Schema(
            required(
                1,
                "s1",
                StructType.of(
                    optional(
                        2,
                        "s2",
                        StructType.of(
                            optional(
                                3,
                                "s3",
                                StructType.of(
                                    optional(4, "list1", ListType.ofOptional(5, StringType.get())),
                                    optional(
                                        6,
                                        "list2",
                                        ListType.ofOptional(7, StringType.get())))))))));

    Assertions.assertThat(union.asStruct()).isEqualTo(expected.asStruct());
  }
}
