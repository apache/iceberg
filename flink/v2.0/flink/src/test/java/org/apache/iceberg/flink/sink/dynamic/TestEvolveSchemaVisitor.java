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
package org.apache.iceberg.flink.sink.dynamic;

import static org.apache.iceberg.types.Types.NestedField.of;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Schema;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.UUIDType;
import org.junit.jupiter.api.Test;

public class TestEvolveSchemaVisitor {

  private static final TableIdentifier TABLE = TableIdentifier.of("table");
  private static final boolean DROP_COLUMNS = true;
  private static final boolean PRESERVE_COLUMNS = false;

  private static List<? extends PrimitiveType> primitiveTypes() {
    return Lists.newArrayList(
        StringType.get(),
        TimeType.get(),
        Types.TimestampType.withoutZone(),
        Types.TimestampType.withZone(),
        UUIDType.get(),
        Types.DateType.get(),
        Types.BooleanType.get(),
        Types.BinaryType.get(),
        DoubleType.get(),
        IntegerType.get(),
        Types.FixedType.ofLength(10),
        DecimalType.of(10, 2),
        LongType.get(),
        FloatType.get());
  }

  private static Types.NestedField[] primitiveFields(
      Integer initialValue, List<? extends PrimitiveType> primitiveTypes) {
    return primitiveFields(initialValue, primitiveTypes, true);
  }

  private static Types.NestedField[] primitiveFields(
      Integer initialValue, List<? extends PrimitiveType> primitiveTypes, boolean optional) {
    AtomicInteger atomicInteger = new AtomicInteger(initialValue);
    return primitiveTypes.stream()
        .map(
            type ->
                of(
                    atomicInteger.incrementAndGet(),
                    optional,
                    type.toString(),
                    Types.fromPrimitiveString(type.toString())))
        .toArray(Types.NestedField[]::new);
  }

  @Test
  public void testAddTopLevelPrimitives() {
    Schema targetSchema = new Schema(primitiveFields(0, primitiveTypes()));
    UpdateSchema updateApi = loadUpdateApi(new Schema());
    EvolveSchemaVisitor.visit(TABLE, updateApi, new Schema(), targetSchema, PRESERVE_COLUMNS);
    assertThat(targetSchema.asStruct()).isEqualTo(updateApi.apply().asStruct());
  }

  @Test
  public void testMakeTopLevelPrimitivesOptional() {
    Schema existingSchema = new Schema(primitiveFields(0, primitiveTypes(), false));
    assertThat(existingSchema.columns().stream().allMatch(Types.NestedField::isRequired)).isTrue();

    UpdateSchema updateApi = loadUpdateApi(existingSchema);
    EvolveSchemaVisitor.visit(TABLE, updateApi, existingSchema, new Schema(), PRESERVE_COLUMNS);
    Schema newSchema = updateApi.apply();
    assertThat(newSchema.asStruct().fields()).hasSize(14);
    assertThat(newSchema.columns().stream().allMatch(Types.NestedField::isOptional)).isTrue();
  }

  @Test
  public void testDropUnusedColumns() {
    Schema existingSchema =
        new Schema(
            optional(1, "a", StringType.get()),
            optional(
                2,
                "b",
                StructType.of(
                    optional(4, "nested1", StringType.get()),
                    optional(5, "nested2", StringType.get()))),
            optional(3, "c", IntegerType.get()));

    Schema targetSchema =
        new Schema(
            optional(1, "a", StringType.get()),
            optional(2, "b", StructType.of(optional(5, "nested2", StringType.get()))));

    UpdateSchema updateApi = loadUpdateApi(existingSchema);
    EvolveSchemaVisitor.visit(TABLE, updateApi, existingSchema, targetSchema, DROP_COLUMNS);

    Schema newSchema = updateApi.apply();
    assertThat(newSchema.sameSchema(targetSchema)).isTrue();
  }

  @Test
  public void testPreserveUnusedColumns() {
    Schema existingSchema =
        new Schema(
            optional(1, "a", StringType.get()),
            optional(
                2,
                "b",
                StructType.of(
                    optional(4, "nested1", StringType.get()),
                    optional(5, "nested2", StringType.get()))),
            optional(3, "c", IntegerType.get()));

    Schema targetSchema = new Schema(optional(1, "a", StringType.get()));

    UpdateSchema updateApi = loadUpdateApi(existingSchema);
    EvolveSchemaVisitor.visit(TABLE, updateApi, existingSchema, targetSchema, PRESERVE_COLUMNS);

    Schema newSchema = updateApi.apply();
    assertThat(newSchema.sameSchema(existingSchema)).isTrue();
  }

  @Test
  public void testIdentifyFieldsByName() {
    Schema existingSchema =
        new Schema(Types.NestedField.optional(42, "myField", Types.LongType.get()));
    UpdateSchema updateApi = loadUpdateApi(existingSchema);
    Schema newSchema =
        new Schema(Arrays.asList(Types.NestedField.optional(-1, "myField", Types.LongType.get())));
    EvolveSchemaVisitor.visit(TABLE, updateApi, existingSchema, newSchema, PRESERVE_COLUMNS);
    assertThat(updateApi.apply().sameSchema(existingSchema)).isTrue();
  }

  @Test
  public void testChangeOrderTopLevelPrimitives() {
    Schema existingSchema =
        new Schema(
            Arrays.asList(optional(1, "a", StringType.get()), optional(2, "b", StringType.get())));
    Schema targetSchema =
        new Schema(
            Arrays.asList(optional(2, "b", StringType.get()), optional(1, "a", StringType.get())));
    UpdateSchema updateApi = loadUpdateApi(existingSchema);
    EvolveSchemaVisitor.visit(TABLE, updateApi, existingSchema, targetSchema, PRESERVE_COLUMNS);
    assertThat(updateApi.apply().asStruct()).isEqualTo(targetSchema.asStruct());
  }

  @Test
  public void testAddTopLevelListOfPrimitives() {
    for (PrimitiveType primitiveType : primitiveTypes()) {
      Schema targetSchema = new Schema(optional(1, "aList", ListType.ofOptional(2, primitiveType)));
      UpdateSchema updateApi = loadUpdateApi(new Schema());
      EvolveSchemaVisitor.visit(TABLE, updateApi, new Schema(), targetSchema, PRESERVE_COLUMNS);
      assertThat(updateApi.apply().asStruct()).isEqualTo(targetSchema.asStruct());
    }
  }

  @Test
  public void testMakeTopLevelListOfPrimitivesOptional() {
    for (PrimitiveType primitiveType : primitiveTypes()) {
      Schema existingSchema =
          new Schema(optional(1, "aList", ListType.ofRequired(2, primitiveType)));
      Schema targetSchema = new Schema();
      UpdateSchema updateApi = loadUpdateApi(existingSchema);
      EvolveSchemaVisitor.visit(TABLE, updateApi, existingSchema, targetSchema, PRESERVE_COLUMNS);
      Schema expectedSchema =
          new Schema(optional(1, "aList", ListType.ofRequired(2, primitiveType)));
      assertThat(updateApi.apply().asStruct()).isEqualTo(expectedSchema.asStruct());
    }
  }

  @Test
  public void testAddTopLevelMapOfPrimitives() {
    for (PrimitiveType primitiveType : primitiveTypes()) {
      Schema targetSchema =
          new Schema(optional(1, "aMap", MapType.ofOptional(2, 3, primitiveType, primitiveType)));
      UpdateSchema updateApi = loadUpdateApi(new Schema());
      EvolveSchemaVisitor.visit(TABLE, updateApi, new Schema(), targetSchema, PRESERVE_COLUMNS);
      assertThat(updateApi.apply().asStruct()).isEqualTo(targetSchema.asStruct());
    }
  }

  @Test
  public void testAddTopLevelStructOfPrimitives() {
    for (PrimitiveType primitiveType : primitiveTypes()) {
      Schema currentSchema =
          new Schema(
              optional(1, "aStruct", StructType.of(optional(2, "primitive", primitiveType))));
      UpdateSchema updateApi = loadUpdateApi(new Schema());
      EvolveSchemaVisitor.visit(TABLE, updateApi, new Schema(), currentSchema, PRESERVE_COLUMNS);
      assertThat(updateApi.apply().asStruct()).isEqualTo(currentSchema.asStruct());
    }
  }

  @Test
  public void testAddNestedPrimitive() {
    for (PrimitiveType primitiveType : primitiveTypes()) {
      Schema currentSchema = new Schema(optional(1, "aStruct", StructType.of()));
      Schema targetSchema =
          new Schema(
              optional(1, "aStruct", StructType.of(optional(2, "primitive", primitiveType))));
      UpdateSchema updateApi = loadUpdateApi(currentSchema);
      EvolveSchemaVisitor.visit(TABLE, updateApi, currentSchema, targetSchema, PRESERVE_COLUMNS);
      assertThat(updateApi.apply().asStruct()).isEqualTo(targetSchema.asStruct());
    }
  }

  @Test
  public void testMakeNestedPrimitiveOptional() {
    for (PrimitiveType primitiveType : primitiveTypes()) {
      Schema currentSchema =
          new Schema(
              optional(1, "aStruct", StructType.of(required(2, "primitive", primitiveType))));
      Schema targetSchema =
          new Schema(
              optional(1, "aStruct", StructType.of(optional(2, "primitive", primitiveType))));
      UpdateSchema updateApi = loadUpdateApi(currentSchema);
      EvolveSchemaVisitor.visit(TABLE, updateApi, currentSchema, targetSchema, PRESERVE_COLUMNS);
      assertThat(updateApi.apply().asStruct()).isEqualTo(targetSchema.asStruct());
    }
  }

  @Test
  public void testAddNestedPrimitives() {
    Schema currentSchema = new Schema(optional(1, "aStruct", StructType.of()));
    Schema targetSchema =
        new Schema(optional(1, "aStruct", StructType.of(primitiveFields(1, primitiveTypes()))));
    UpdateSchema updateApi = loadUpdateApi(currentSchema);
    EvolveSchemaVisitor.visit(TABLE, updateApi, currentSchema, targetSchema, PRESERVE_COLUMNS);
    assertThat(updateApi.apply().asStruct()).isEqualTo(targetSchema.asStruct());
  }

  @Test
  public void testAddNestedLists() {
    Schema targetSchema =
        new Schema(
            optional(
                1,
                "aList",
                ListType.ofOptional(
                    2,
                    ListType.ofOptional(
                        3,
                        ListType.ofOptional(
                            4,
                            ListType.ofOptional(
                                5,
                                ListType.ofOptional(
                                    6,
                                    ListType.ofOptional(
                                        7,
                                        ListType.ofOptional(
                                            8,
                                            ListType.ofOptional(
                                                9,
                                                ListType.ofOptional(
                                                    10, DecimalType.of(11, 20))))))))))));
    UpdateSchema updateApi = loadUpdateApi(new Schema());
    EvolveSchemaVisitor.visit(TABLE, updateApi, new Schema(), targetSchema, PRESERVE_COLUMNS);
    assertThat(updateApi.apply().asStruct()).isEqualTo(targetSchema.asStruct());
  }

  @Test
  public void testAddNestedStruct() {
    Schema currentSchema =
        new Schema(optional(1, "struct1", StructType.of(optional(2, "struct2", StructType.of()))));
    Schema targetSchema =
        new Schema(
            optional(
                1,
                "struct1",
                StructType.of(
                    optional(
                        2,
                        "struct2",
                        StructType.of(
                            optional(
                                3,
                                "struct3",
                                StructType.of(
                                    optional(
                                        4,
                                        "struct4",
                                        StructType.of(
                                            optional(
                                                5,
                                                "struct5",
                                                StructType.of(
                                                    optional(
                                                        6,
                                                        "struct6",
                                                        StructType.of(
                                                            optional(
                                                                7,
                                                                "aString",
                                                                StringType.get()))))))))))))));
    UpdateSchema updateApi = loadUpdateApi(currentSchema);
    EvolveSchemaVisitor.visit(TABLE, updateApi, currentSchema, targetSchema, PRESERVE_COLUMNS);
    assertThat(updateApi.apply().asStruct()).isEqualTo(targetSchema.asStruct());
  }

  @Test
  public void testAddNestedMaps() {
    Schema targetSchema =
        new Schema(
            optional(
                1,
                "struct",
                MapType.ofOptional(
                    2,
                    3,
                    StringType.get(),
                    MapType.ofOptional(
                        4,
                        5,
                        StringType.get(),
                        MapType.ofOptional(
                            6,
                            7,
                            StringType.get(),
                            MapType.ofOptional(
                                8,
                                9,
                                StringType.get(),
                                MapType.ofOptional(
                                    10,
                                    11,
                                    StringType.get(),
                                    MapType.ofOptional(
                                        12, 13, StringType.get(), StringType.get()))))))));

    UpdateSchema updateApi = loadUpdateApi(new Schema());
    EvolveSchemaVisitor.visit(TABLE, updateApi, new Schema(), targetSchema, PRESERVE_COLUMNS);
    assertThat(updateApi.apply().asStruct()).isEqualTo(targetSchema.asStruct());
  }

  @Test
  public void testDetectInvalidTopLevelList() {
    Schema currentSchema =
        new Schema(optional(1, "aList", ListType.ofOptional(2, StringType.get())));
    Schema targetSchema = new Schema(optional(1, "aList", ListType.ofOptional(2, LongType.get())));
    assertThatThrownBy(
            () ->
                EvolveSchemaVisitor.visit(
                    TABLE,
                    loadUpdateApi(currentSchema),
                    currentSchema,
                    targetSchema,
                    PRESERVE_COLUMNS))
        .hasMessage("Cannot change column type: aList.element: string -> long")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testDetectInvalidTopLevelMapValue() {

    Schema currentSchema =
        new Schema(
            optional(1, "aMap", MapType.ofOptional(2, 3, StringType.get(), StringType.get())));
    Schema targetSchema =
        new Schema(optional(1, "aMap", MapType.ofOptional(2, 3, StringType.get(), LongType.get())));

    assertThatThrownBy(
            () ->
                EvolveSchemaVisitor.visit(
                    TABLE,
                    loadUpdateApi(currentSchema),
                    currentSchema,
                    targetSchema,
                    PRESERVE_COLUMNS))
        .hasMessage("Cannot change column type: aMap.value: string -> long")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testDetectInvalidTopLevelMapKey() {
    Schema currentSchema =
        new Schema(
            optional(1, "aMap", MapType.ofOptional(2, 3, StringType.get(), StringType.get())));
    Schema targetSchema =
        new Schema(optional(1, "aMap", MapType.ofOptional(2, 3, UUIDType.get(), StringType.get())));
    assertThatThrownBy(
            () ->
                EvolveSchemaVisitor.visit(
                    TABLE,
                    loadUpdateApi(currentSchema),
                    currentSchema,
                    targetSchema,
                    PRESERVE_COLUMNS))
        .hasMessage("Cannot change column type: aMap.key: string -> uuid")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  // int 32-bit signed integers -> Can promote to long
  public void testTypePromoteIntegerToLong() {
    Schema currentSchema = new Schema(required(1, "aCol", IntegerType.get()));
    Schema targetSchema = new Schema(required(1, "aCol", LongType.get()));

    UpdateSchema updateApi = loadUpdateApi(currentSchema);
    EvolveSchemaVisitor.visit(TABLE, updateApi, currentSchema, targetSchema, PRESERVE_COLUMNS);
    Schema applied = updateApi.apply();
    assertThat(applied.asStruct().fields()).hasSize(1);
    assertThat(applied.asStruct().fields().get(0).type()).isEqualTo(LongType.get());
  }

  @Test
  // float 32-bit IEEE 754 floating point -> Can promote to double
  public void testTypePromoteFloatToDouble() {
    Schema currentSchema = new Schema(required(1, "aCol", FloatType.get()));
    Schema targetSchema = new Schema(required(1, "aCol", DoubleType.get()));

    UpdateSchema updateApi = loadUpdateApi(currentSchema);
    EvolveSchemaVisitor.visit(TABLE, updateApi, currentSchema, targetSchema, PRESERVE_COLUMNS);
    Schema applied = updateApi.apply();
    assertThat(applied.asStruct().fields()).hasSize(1);
    assertThat(applied.asStruct().fields().get(0).type()).isEqualTo(DoubleType.get());
  }

  @Test
  public void testInvalidTypePromoteDoubleToFloat() {
    Schema currentSchema = new Schema(required(1, "aCol", DoubleType.get()));
    Schema targetSchema = new Schema(required(1, "aCol", FloatType.get()));
    assertThatThrownBy(
            () ->
                EvolveSchemaVisitor.visit(
                    TABLE,
                    loadUpdateApi(currentSchema),
                    currentSchema,
                    targetSchema,
                    PRESERVE_COLUMNS))
        .hasMessage("Cannot change column type: aCol: double -> float")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  // decimal(P,S) Fixed-point decimal; precision P, scale S -> Scale is fixed [1], precision must be
  // 38 or less
  public void testTypePromoteDecimalToFixedScaleWithWiderPrecision() {
    Schema currentSchema = new Schema(required(1, "aCol", DecimalType.of(20, 1)));
    Schema targetSchema = new Schema(required(1, "aCol", DecimalType.of(22, 1)));

    UpdateSchema updateApi = loadUpdateApi(currentSchema);
    EvolveSchemaVisitor.visit(TABLE, updateApi, currentSchema, targetSchema, PRESERVE_COLUMNS);
    assertThat(updateApi.apply().asStruct()).isEqualTo(targetSchema.asStruct());
  }

  @Test
  public void testAddPrimitiveToNestedStruct() {
    Schema existingSchema =
        new Schema(
            required(
                1,
                "struct1",
                StructType.of(
                    optional(
                        2,
                        "struct2",
                        StructType.of(
                            optional(
                                3,
                                "list",
                                ListType.ofOptional(
                                    4,
                                    StructType.of(optional(5, "number", IntegerType.get())))))))));

    Schema targetSchema =
        new Schema(
            required(
                1,
                "struct1",
                StructType.of(
                    optional(
                        2,
                        "struct2",
                        StructType.of(
                            optional(
                                3,
                                "list",
                                ListType.ofOptional(
                                    4,
                                    StructType.of(
                                        optional(5, "number", LongType.get()),
                                        optional(6, "time", TimeType.get())))))))));

    UpdateSchema updateApi = loadUpdateApi(existingSchema);
    EvolveSchemaVisitor.visit(TABLE, updateApi, existingSchema, targetSchema, PRESERVE_COLUMNS);
    assertThat(updateApi.apply().asStruct()).isEqualTo(targetSchema.asStruct());
  }

  @Test
  public void testReplaceListWithPrimitive() {
    Schema currentSchema =
        new Schema(optional(1, "aColumn", ListType.ofOptional(2, StringType.get())));
    Schema targetSchema = new Schema(optional(1, "aColumn", StringType.get()));
    assertThatThrownBy(
            () ->
                EvolveSchemaVisitor.visit(
                    TABLE,
                    loadUpdateApi(currentSchema),
                    currentSchema,
                    targetSchema,
                    PRESERVE_COLUMNS))
        .hasMessage("Cannot change column type: aColumn: list<string> -> string")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void addNewTopLevelStruct() {
    Schema currentSchema =
        new Schema(
            optional(
                1,
                "map1",
                MapType.ofOptional(
                    2,
                    3,
                    StringType.get(),
                    ListType.ofOptional(
                        4, StructType.of(optional(5, "string1", StringType.get()))))));

    Schema targetSchema =
        new Schema(
            optional(
                1,
                "map1",
                MapType.ofOptional(
                    2,
                    3,
                    StringType.get(),
                    ListType.ofOptional(
                        4, StructType.of(optional(5, "string1", StringType.get()))))),
            optional(
                6,
                "struct1",
                StructType.of(
                    optional(7, "d1", StructType.of(optional(8, "d2", StringType.get()))))));

    UpdateSchema updateApi = loadUpdateApi(currentSchema);
    EvolveSchemaVisitor.visit(TABLE, updateApi, currentSchema, targetSchema, PRESERVE_COLUMNS);
    assertThat(updateApi.apply().asStruct()).isEqualTo(targetSchema.asStruct());
  }

  @Test
  public void testAppendNestedStruct() {
    Schema currentSchema =
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

    Schema targetSchema =
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

    UpdateSchema updateApi = loadUpdateApi(currentSchema);
    EvolveSchemaVisitor.visit(TABLE, updateApi, currentSchema, targetSchema, PRESERVE_COLUMNS);
    assertThat(updateApi.apply().asStruct()).isEqualTo(targetSchema.asStruct());
  }

  @Test
  public void testMakeNestedStructOptional() {
    Schema currentSchema = getNestedSchemaWithOptionalModifier(false);
    Schema targetSchema =
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
    UpdateSchema updateApi = loadUpdateApi(currentSchema);
    EvolveSchemaVisitor.visit(TABLE, updateApi, currentSchema, targetSchema, PRESERVE_COLUMNS);
    assertThat(getNestedSchemaWithOptionalModifier(true).asStruct())
        .isEqualTo(updateApi.apply().asStruct());
  }

  private static Schema getNestedSchemaWithOptionalModifier(boolean nestedIsOptional) {
    return new Schema(
        required(
            1,
            "s1",
            StructType.of(
                optional(
                    2,
                    "s2",
                    StructType.of(
                        optional(3, "s3", StructType.of(optional(4, "s4", StringType.get()))),
                        of(
                            5,
                            nestedIsOptional,
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
                                                            9, "s4", StringType.get()))))))))))))));
  }

  private static UpdateSchema loadUpdateApi(Schema schema) {
    try {
      Constructor<?> constructor =
          TestEvolveSchemaVisitor.class
              .getClassLoader()
              .loadClass("org.apache.iceberg.SchemaUpdate")
              .getDeclaredConstructor(Schema.class, int.class);
      constructor.setAccessible(true);
      return (UpdateSchema) constructor.newInstance(schema, schema.highestFieldId());
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate SchemaUpdate class", e);
    }
  }
}
