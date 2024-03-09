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
package org.apache.iceberg.orc;

import static org.apache.iceberg.orc.ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE;
import static org.apache.iceberg.orc.ORCSchemaUtil.ICEBERG_REQUIRED_ATTRIBUTE;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestORCSchemaUtil {

  private static final Types.StructType SUPPORTED_PRIMITIVES =
      Types.StructType.of(
          optional(1, "intCol", Types.IntegerType.get()),
          optional(3, "longCol", Types.LongType.get()),
          optional(6, "intCol2", Types.IntegerType.get()),
          optional(20, "intCol3", Types.IntegerType.get()),
          required(9, "doubleCol", Types.DoubleType.get()),
          required(10, "uuidCol", Types.UUIDType.get()),
          optional(2, "booleanCol", Types.BooleanType.get()),
          optional(21, "fixedCol", Types.FixedType.ofLength(4096)),
          required(22, "binaryCol", Types.BinaryType.get()),
          required(23, "stringCol", Types.StringType.get()),
          required(25, "floatCol", Types.FloatType.get()),
          optional(30, "dateCol", Types.DateType.get()),
          required(32, "timeCol", Types.TimeType.get()),
          required(34, "timestampCol", Types.TimestampType.withZone()),
          required(114, "dec_9_0", Types.DecimalType.of(9, 0)),
          required(115, "dec_11_2", Types.DecimalType.of(11, 2)),
          required(116, "dec_38_10", Types.DecimalType.of(38, 10)) // spark's maximum precision
          );

  @Test
  public void testRoundtripConversionPrimitive() {
    TypeDescription orcSchema = ORCSchemaUtil.convert(new Schema(SUPPORTED_PRIMITIVES.fields()));
    Assertions.assertThat(ORCSchemaUtil.convert(orcSchema).asStruct())
        .isEqualTo(SUPPORTED_PRIMITIVES);
  }

  @Test
  public void testRoundtripConversionNested() {
    Types.StructType leafStructType =
        Types.StructType.of(
            optional(6, "leafLongCol", Types.LongType.get()),
            optional(7, "leafBinaryCol", Types.BinaryType.get()));
    Types.StructType nestedStructType =
        Types.StructType.of(
            optional(4, "longCol", Types.LongType.get()),
            optional(5, "leafStructCol", leafStructType));
    Types.StructType structPrimTypeForList =
        Types.StructType.of(
            optional(506, "leafLongCol", Types.LongType.get()),
            optional(507, "leafBinaryCol", Types.BinaryType.get()));
    Types.StructType leafStructTypeForList =
        Types.StructType.of(
            optional(516, "leafLongCol", Types.LongType.get()),
            optional(517, "leafBinaryCol", Types.BinaryType.get()));
    Types.StructType nestedStructTypeForList =
        Types.StructType.of(
            optional(504, "longCol", Types.LongType.get()),
            optional(505, "leafStructCol", leafStructTypeForList));
    Types.StructType structPrimTypeForMap =
        Types.StructType.of(
            optional(606, "leafLongCol", Types.LongType.get()),
            optional(607, "leafBinaryCol", Types.BinaryType.get()));
    Types.StructType leafStructTypeForMap =
        Types.StructType.of(
            optional(616, "leafLongCol", Types.LongType.get()),
            optional(617, "leafBinaryCol", Types.BinaryType.get()));
    Types.StructType nestedStructTypeForMap =
        Types.StructType.of(
            optional(604, "longCol", Types.LongType.get()),
            optional(605, "leafStructCol", leafStructTypeForMap));
    Types.StructType leafStructTypeForStruct =
        Types.StructType.of(
            optional(716, "leafLongCol", Types.LongType.get()),
            optional(717, "leafBinaryCol", Types.BinaryType.get()));
    Types.StructType nestedStructTypeForStruct =
        Types.StructType.of(
            optional(704, "longCol", Types.LongType.get()),
            optional(705, "leafStructCol", leafStructTypeForStruct));
    // all fields in expected iceberg schema will be optional since we don't have a column mapping
    Schema expectedSchema =
        new Schema(
            optional(1, "intCol", Types.IntegerType.get()),
            optional(2, "longCol", Types.LongType.get()),
            optional(3, "nestedStructCol", nestedStructType),
            optional(8, "intCol3", Types.IntegerType.get()),
            optional(9, "doubleCol", Types.DoubleType.get()),
            required(10, "uuidCol", Types.UUIDType.get()),
            optional(20, "booleanCol", Types.BooleanType.get()),
            optional(21, "fixedCol", Types.FixedType.ofLength(4096)),
            required(22, "binaryCol", Types.BinaryType.get()),
            required(23, "stringCol", Types.StringType.get()),
            required(24, "decimalCol", Types.DecimalType.of(15, 3)),
            required(25, "floatCol", Types.FloatType.get()),
            optional(30, "dateCol", Types.DateType.get()),
            required(32, "timeCol", Types.TimeType.get()),
            required(34, "timestampCol", Types.TimestampType.withZone()),
            required(35, "listPrimCol", Types.ListType.ofRequired(135, Types.LongType.get())),
            required(36, "listPrimNestCol", Types.ListType.ofRequired(136, structPrimTypeForList)),
            required(37, "listNestedCol", Types.ListType.ofRequired(137, nestedStructTypeForList)),
            optional(
                38,
                "mapPrimCol",
                Types.MapType.ofRequired(
                    138, 238, Types.StringType.get(), Types.FixedType.ofLength(4096))),
            required(
                39,
                "mapPrimNestCol",
                Types.MapType.ofRequired(139, 239, Types.StringType.get(), structPrimTypeForMap)),
            required(
                40,
                "mapNestedCol",
                Types.MapType.ofRequired(140, 240, Types.StringType.get(), nestedStructTypeForMap)),
            required(
                41,
                "structListNestCol",
                Types.ListType.ofRequired(
                    241,
                    Types.StructType.of(
                        optional(816, "leafLongCol", Types.LongType.get()),
                        optional(817, "leafBinaryCol", Types.BinaryType.get())))),
            required(
                42,
                "structMapNestCol",
                Types.MapType.ofRequired(
                    242,
                    342,
                    Types.StringType.get(),
                    Types.StructType.of(
                        optional(916, "leafLongCol", Types.LongType.get()),
                        optional(917, "leafBinaryCol", Types.BinaryType.get())))),
            required(
                43,
                "structStructNestCol",
                Types.StructType.of(
                    required(
                        243,
                        "innerStructNest",
                        Types.StructType.of(
                            optional(1016, "leafLongCol", Types.LongType.get()),
                            optional(1017, "leafBinaryCol", Types.BinaryType.get()))))),
            required(
                44,
                "structStructComplexNestCol",
                Types.StructType.of(
                    required(
                        244,
                        "innerStructNest",
                        Types.StructType.of(
                            optional(1116, "leafLongCol", Types.LongType.get()),
                            optional(
                                1117,
                                "leftMapOfListStructCol",
                                Types.MapType.ofRequired(
                                    1150,
                                    1151,
                                    Types.StringType.get(),
                                    Types.ListType.ofRequired(
                                        1250, nestedStructTypeForStruct))))))));
    TypeDescription orcSchema = ORCSchemaUtil.convert(expectedSchema);
    Assertions.assertThat(ORCSchemaUtil.convert(orcSchema).asStruct())
        .isEqualTo(expectedSchema.asStruct());
  }

  @Test
  public void testTypePromotions() {
    Schema originalSchema =
        new Schema(
            optional(1, "a", Types.IntegerType.get()),
            optional(2, "b", Types.FloatType.get()),
            optional(3, "c", Types.DecimalType.of(10, 2)));

    // Original mapping (stored in ORC)
    TypeDescription orcSchema = ORCSchemaUtil.convert(originalSchema);

    // Evolve schema
    Schema evolveSchema =
        new Schema(
            optional(1, "a", Types.LongType.get()),
            optional(2, "b", Types.DoubleType.get()),
            optional(3, "c", Types.DecimalType.of(15, 2)));

    TypeDescription newOrcSchema = ORCSchemaUtil.buildOrcProjection(evolveSchema, orcSchema);
    Assertions.assertThat(newOrcSchema.getChildren()).hasSize(3);
    Assertions.assertThat(newOrcSchema.findSubtype("a").getId()).isEqualTo(1);
    Assertions.assertThat(newOrcSchema.findSubtype("a").getCategory())
        .isEqualTo(TypeDescription.Category.LONG);
    Assertions.assertThat(newOrcSchema.findSubtype("b").getId()).isEqualTo(2);
    Assertions.assertThat(newOrcSchema.findSubtype("b").getCategory())
        .isEqualTo(TypeDescription.Category.DOUBLE);
    TypeDescription decimalC = newOrcSchema.findSubtype("c");
    Assertions.assertThat(decimalC.getId()).isEqualTo(3);
    Assertions.assertThat(decimalC.getCategory()).isEqualTo(TypeDescription.Category.DECIMAL);
    Assertions.assertThat(decimalC.getPrecision()).isEqualTo(15);
    Assertions.assertThat(decimalC.getScale()).isEqualTo(2);
  }

  @Test
  public void testInvalidTypePromotions() {
    Schema originalSchema = new Schema(optional(1, "a", Types.LongType.get()));

    TypeDescription orcSchema = ORCSchemaUtil.convert(originalSchema);
    Schema evolveSchema = new Schema(optional(1, "a", Types.IntegerType.get()));

    Assertions.assertThatThrownBy(() -> ORCSchemaUtil.buildOrcProjection(evolveSchema, orcSchema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Can not promote LONG type to INTEGER");
  }

  @Test
  public void testSkipNonIcebergColumns() {
    TypeDescription schema = TypeDescription.createStruct();
    TypeDescription intCol = TypeDescription.createInt();
    intCol.setAttribute(ICEBERG_ID_ATTRIBUTE, "1");
    intCol.setAttribute(ICEBERG_REQUIRED_ATTRIBUTE, "true");
    TypeDescription listCol =
        TypeDescription.createList(
            TypeDescription.createMap(
                TypeDescription.createString(), TypeDescription.createDate()));
    listCol.setAttribute(ICEBERG_ID_ATTRIBUTE, "2");
    schema.addField("intCol", intCol);
    schema.addField("listCol", listCol);
    TypeDescription stringKey = TypeDescription.createString();
    stringKey.setAttribute(ICEBERG_ID_ATTRIBUTE, "3");
    TypeDescription booleanVal = TypeDescription.createBoolean();
    booleanVal.setAttribute(ICEBERG_ID_ATTRIBUTE, "4");
    TypeDescription mapCol = TypeDescription.createMap(stringKey, booleanVal);
    mapCol.setAttribute(ICEBERG_ID_ATTRIBUTE, "5");
    schema.addField("mapCol", mapCol);

    Schema icebergSchema = ORCSchemaUtil.convert(schema);
    Schema expectedSchema =
        new Schema(
            required(1, "intCol", Types.IntegerType.get()),
            // Skipped listCol since element has no Iceberg ID
            optional(
                5,
                "mapCol",
                Types.MapType.ofOptional(3, 4, Types.StringType.get(), Types.BooleanType.get())));
    Assertions.assertThat(icebergSchema.asStruct())
        .as("Schemas must match.")
        .isEqualTo(expectedSchema.asStruct());

    TypeDescription structCol = TypeDescription.createStruct();
    structCol.setAttribute(ICEBERG_ID_ATTRIBUTE, "7");
    structCol.setAttribute(ICEBERG_REQUIRED_ATTRIBUTE, "true");
    TypeDescription binaryCol = TypeDescription.createBinary();
    TypeDescription doubleCol = TypeDescription.createDouble();
    doubleCol.setAttribute(ICEBERG_ID_ATTRIBUTE, "6");
    doubleCol.setAttribute(ICEBERG_REQUIRED_ATTRIBUTE, "true");
    structCol.addField("binaryCol", binaryCol);
    structCol.addField("doubleCol", doubleCol);
    schema.addField("structCol", structCol);
    TypeDescription stringKey2 = TypeDescription.createString();
    stringKey2.setAttribute(ICEBERG_ID_ATTRIBUTE, "8");
    TypeDescription mapCol2 = TypeDescription.createMap(stringKey2, TypeDescription.createDate());
    mapCol2.setAttribute(ICEBERG_ID_ATTRIBUTE, "10");
    schema.addField("mapCol2", mapCol2);

    Schema icebergSchema2 = ORCSchemaUtil.convert(schema);
    Schema expectedSchema2 =
        new Schema(
            required(1, "intCol", Types.IntegerType.get()),
            optional(
                5,
                "mapCol",
                Types.MapType.ofOptional(3, 4, Types.StringType.get(), Types.BooleanType.get())),
            required(
                7,
                "structCol",
                Types.StructType.of(
                    // Skipped binaryCol
                    required(6, "doubleCol", Types.DoubleType.get())
                    // Skipped mapCol2 since value has no Iceberg ID
                    )));
    Assertions.assertThat(icebergSchema2.asStruct())
        .as("Schemas must match.")
        .isEqualTo(expectedSchema2.asStruct());
  }

  @Test
  public void testHasIds() {
    Schema schema =
        new Schema(
            optional(
                1,
                "data",
                Types.StructType.of(
                    optional(
                        10,
                        "entries",
                        Types.MapType.ofOptional(
                            11, 12, Types.StringType.get(), Types.DateType.get())))),
            optional(2, "intCol", Types.IntegerType.get()),
            optional(3, "longCol", Types.LongType.get()),
            optional(4, "listCol", Types.ListType.ofOptional(40, Types.DoubleType.get())));

    TypeDescription orcSchema = ORCSchemaUtil.removeIds(ORCSchemaUtil.convert(schema));
    Assertions.assertThat(ORCSchemaUtil.hasIds(orcSchema)).as("Should not have Ids").isFalse();

    TypeDescription map2Col =
        TypeDescription.createMap(TypeDescription.createString(), TypeDescription.createBinary());
    map2Col.setAttribute(ICEBERG_ID_ATTRIBUTE, "4");
    orcSchema.addField("map2Col", map2Col);
    Assertions.assertThat(ORCSchemaUtil.hasIds(orcSchema))
        .as("Should have Ids after adding one type with Id")
        .isTrue();
  }

  @Test
  public void testAssignIdsByNameMapping() {
    Types.StructType structType =
        Types.StructType.of(
            required(0, "id", Types.LongType.get()),
            optional(
                1,
                "list_of_maps",
                Types.ListType.ofOptional(
                    2,
                    Types.MapType.ofOptional(3, 4, Types.StringType.get(), SUPPORTED_PRIMITIVES))),
            optional(
                5,
                "map_of_lists",
                Types.MapType.ofOptional(
                    6,
                    7,
                    Types.StringType.get(),
                    Types.ListType.ofOptional(8, SUPPORTED_PRIMITIVES))),
            required(
                9,
                "list_of_lists",
                Types.ListType.ofOptional(10, Types.ListType.ofOptional(11, SUPPORTED_PRIMITIVES))),
            required(
                12,
                "map_of_maps",
                Types.MapType.ofOptional(
                    13,
                    14,
                    Types.StringType.get(),
                    Types.MapType.ofOptional(
                        15, 16, Types.StringType.get(), SUPPORTED_PRIMITIVES))),
            required(
                17,
                "list_of_struct_of_nested_types",
                Types.ListType.ofOptional(
                    19,
                    Types.StructType.of(
                        Types.NestedField.required(
                            20,
                            "m1",
                            Types.MapType.ofOptional(
                                21, 22, Types.StringType.get(), SUPPORTED_PRIMITIVES)),
                        Types.NestedField.optional(
                            23, "l1", Types.ListType.ofRequired(24, SUPPORTED_PRIMITIVES)),
                        Types.NestedField.required(
                            25, "l2", Types.ListType.ofRequired(26, SUPPORTED_PRIMITIVES)),
                        Types.NestedField.optional(
                            27,
                            "m2",
                            Types.MapType.ofOptional(
                                28, 29, Types.StringType.get(), SUPPORTED_PRIMITIVES))))));

    Schema schema =
        new Schema(
            TypeUtil.assignFreshIds(structType, new AtomicInteger(0)::incrementAndGet)
                .asStructType()
                .fields());

    NameMapping nameMapping = MappingUtil.create(schema);
    TypeDescription typeDescriptionWithIds = ORCSchemaUtil.convert(schema);
    TypeDescription typeDescriptionWithIdsFromNameMapping =
        ORCSchemaUtil.applyNameMapping(
            ORCSchemaUtil.removeIds(typeDescriptionWithIds), nameMapping);

    Assertions.assertThat(
            equalsWithIds(typeDescriptionWithIds, typeDescriptionWithIdsFromNameMapping))
        .as("TypeDescription schemas should be equal, including IDs")
        .isTrue();
  }

  @Test
  public void testAssignIdsByNameMappingAndProject() {
    Types.StructType structType =
        Types.StructType.of(
            required(1, "id", Types.LongType.get()),
            optional(
                2,
                "list_of_structs",
                Types.ListType.ofOptional(
                    3,
                    Types.StructType.of(
                        required(4, "entry", Types.LongType.get()),
                        required(5, "data", Types.BinaryType.get())))),
            optional(
                6,
                "map",
                Types.MapType.ofOptional(7, 8, Types.StringType.get(), Types.DoubleType.get())),
            optional(
                12,
                "map_of_structs",
                Types.MapType.ofOptional(
                    13,
                    14,
                    Types.StringType.get(),
                    Types.StructType.of(required(20, "field", Types.LongType.get())))),
            required(
                30,
                "struct",
                Types.StructType.of(
                    required(31, "lat", Types.DoubleType.get()),
                    required(32, "long", Types.DoubleType.get()))));

    TypeDescription fileSchema =
        ORCSchemaUtil.removeIds(
            ORCSchemaUtil.convert(new Schema(structType.asStructType().fields())));

    Schema mappingSchema =
        new Schema(
            Types.StructType.of(
                    optional(1, "new_id", Types.LongType.get()),
                    optional(
                        2,
                        "list_of_structs",
                        Types.ListType.ofOptional(
                            3, Types.StructType.of(required(5, "data", Types.BinaryType.get())))),
                    optional(
                        6,
                        "map",
                        Types.MapType.ofOptional(
                            7, 8, Types.StringType.get(), Types.DoubleType.get())),
                    optional(
                        30,
                        "struct",
                        Types.StructType.of(
                            optional(31, "latitude", Types.DoubleType.get()),
                            optional(32, "longitude", Types.DoubleType.get()))),
                    optional(40, "long", Types.LongType.get()))
                .asStructType()
                .fields());

    NameMapping nameMapping = MappingUtil.create(mappingSchema);
    TypeDescription typeDescriptionWithIdsFromNameMapping =
        ORCSchemaUtil.applyNameMapping(fileSchema, nameMapping);

    TypeDescription expected = TypeDescription.createStruct();
    // new field
    TypeDescription newId = TypeDescription.createLong();
    newId.setAttribute(ICEBERG_ID_ATTRIBUTE, "1");
    expected.addField("new_id_r1", newId);

    // list_of_structs
    TypeDescription structElem = TypeDescription.createStruct();
    structElem.setAttribute(ICEBERG_ID_ATTRIBUTE, "3");
    TypeDescription dataInStruct = TypeDescription.createBinary();
    dataInStruct.setAttribute(ICEBERG_ID_ATTRIBUTE, "5");
    structElem.addField("data", dataInStruct);
    TypeDescription list = TypeDescription.createList(structElem);
    list.setAttribute(ICEBERG_ID_ATTRIBUTE, "2");

    // map
    TypeDescription mapKey = TypeDescription.createString();
    mapKey.setAttribute(ICEBERG_ID_ATTRIBUTE, "7");
    TypeDescription mapValue = TypeDescription.createDouble();
    mapValue.setAttribute(ICEBERG_ID_ATTRIBUTE, "8");
    TypeDescription map = TypeDescription.createMap(mapKey, mapValue);
    map.setAttribute(ICEBERG_ID_ATTRIBUTE, "6");

    expected.addField("list_of_structs", list);
    expected.addField("map", map);

    TypeDescription struct = TypeDescription.createStruct();
    struct.setAttribute(ICEBERG_ID_ATTRIBUTE, "30");
    TypeDescription latitude = TypeDescription.createDouble();
    latitude.setAttribute(ICEBERG_ID_ATTRIBUTE, "31");
    TypeDescription longitude = TypeDescription.createDouble();
    longitude.setAttribute(ICEBERG_ID_ATTRIBUTE, "32");
    struct.addField("latitude_r31", latitude);
    struct.addField("longitude_r32", longitude);
    expected.addField("struct", struct);
    TypeDescription longField = TypeDescription.createLong();
    longField.setAttribute(ICEBERG_ID_ATTRIBUTE, "40");
    expected.addField("long_r40", longField);

    Assertions.assertThat(typeDescriptionWithIdsFromNameMapping.equals(fileSchema, false))
        .as("ORC Schema must have the same structure, but one has Iceberg IDs")
        .isTrue();

    TypeDescription projectedOrcSchema =
        ORCSchemaUtil.buildOrcProjection(mappingSchema, typeDescriptionWithIdsFromNameMapping);

    Assertions.assertThat(equalsWithIds(expected, projectedOrcSchema))
        .as("Schema should be the prunned by projection")
        .isTrue();
  }

  private static boolean equalsWithIds(TypeDescription first, TypeDescription second) {
    if (second == first) {
      return true;
    }

    if (!first.equals(second, false)) {
      return false;
    }

    // check the ID attribute on non-root TypeDescriptions
    if (first.getId() > 0 && second.getId() > 0) {
      if (first.getAttributeValue(ICEBERG_ID_ATTRIBUTE) == null
          || second.getAttributeValue(ICEBERG_ID_ATTRIBUTE) == null) {
        return false;
      }

      if (!first
          .getAttributeValue(ICEBERG_ID_ATTRIBUTE)
          .equals(second.getAttributeValue(ICEBERG_ID_ATTRIBUTE))) {
        return false;
      }
    }

    // check the children
    List<TypeDescription> firstChildren =
        Optional.ofNullable(first.getChildren()).orElse(Collections.emptyList());
    List<TypeDescription> secondChildren =
        Optional.ofNullable(second.getChildren()).orElse(Collections.emptyList());
    if (firstChildren.size() != secondChildren.size()) {
      return false;
    }
    for (int i = 0; i < firstChildren.size(); ++i) {
      if (!equalsWithIds(firstChildren.get(i), secondChildren.get(i))) {
        return false;
      }
    }

    return true;
  }
}
