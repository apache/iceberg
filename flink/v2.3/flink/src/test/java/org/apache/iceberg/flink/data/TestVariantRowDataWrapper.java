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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.VariantType;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantBuilder;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.flink.DataGenerator;
import org.apache.iceberg.flink.DataGenerators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
class TestVariantRowDataWrapper {
  @Parameter private DataGenerator data;

  @Parameters(name = "DataGenerator={0}")
  public static Iterable<Object[]> parameters() {
    return Lists.newArrayList(
        new Object[] {new DataGenerators.Primitives()},
        new Object[] {new DataGenerators.StructOfPrimitive()},
        new Object[] {new DataGenerators.StructOfArray()},
        new Object[] {new DataGenerators.StructOfMap()},
        new Object[] {new DataGenerators.StructOfStruct()},
        new Object[] {new DataGenerators.ArrayOfPrimitive()},
        new Object[] {new DataGenerators.ArrayOfArray()},
        new Object[] {new DataGenerators.ArrayOfMap()},
        new Object[] {new DataGenerators.ArrayOfStruct()},
        new Object[] {new DataGenerators.MapOfPrimitives()},
        new Object[] {new DataGenerators.MapOfArray()},
        new Object[] {new DataGenerators.MapOfMap()},
        new Object[] {new DataGenerators.MapOfStruct()});
  }

  @TestTemplate
  void testConversion() {
    verifyConversion(data);
  }

  @Test
  void testNullMissingField() {
    RowType rowType = nullableRowType();
    Variant variant = Variant.newBuilder().object().build();
    VariantRowDataWrapper wrapper = new VariantRowDataWrapper(rowType).wrap(variant);

    assertNulls(rowType, wrapper);
    assertThat(wrapper.getVariant(7)).isNull();
  }

  @Test
  void testNullFieldsExplicitNull() {
    RowType rowType = nullableRowType();
    VariantBuilder builder = Variant.newBuilder();
    Variant variant =
        builder
            .object()
            .add("string_field", builder.ofNull())
            .add("binary_field", builder.ofNull())
            .add("decimal_field", builder.ofNull())
            .add("timestamp_field", builder.ofNull())
            .add("array_field", builder.ofNull())
            .add("map_field", builder.ofNull())
            .add("row_field", builder.ofNull())
            .add("variant_field", builder.ofNull())
            .build();
    VariantRowDataWrapper wrapper = new VariantRowDataWrapper(rowType).wrap(variant);

    assertNulls(rowType, wrapper);
    Variant variantField = wrapper.getVariant(7);
    assertThat(variantField).isNotNull();
    assertThat(variantField.isNull()).isTrue();
  }

  @Test
  void testArrayWithNullElements() {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new ArrayType(true, new IntType(true))},
            new String[] {"array_field"});

    VariantBuilder builder = Variant.newBuilder();
    Variant variant =
        builder
            .object()
            .add(
                "array_field",
                builder.array().add(builder.of(1)).add(builder.ofNull()).add(builder.of(3)).build())
            .build();
    VariantRowDataWrapper wrapper = new VariantRowDataWrapper(rowType).wrap(variant);

    ArrayData array = wrapper.getArray(0);
    assertThat(array).isNotNull();
    assertThat(array.size()).isEqualTo(3);
    assertThat(array.isNullAt(0)).isFalse();
    assertThat(array.getInt(0)).isEqualTo(1);
    assertThat(array.isNullAt(1)).isTrue();
    assertThat(array.isNullAt(2)).isFalse();
    assertThat(array.getInt(2)).isEqualTo(3);
  }

  @Test
  void testMapWithNullValues() {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new MapType(true, new VarCharType(false, 255), new IntType(true))},
            new String[] {"map_field"});

    VariantBuilder builder = Variant.newBuilder();
    Variant variant =
        builder
            .object()
            .add(
                "map_field",
                builder.object().add("a", builder.of(1)).add("b", builder.ofNull()).build())
            .build();
    VariantRowDataWrapper wrapper = new VariantRowDataWrapper(rowType).wrap(variant);

    MapData map = wrapper.getMap(0);
    assertThat(map).isNotNull();
    assertThat(map.size()).isEqualTo(2);

    ArrayData keys = map.keyArray();
    ArrayData values = map.valueArray();
    for (int i = 0; i < keys.size(); i++) {
      String key = keys.getString(i).toString();
      if ("b".equals(key)) {
        assertThat(values.isNullAt(i)).isTrue();
      } else {
        assertThat(values.isNullAt(i)).isFalse();
        assertThat(values.getInt(i)).isEqualTo(1);
      }
    }
  }

  private static RowType nullableRowType() {
    return RowType.of(
        new LogicalType[] {
          new VarCharType(true, 255),
          new VarBinaryType(true, 100),
          new DecimalType(true, 10, 2),
          new TimestampType(true, 6),
          new ArrayType(true, new IntType(true)),
          new MapType(true, new VarCharType(false, 255), new IntType(true)),
          RowType.of(true, new LogicalType[] {new IntType(true)}, new String[] {"x"}),
          new VariantType(true)
        },
        new String[] {
          "string_field",
          "binary_field",
          "decimal_field",
          "timestamp_field",
          "array_field",
          "map_field",
          "row_field",
          "variant_field"
        });
  }

  private static void assertNulls(RowType rowType, VariantRowDataWrapper wrapper) {
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      assertThat(wrapper.isNullAt(i)).isTrue();
    }

    assertThat(wrapper.getString(0)).isNull();
    assertThat(wrapper.getBinary(1)).isNull();
    assertThat(wrapper.getDecimal(2, 10, 2)).isNull();
    assertThat(wrapper.getTimestamp(3, 6)).isNull();
    assertThat(wrapper.getArray(4)).isNull();
    assertThat(wrapper.getMap(5)).isNull();
    assertThat(wrapper.getRow(6, 1)).isNull();
  }

  private static void verifyConversion(DataGenerator dataGenerator) {
    RowType rowType = dataGenerator.flinkRowType();
    Variant variant = dataGenerator.generateFlinkVariantData();
    RowData rowData = dataGenerator.generateFlinkRowData();
    VariantRowDataWrapper variantRowDataWrapper = variantRowDataWrapper(rowType, variant);
    compareData(rowType, variantRowDataWrapper, rowData);
  }

  private static VariantRowDataWrapper variantRowDataWrapper(RowType rowType, Variant variant) {
    VariantRowDataWrapper wrapper = new VariantRowDataWrapper(rowType);
    return wrapper.wrap(variant);
  }

  private static void compareData(RowType rowType, Object variantRowDataWrapper, Object rowData) {
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      LogicalType logicalType = rowType.getTypeAt(i);
      RowData.FieldGetter fieldGetter = RowData.createFieldGetter(logicalType, i);

      Object variantValue = fieldGetter.getFieldOrNull((RowData) variantRowDataWrapper);
      Object rowDataValue = fieldGetter.getFieldOrNull((RowData) rowData);

      switch (logicalType.getTypeRoot()) {
        case ROW:
          compareData((RowType) logicalType, variantValue, rowDataValue);
          break;
        case MAP:
          MapType mapType = (MapType) logicalType;
          LogicalType valueType = mapType.getValueType();

          if (valueType.is(LogicalTypeRoot.ROW)) {
            GenericMapData mapVariantData = (GenericMapData) variantValue;
            GenericMapData mapRowData = (GenericMapData) rowDataValue;

            ArrayData rowDataKeys = mapRowData.keyArray();
            ArrayData variantKeys = mapVariantData.keyArray();

            assertThat(variantKeys.size()).isEqualTo(rowDataKeys.size());
            List<StringData> variantKeyList = Lists.newArrayList();
            List<StringData> rowDataKeyList = Lists.newArrayList();
            for (int k = 0; k < rowDataKeys.size(); k++) {
              variantKeyList.add(variantKeys.getString(k));
              rowDataKeyList.add(rowDataKeys.getString(k));
            }

            assertThat(variantKeyList).containsExactlyInAnyOrderElementsOf(rowDataKeyList);

            for (int k = 0; k < rowDataKeys.size(); k++) {
              StringData key = rowDataKeys.getString(k);
              compareData((RowType) valueType, mapVariantData.get(key), mapRowData.get(key));
            }
          } else {
            assertThat(variantValue).isEqualTo(rowDataValue);
          }
          break;
        case ARRAY:
          ArrayType arrayType = (ArrayType) logicalType;
          LogicalType elementType = arrayType.getElementType();

          if (elementType.is(LogicalTypeRoot.ROW)) {
            GenericArrayData arrayVariantData = (GenericArrayData) variantValue;
            GenericArrayData arrayRowData = (GenericArrayData) rowDataValue;

            assertThat(arrayVariantData.size()).isEqualTo(arrayRowData.size());
            for (int j = 0; j < arrayVariantData.size(); j++) {
              ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
              compareData(
                  (RowType) elementType,
                  elementGetter.getElementOrNull(arrayVariantData, j),
                  elementGetter.getElementOrNull(arrayRowData, j));
            }
          } else {
            assertThat(variantValue).isEqualTo(rowDataValue);
          }
          break;
        default:
          assertThat(variantValue).isEqualTo(rowDataValue);
      }
    }
  }
}
