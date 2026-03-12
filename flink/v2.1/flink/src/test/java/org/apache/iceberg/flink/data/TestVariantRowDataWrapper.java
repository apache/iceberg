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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.variant.Variant;
import org.apache.iceberg.flink.DataGenerator;
import org.apache.iceberg.flink.DataGenerators;
import org.junit.jupiter.api.Test;

public class TestVariantRowDataWrapper {

  @Test
  public void testPrimitives() {
    testDataGenerator(new DataGenerators.Primitives());
  }

  @Test
  public void testStructOfPrimitive() {
    testDataGenerator(new DataGenerators.StructOfPrimitive());
  }

  @Test
  public void testStructOfArray() {
    testDataGenerator(new DataGenerators.StructOfArray());
  }

  @Test
  public void testStructOfMap() {
    testDataGenerator(new DataGenerators.StructOfMap());
  }

  @Test
  public void testStructOfStruct() {
    testDataGenerator(new DataGenerators.StructOfStruct());
  }

  @Test
  public void testArrayOfPrimitive() {
    testDataGenerator(new DataGenerators.ArrayOfPrimitive());
  }

  @Test
  public void testArrayOfArray() {
    testDataGenerator(new DataGenerators.ArrayOfArray());
  }

  @Test
  public void testArrayOfMap() {
    testDataGenerator(new DataGenerators.ArrayOfMap());
  }

  @Test
  public void testArrayOfStruct() {
    testDataGenerator(new DataGenerators.ArrayOfStruct());
  }

  @Test
  public void testMapOfPrimitives() {
    testDataGenerator(new DataGenerators.MapOfPrimitives());
  }

  @Test
  public void testMapOfArray() {
    testDataGenerator(new DataGenerators.MapOfArray());
  }

  @Test
  public void testMapOfMap() {
    testDataGenerator(new DataGenerators.MapOfMap());
  }

  @Test
  public void testMapOfStruct() {
    testDataGenerator(new DataGenerators.MapOfStruct());
  }

  private static void testDataGenerator(DataGenerator dataGenerator) {
    RowType rowType = dataGenerator.flinkRowType();
    Variant variant = dataGenerator.generateFlinkVariantData();
    RowData rowData = dataGenerator.generateFlinkRowData();
    VariantRowDataWrapper variantRowDataWrapper = getVariantRowDataWrapper(rowType, variant);
    compareData(rowType, variantRowDataWrapper, rowData);
  }

  private static VariantRowDataWrapper getVariantRowDataWrapper(RowType rowType, Variant variant) {
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
          return;
        case MAP:
          MapType mapType = (MapType) logicalType;
          LogicalType valueType = mapType.getValueType();

          if (valueType.is(LogicalTypeRoot.ROW)) {
            GenericMapData mapVariantData = (GenericMapData) variantValue;
            GenericMapData mapRowData = (GenericMapData) rowDataValue;

            ArrayData keys = mapRowData.keyArray();
            for (int k = 0; k < keys.size(); k++) {
              StringData key = keys.getString(k);
              compareData((RowType) valueType, mapVariantData.get(key), mapRowData.get(key));
            }
          } else {
            assertThat(variantValue).isEqualTo(rowDataValue);
          }
          return;
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
          return;
        default:
          assertThat(variantValue).isEqualTo(rowDataValue);
      }
    }
  }
}
