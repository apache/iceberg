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
package org.apache.iceberg.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Function;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.flink.DataGenerator;
import org.apache.iceberg.flink.DataGenerators;
import org.junit.jupiter.api.Test;

class TestRowDataTransformerUtil {

  @Test
  public void testPrimitiveTypes() {
    testConverter(new DataGenerators.Primitives());
  }

  @Test
  public void testStructOfPrimitive() {
    testConverter(new DataGenerators.StructOfPrimitive());
  }

  @Test
  public void testStructOfArray() {
    testConverter(new DataGenerators.StructOfArray());
  }

  @Test
  public void testStructOfMap() {
    testConverter(new DataGenerators.StructOfMap());
  }

  @Test
  public void testStructOfStruct() {
    testConverter(new DataGenerators.StructOfStruct());
  }

  @Test
  public void testArrayOfPrimitive() {
    testConverter(new DataGenerators.ArrayOfPrimitive());
  }

  @Test
  public void testArrayOfArray() {
    testConverter(new DataGenerators.ArrayOfArray());
  }

  @Test
  public void testArrayOfMap() {
    testConverter(new DataGenerators.ArrayOfMap());
  }

  @Test
  public void testArrayOfStruct() {
    testConverter(new DataGenerators.ArrayOfStruct());
  }

  @Test
  public void testMapOfPrimitives() {
    testConverter(new DataGenerators.MapOfPrimitives());
  }

  @Test
  public void testMapOfArray() {
    testConverter(new DataGenerators.MapOfArray());
  }

  @Test
  public void testMapOfMap() {
    testConverter(new DataGenerators.MapOfMap());
  }

  @Test
  public void testMapOfStruct() {
    testConverter(new DataGenerators.MapOfStruct());
  }

  @Test
  public void testPositionDelete() {
    Function<PositionDelete<RowData>, RowData> transformer =
        RowDataTransformerUtil.deleteTransformer();
    PositionDelete<RowData> delete = PositionDelete.create();
    RowData data = transformer.apply(delete.set("path", 1L));
    assertThat(data.getString(0)).isEqualTo(StringData.fromString("path"));
    assertThat(data.getLong(1)).isEqualTo(1L);
    assertThat(data.getRow(2, 0)).isNull();
    assertThat(data.isNullAt(2)).isTrue();
  }

  @Test
  public void testPositionDeleteWithRow() {
    Function<PositionDelete<RowData>, RowData> transformer =
        RowDataTransformerUtil.deleteTransformer();
    PositionDelete<RowData> delete = PositionDelete.create();
    DataGenerator dataGenerator = new DataGenerators.Primitives();
    RowData deletedRow = dataGenerator.generateFlinkRowData();
    RowData data = transformer.apply(delete.set("path", 1L, deletedRow));
    assertThat(data.getString(0)).isEqualTo(StringData.fromString("path"));
    assertThat(data.getLong(1)).isEqualTo(1L);
    assertThat(data.isNullAt(2)).isFalse();
    RowData actual = data.getRow(2, deletedRow.getArity());
    // Run the serializer copy to ensure that the wrapped row data is at least accessible
    new RowDataSerializer(new DataGenerators.Primitives().flinkRowType()).copy(actual);
  }

  protected void testConverter(DataGenerator dataGenerator) {
    Function<RowData, RowData> transformer =
        RowDataTransformerUtil.forcedTransformer(
            dataGenerator.flinkRowType(), dataGenerator.icebergSchema().asStruct());
    RowData expected = dataGenerator.generateFlinkRowData();
    RowData actual = transformer.apply(expected);

    // Run the serializer copy to ensure that the wrapped row data is at least accessible
    new RowDataSerializer(dataGenerator.flinkRowType()).copy(actual);
  }
}
