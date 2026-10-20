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
package org.apache.iceberg.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.RecordWrapperTestBase;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.data.RandomRowData;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.junit.jupiter.api.Test;

public class TestRowDataWrapper extends RecordWrapperTestBase {

  /**
   * Flink's time type has been truncated to millis seconds, so we need a customized assert method
   * to check the values.
   */
  @Override
  public void testTime() {
    generateAndValidate(
        new Schema(TIME.fields()),
        (message, expectedWrapper, actualWrapper) -> {
          for (int pos = 0; pos < TIME.fields().size(); pos++) {
            Object expected = expectedWrapper.get().get(pos, Object.class);
            Object actual = actualWrapper.get().get(pos, Object.class);
            if (expected == actual) {
              return;
            }

            assertThat(actual).isNotNull();
            assertThat(expected).isNotNull();

            int expectedMilliseconds = (int) ((long) expected / 1000_000);
            int actualMilliseconds = (int) ((long) actual / 1000_000);
            assertThat(actualMilliseconds).as(message).isEqualTo(expectedMilliseconds);
          }
        });
  }

  @Override
  protected void generateAndValidate(
      Schema schema, RecordWrapperTestBase.AssertMethod assertMethod) {
    int numRecords = 100;
    Iterable<Record> recordList = RandomGenericData.generate(schema, numRecords, 101L);
    Iterable<RowData> rowDataList = RandomRowData.generate(schema, numRecords, 101L);

    InternalRecordWrapper recordWrapper = new InternalRecordWrapper(schema.asStruct());
    RowDataWrapper rowDataWrapper =
        new RowDataWrapper(FlinkSchemaUtil.convert(schema), schema.asStruct());

    Iterator<Record> actual = recordList.iterator();
    Iterator<RowData> expected = rowDataList.iterator();

    StructLikeWrapper actualWrapper = StructLikeWrapper.forType(schema.asStruct());
    StructLikeWrapper expectedWrapper = StructLikeWrapper.forType(schema.asStruct());
    for (int i = 0; i < numRecords; i++) {
      assertThat(actual).hasNext();
      assertThat(expected).hasNext();

      StructLike recordStructLike = recordWrapper.wrap(actual.next());
      StructLike rowDataStructLike = rowDataWrapper.wrap(expected.next());

      assertMethod.assertEquals(
          "Should have expected StructLike values",
          expectedWrapper.set(rowDataStructLike),
          actualWrapper.set(recordStructLike));
    }

    assertThat(actual).isExhausted();
    assertThat(expected).isExhausted();
  }

  @Test
  void listElementsAreConverted() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1, "tags", Types.ListType.ofOptional(2, Types.StringType.get())));
    RowData row =
        GenericRowData.of(new GenericArrayData(new Object[] {StringData.fromString("a"), null}));

    assertThat(wrap(schema, row).get(0, List.class)).containsExactly("a", null);
  }

  @Test
  void listOfTimestampsIsConvertedToMicros() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1, "times", Types.ListType.ofOptional(2, Types.TimestampType.withoutZone())));
    LocalDateTime time = LocalDateTime.of(2026, 10, 14, 12, 30, 45, 123_456_000);
    RowData row =
        GenericRowData.of(
            new GenericArrayData(new Object[] {TimestampData.fromLocalDateTime(time)}));

    assertThat(wrap(schema, row).get(0, List.class))
        .containsExactly(DateTimeUtil.microsFromTimestamp(time));
  }

  @Test
  void listOfStructsHasIndependentElements() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1,
                "points",
                Types.ListType.ofOptional(
                    2,
                    Types.StructType.of(
                        Types.NestedField.required(3, "x", Types.IntegerType.get())))));
    RowData row =
        GenericRowData.of(
            new GenericArrayData(new Object[] {GenericRowData.of(1), GenericRowData.of(2)}));

    List<?> points = wrap(schema, row).get(0, List.class);
    assertThat(points).hasSize(2);
    StructLike first = (StructLike) points.get(0);
    StructLike second = (StructLike) points.get(1);
    assertThat(first.get(0, Integer.class)).isEqualTo(1);
    assertThat(second.get(0, Integer.class)).isEqualTo(2);
  }

  @Test
  void nestedStructsInListElementsAreIndependent() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1,
                "items",
                Types.ListType.ofOptional(
                    2,
                    Types.StructType.of(
                        Types.NestedField.optional(
                            3,
                            "inner",
                            Types.StructType.of(
                                Types.NestedField.required(4, "x", Types.IntegerType.get())))))));
    RowData row =
        GenericRowData.of(
            new GenericArrayData(
                new Object[] {
                  GenericRowData.of(GenericRowData.of(1)), GenericRowData.of(GenericRowData.of(2))
                }));

    List<?> items = wrap(schema, row).get(0, List.class);
    StructLike first = ((StructLike) items.get(0)).get(0, StructLike.class);
    StructLike second = ((StructLike) items.get(1)).get(0, StructLike.class);

    assertThat(first.get(0, Integer.class)).isEqualTo(1);
    assertThat(second.get(0, Integer.class)).isEqualTo(2);
  }

  @Test
  void listOfUnknownElements() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1, "unknowns", Types.ListType.ofOptional(2, Types.UnknownType.get())));
    RowData row = GenericRowData.of(new GenericArrayData(new Object[] {null}));

    assertThat(wrap(schema, row).get(0, List.class)).containsExactly((Object) null);
  }

  @Test
  void mapKeysAndValuesAreConverted() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1,
                "prices",
                Types.MapType.ofOptional(
                    2, 3, Types.StringType.get(), Types.DecimalType.of(9, 2))));
    BigDecimal price = new BigDecimal("1.50");
    RowData row =
        GenericRowData.of(
            new GenericMapData(
                ImmutableMap.of(
                    StringData.fromString("a"), DecimalData.fromBigDecimal(price, 9, 2))));

    assertThat(wrap(schema, row).get(0, Map.class)).isEqualTo(ImmutableMap.of("a", price));
  }

  @Test
  void emptyListIsConverted() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1, "tags", Types.ListType.ofOptional(2, Types.StringType.get())));
    RowData row = GenericRowData.of(new GenericArrayData(new Object[0]));

    assertThat(wrap(schema, row).get(0, List.class)).isEmpty();
  }

  @Test
  void nestedListIsConverted() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1,
                "matrix",
                Types.ListType.ofOptional(
                    2, Types.ListType.ofOptional(3, Types.IntegerType.get()))));
    RowData row =
        GenericRowData.of(
            new GenericArrayData(
                new Object[] {
                  new GenericArrayData(new Object[] {1, 2}), new GenericArrayData(new Object[] {3})
                }));

    assertThat(wrap(schema, row).get(0, List.class))
        .containsExactly(ImmutableList.of(1, 2), ImmutableList.of(3));
  }

  @Test
  void mapNullValueIsPreserved() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1,
                "attributes",
                Types.MapType.ofOptional(2, 3, Types.StringType.get(), Types.StringType.get())));
    Map<Object, Object> data = Maps.newHashMap();
    data.put(StringData.fromString("k"), null);
    RowData row = GenericRowData.of(new GenericMapData(data));

    assertThat(wrap(schema, row).get(0, Map.class)).hasSize(1).containsEntry("k", null);
  }

  private static StructLike wrap(Schema schema, RowData row) {
    return new RowDataWrapper(FlinkSchemaUtil.convert(schema), schema.asStruct()).wrap(row);
  }
}
