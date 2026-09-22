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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.MetadataColumns;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestChangelogBinaryValues {
  private static final String DELETE = ChangelogOperation.DELETE.name();
  private static final String INSERT = ChangelogOperation.INSERT.name();

  static Stream<Arguments> values() {
    DataType binary = DataTypes.BinaryType;
    DataType array = DataTypes.createArrayType(binary);
    StructType struct = new StructType().add("patch", binary).add("number", DataTypes.IntegerType);
    return Stream.of(
        Arguments.of(binary, (IntFunction<Object>) value -> new byte[] {(byte) value}),
        Arguments.of(
            array, (IntFunction<Object>) value -> Arrays.asList(new byte[] {(byte) value}, null)),
        Arguments.of(
            DataTypes.createArrayType(array),
            (IntFunction<Object>) value -> List.of(List.of(new byte[] {(byte) value}))),
        Arguments.of(
            struct, (IntFunction<Object>) value -> RowFactory.create(new byte[] {(byte) value}, 7)),
        Arguments.of(
            DataTypes.createArrayType(struct),
            (IntFunction<Object>)
                value -> List.of(RowFactory.create(new byte[] {(byte) value}, 7))),
        Arguments.of(
            DataTypes.createMapType(DataTypes.StringType, array),
            (IntFunction<Object>)
                value -> Collections.singletonMap("key", List.of(new byte[] {(byte) value}))),
        Arguments.of(
            DataTypes.createMapType(binary, binary),
            (IntFunction<Object>)
                value -> Collections.singletonMap(new byte[] {1}, new byte[] {(byte) value})),
        Arguments.of(
            DataTypes.createMapType(binary, DataTypes.StringType),
            (IntFunction<Object>)
                value -> Collections.singletonMap(new byte[] {(byte) value}, "value")),
        Arguments.of(
            DataTypes.createMapType(array, binary),
            (IntFunction<Object>)
                value ->
                    Collections.singletonMap(List.of(new byte[] {1}), new byte[] {(byte) value})),
        Arguments.of(
            DataTypes.createArrayType(DataTypes.IntegerType),
            (IntFunction<Object>) value -> Arrays.asList(value, null)),
        Arguments.of(
            DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType),
            (IntFunction<Object>) value -> Collections.singletonMap("key", value)));
  }

  @ParameterizedTest
  @MethodSource("values")
  void removesNetChangesWithEqualValues(DataType type, IntFunction<Object> values) {
    Row insert = row(type, values.apply(1), INSERT, 0);
    Row delete = row(type, values.apply(1), DELETE, 1);
    Row latest = row(type, values.apply(2), INSERT, 1);
    Iterator<Row> result =
        ChangelogIterator.removeNetCarryovers(
            List.of(insert, delete, latest).iterator(), schema(type));
    assertThat(result).toIterable().containsExactly(latest);
  }

  @ParameterizedTest
  @MethodSource("values")
  void removesCarryoversWithEqualValues(DataType type, IntFunction<Object> values) {
    assertRemoved(
        type,
        List.of(row(type, values.apply(1), DELETE, 0), row(type, values.apply(1), INSERT, 0)));
  }

  @ParameterizedTest
  @MethodSource("values")
  void retainsChangesWithDifferentValues(DataType type, IntFunction<Object> values) {
    assertRetained(
        type,
        List.of(row(type, values.apply(1), DELETE, 0), row(type, values.apply(2), INSERT, 0)));
  }

  @ParameterizedTest
  @MethodSource("values")
  void distinguishesNullFromNonNull(DataType type, IntFunction<Object> values) {
    assertRetained(
        type, List.of(row(type, null, DELETE, 0), row(type, values.apply(1), INSERT, 0)));
    assertRetained(
        type, List.of(row(type, values.apply(1), DELETE, 0), row(type, null, INSERT, 0)));
    assertRemoved(type, List.of(row(type, null, DELETE, 0), row(type, null, INSERT, 0)));
  }

  static Stream<Arguments> mapKeys() {
    return Stream.of(
        Arguments.of(DataTypes.StringType, (IntFunction<Object>) value -> "key" + value),
        Arguments.of(
            DataTypes.BinaryType, (IntFunction<Object>) value -> new byte[] {(byte) value}),
        Arguments.of(
            DataTypes.createArrayType(DataTypes.BinaryType),
            (IntFunction<Object>) value -> List.of(new byte[] {(byte) value})));
  }

  @ParameterizedTest
  @MethodSource("mapKeys")
  void ignoresMapEntryOrder(DataType keyType, IntFunction<Object> keys) {
    DataType type = DataTypes.createMapType(keyType, DataTypes.BinaryType);
    Map<Object, Object> left = new LinkedHashMap<>();
    left.put(keys.apply(1), new byte[] {1});
    left.put(keys.apply(2), null);
    Map<Object, Object> right = new LinkedHashMap<>();
    right.put(keys.apply(2), null);
    right.put(keys.apply(1), new byte[] {1});
    assertRemoved(type, List.of(row(type, left, DELETE, 0), row(type, right, INSERT, 0)));
  }

  @Test
  void retainsChangesWithDifferentArrayLengths() {
    DataType type = DataTypes.createArrayType(DataTypes.BinaryType);
    assertRetained(
        type,
        List.of(
            row(type, List.of(new byte[] {1}), DELETE, 0),
            row(type, List.of(new byte[] {1}, new byte[] {1}), INSERT, 0)));
  }

  @Test
  void retainsChangesWithDifferentArrayOrder() {
    DataType type = DataTypes.createArrayType(DataTypes.BinaryType);
    assertRetained(
        type,
        List.of(
            row(type, List.of(new byte[] {1}, new byte[] {2}), DELETE, 0),
            row(type, List.of(new byte[] {2}, new byte[] {1}), INSERT, 0)));
  }

  @ParameterizedTest
  @MethodSource("mapKeys")
  void retainsChangesWithDifferentMapKeysAndNullValues(DataType keyType, IntFunction<Object> keys) {
    DataType type = DataTypes.createMapType(keyType, DataTypes.BinaryType);
    assertRetained(
        type,
        List.of(
            row(type, Collections.singletonMap(keys.apply(1), null), DELETE, 0),
            row(type, Collections.singletonMap(keys.apply(2), null), INSERT, 0)));
  }

  @ParameterizedTest
  @MethodSource("mapKeys")
  void retainsChangesWithDifferentMapSizes(DataType keyType, IntFunction<Object> keys) {
    DataType type = DataTypes.createMapType(keyType, DataTypes.BinaryType);
    Map<Object, Object> larger = new LinkedHashMap<>();
    larger.put(keys.apply(1), new byte[] {1});
    larger.put(keys.apply(2), new byte[] {2});
    assertRetained(
        type,
        List.of(
            row(type, Collections.singletonMap(keys.apply(1), new byte[] {1}), DELETE, 0),
            row(type, larger, INSERT, 0)));
  }

  private static void assertRemoved(DataType type, List<Row> rows) {
    assertThat(ChangelogIterator.removeCarryovers(rows.iterator(), schema(type))).isExhausted();
    assertThat(ChangelogIterator.removeNetCarryovers(rows.iterator(), schema(type))).isExhausted();
  }

  private static void assertRetained(DataType type, List<Row> rows) {
    assertThat(ChangelogIterator.removeCarryovers(rows.iterator(), schema(type)))
        .toIterable()
        .containsExactlyElementsOf(rows);
    assertThat(ChangelogIterator.removeNetCarryovers(rows.iterator(), schema(type)))
        .toIterable()
        .containsExactlyElementsOf(rows);
  }

  private static StructType schema(DataType type) {
    return new StructType()
        .add("id", DataTypes.IntegerType)
        .add("value", type)
        .add(MetadataColumns.CHANGE_TYPE.name(), DataTypes.StringType)
        .add(MetadataColumns.CHANGE_ORDINAL.name(), DataTypes.IntegerType)
        .add(MetadataColumns.COMMIT_SNAPSHOT_ID.name(), DataTypes.LongType);
  }

  private static Row row(DataType type, Object value, String operation, int ordinal) {
    Object internal = CatalystTypeConverters.createToCatalystConverter(type).apply(value);
    Object external = CatalystTypeConverters.createToScalaConverter(type).apply(internal);
    return RowFactory.create(1, external, operation, ordinal, (long) ordinal);
  }
}
