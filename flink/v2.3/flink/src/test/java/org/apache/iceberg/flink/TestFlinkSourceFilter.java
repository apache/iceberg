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

import java.util.stream.Stream;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestFlinkSourceFilter {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(
              2, "tags", Types.ListType.ofOptional(3, Types.StringType.get())),
          Types.NestedField.optional(
              4,
              "attributes",
              Types.MapType.ofOptional(5, 6, Types.StringType.get(), Types.StringType.get())),
          Types.NestedField.optional(
              7,
              "nested",
              Types.StructType.of(
                  Types.NestedField.optional(
                      8, "nested_tags", Types.ListType.ofOptional(9, Types.StringType.get())))));

  private static final RowType ROW_TYPE = FlinkSchemaUtil.convert(SCHEMA);

  private static final RowData ROW =
      GenericRowData.of(
          1L,
          new GenericArrayData(new Object[] {StringData.fromString("a")}),
          new GenericMapData(
              ImmutableMap.of(StringData.fromString("k"), StringData.fromString("v"))),
          GenericRowData.of(new GenericArrayData(new Object[] {StringData.fromString("b")})));

  private static final RowData ROW_WITH_NULL_COLLECTIONS =
      GenericRowData.of(2L, null, null, new GenericRowData(1));

  private static final RowData ROW_WITH_NULL_STRUCT = GenericRowData.of(3L, null, null, null);

  private static Stream<Arguments> collectionColumns() {
    return Stream.of("tags", "attributes", "nested.nested_tags")
        .flatMap(column -> Stream.of(Arguments.of(column, false), Arguments.of(column, true)));
  }

  @ParameterizedTest(name = "{0}, binary={1}")
  @MethodSource("collectionColumns")
  void nonNullCollectionIsNotNull(String column, boolean binary) {
    RowData row = binary ? toBinary(ROW) : ROW;
    assertThat(matches(Expressions.notNull(column), row)).isTrue();
    assertThat(matches(Expressions.isNull(column), row)).isFalse();
  }

  @ParameterizedTest(name = "{0}, binary={1}")
  @MethodSource("collectionColumns")
  void nullCollectionIsNull(String column, boolean binary) {
    RowData row = binary ? toBinary(ROW_WITH_NULL_COLLECTIONS) : ROW_WITH_NULL_COLLECTIONS;
    assertThat(matches(Expressions.isNull(column), row)).isTrue();
    assertThat(matches(Expressions.notNull(column), row)).isFalse();
  }

  @ParameterizedTest(name = "binary={0}")
  @ValueSource(booleans = {false, true})
  void listInNullStructIsNull(boolean binary) {
    RowData row = binary ? toBinary(ROW_WITH_NULL_STRUCT) : ROW_WITH_NULL_STRUCT;
    assertThat(matches(Expressions.isNull("nested.nested_tags"), row)).isTrue();
    assertThat(matches(Expressions.notNull("nested.nested_tags"), row)).isFalse();
  }

  private static boolean matches(Expression expression, RowData row) {
    return new FlinkSourceFilter(SCHEMA, expression, true).filter(row);
  }

  private static RowData toBinary(RowData row) {
    return new RowDataSerializer(ROW_TYPE).toBinaryRow(row);
  }
}
