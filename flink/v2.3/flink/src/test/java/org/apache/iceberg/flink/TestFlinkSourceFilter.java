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

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

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

  private static final RowData ROW =
      GenericRowData.of(
          1L,
          new GenericArrayData(new Object[] {StringData.fromString("a")}),
          new GenericMapData(
              ImmutableMap.of(StringData.fromString("k"), StringData.fromString("v"))),
          GenericRowData.of(new GenericArrayData(new Object[] {StringData.fromString("b")})));

  @Test
  void notNullOnListColumn() {
    FlinkSourceFilter filter = new FlinkSourceFilter(SCHEMA, Expressions.notNull("tags"), true);
    assertThat(filter.filter(ROW)).isTrue();
  }

  @Test
  void isNullOnListColumn() {
    FlinkSourceFilter filter = new FlinkSourceFilter(SCHEMA, Expressions.isNull("tags"), true);
    assertThat(filter.filter(ROW)).isFalse();
  }

  @Test
  void notNullOnMapColumn() {
    FlinkSourceFilter filter =
        new FlinkSourceFilter(SCHEMA, Expressions.notNull("attributes"), true);
    assertThat(filter.filter(ROW)).isTrue();
  }

  @Test
  void notNullOnListNestedInStruct() {
    FlinkSourceFilter filter =
        new FlinkSourceFilter(SCHEMA, Expressions.notNull("nested.nested_tags"), true);
    assertThat(filter.filter(ROW)).isTrue();
  }
}
