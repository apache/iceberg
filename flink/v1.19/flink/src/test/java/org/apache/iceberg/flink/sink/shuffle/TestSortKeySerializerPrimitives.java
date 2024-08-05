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
package org.apache.iceberg.flink.sink.shuffle;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.DataGenerator;
import org.apache.iceberg.flink.DataGenerators;
import org.apache.iceberg.flink.RowDataWrapper;
import org.junit.jupiter.api.Test;

public class TestSortKeySerializerPrimitives extends TestSortKeySerializerBase {
  private final DataGenerator generator = new DataGenerators.Primitives();

  @Override
  protected Schema schema() {
    return generator.icebergSchema();
  }

  @Override
  protected SortOrder sortOrder() {
    return SortOrder.builderFor(schema())
        .asc("boolean_field")
        .sortBy(Expressions.bucket("int_field", 4), SortDirection.DESC, NullOrder.NULLS_LAST)
        .sortBy(Expressions.truncate("string_field", 2), SortDirection.ASC, NullOrder.NULLS_FIRST)
        .sortBy(Expressions.bucket("uuid_field", 16), SortDirection.ASC, NullOrder.NULLS_FIRST)
        .sortBy(Expressions.hour("ts_with_zone_field"), SortDirection.ASC, NullOrder.NULLS_FIRST)
        .sortBy(Expressions.day("ts_without_zone_field"), SortDirection.ASC, NullOrder.NULLS_FIRST)
        // can not test HeapByteBuffer due to equality test inside SerializerTestBase
        // .sortBy(Expressions.truncate("binary_field", 2), SortDirection.ASC,
        // NullOrder.NULLS_FIRST)
        .build();
  }

  @Override
  protected GenericRowData rowData() {
    return generator.generateFlinkRowData();
  }

  @Test
  public void testSerializationSize() throws Exception {
    RowData rowData =
        GenericRowData.of(StringData.fromString("550e8400-e29b-41d4-a716-446655440000"), 1L);
    RowDataWrapper rowDataWrapper =
        new RowDataWrapper(Fixtures.ROW_TYPE, Fixtures.SCHEMA.asStruct());
    StructLike struct = rowDataWrapper.wrap(rowData);
    SortKey sortKey = Fixtures.SORT_KEY.copy();
    sortKey.wrap(struct);
    SortKeySerializer serializer = new SortKeySerializer(Fixtures.SCHEMA, Fixtures.SORT_ORDER);
    DataOutputSerializer output = new DataOutputSerializer(1024);
    serializer.serialize(sortKey, output);
    byte[] serializedBytes = output.getCopyOfBuffer();
    assertThat(serializedBytes.length)
        .as(
            "Serialized bytes for sort key should be 38 bytes (34 UUID text + 4 byte integer of string length")
        .isEqualTo(38);

    DataInputDeserializer input = new DataInputDeserializer(serializedBytes);
    SortKey deserialized = serializer.deserialize(input);
    assertThat(deserialized).isEqualTo(sortKey);
  }
}
