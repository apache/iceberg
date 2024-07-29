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

import static org.apache.iceberg.flink.sink.shuffle.Fixtures.ROW_TYPE;
import static org.apache.iceberg.flink.sink.shuffle.Fixtures.SCHEMA;
import static org.apache.iceberg.flink.sink.shuffle.Fixtures.SORT_KEY;
import static org.apache.iceberg.flink.sink.shuffle.Fixtures.SORT_ORDER;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.IOException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestSortKeySerializerSnapshot {
  private final Schema schema =
      new Schema(
          Types.NestedField.optional(1, "ts", Types.TimestampType.withoutZone()),
          Types.NestedField.optional(2, "str", Types.StringType.get()),
          Types.NestedField.optional(3, "int", Types.IntegerType.get()),
          Types.NestedField.optional(4, "boolean", Types.BooleanType.get()));
  private final SortOrder sortOrder = SortOrder.builderFor(schema).asc("str").asc("int").build();

  @Test
  public void testRestoredSerializer() throws Exception {
    RowData rowData = GenericRowData.of(StringData.fromString("str"), 1);
    RowDataWrapper rowDataWrapper = new RowDataWrapper(ROW_TYPE, SCHEMA.asStruct());
    StructLike struct = rowDataWrapper.wrap(rowData);
    SortKey sortKey = SORT_KEY.copy();
    sortKey.wrap(struct);

    SortKeySerializer originalSerializer = new SortKeySerializer(SCHEMA, SORT_ORDER);
    TypeSerializerSnapshot<SortKey> snapshot =
        roundTrip(originalSerializer.snapshotConfiguration());
    TypeSerializer<SortKey> restoredSerializer = snapshot.restoreSerializer();

    DataOutputSerializer output = new DataOutputSerializer(1024);
    originalSerializer.serialize(sortKey, output);
    byte[] serializedBytes = output.getCopyOfBuffer();

    DataInputDeserializer input = new DataInputDeserializer(serializedBytes);
    SortKey deserialized = restoredSerializer.deserialize(input);
    assertThat(deserialized).isEqualTo(sortKey);
  }

  @Test
  public void testSnapshotIsCompatibleWithSameSortOrder() throws Exception {
    SortKeySerializer oldSerializer = new SortKeySerializer(schema, sortOrder);
    SortKeySerializer.SortKeySerializerSnapshot oldSnapshot =
        roundTrip(oldSerializer.snapshotConfiguration());

    SortKeySerializer newSerializer = new SortKeySerializer(schema, sortOrder);

    TypeSerializerSchemaCompatibility<SortKey> resultCompatibility =
        oldSnapshot.resolveSchemaCompatibility(newSerializer);
    assertThat(resultCompatibility.isCompatibleAsIs()).isTrue();
  }

  @Test
  public void testSnapshotIsCompatibleWithRemoveNonSortField() throws Exception {
    SortKeySerializer oldSerializer = new SortKeySerializer(schema, sortOrder);
    SortKeySerializer.SortKeySerializerSnapshot oldSnapshot =
        roundTrip(oldSerializer.snapshotConfiguration());

    // removed non-sort boolean field
    Schema newSchema =
        new Schema(
            Types.NestedField.optional(1, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(2, "str", Types.StringType.get()),
            Types.NestedField.optional(3, "int", Types.IntegerType.get()));
    SortOrder newSortOrder = SortOrder.builderFor(newSchema).asc("str").asc("int").build();
    SortKeySerializer newSerializer = new SortKeySerializer(newSchema, newSortOrder);

    TypeSerializerSchemaCompatibility<SortKey> resultCompatibility =
        oldSnapshot.resolveSchemaCompatibility(newSerializer);
    assertThat(resultCompatibility.isCompatibleAsIs()).isTrue();
  }

  @Test
  public void testSnapshotIsCompatibleWithAddNonSortField() throws Exception {
    SortKeySerializer oldSerializer = new SortKeySerializer(schema, sortOrder);
    SortKeySerializer.SortKeySerializerSnapshot oldSnapshot =
        roundTrip(oldSerializer.snapshotConfiguration());

    // add a new non-sort float field
    Schema newSchema =
        new Schema(
            Types.NestedField.optional(1, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(2, "str", Types.StringType.get()),
            Types.NestedField.optional(3, "int", Types.IntegerType.get()),
            Types.NestedField.optional(4, "boolean", Types.BooleanType.get()),
            Types.NestedField.required(5, "float", Types.FloatType.get()));
    SortOrder newSortOrder = SortOrder.builderFor(newSchema).asc("str").asc("int").build();
    SortKeySerializer newSerializer = new SortKeySerializer(newSchema, newSortOrder);

    TypeSerializerSchemaCompatibility<SortKey> resultCompatibility =
        oldSnapshot.resolveSchemaCompatibility(newSerializer);
    assertThat(resultCompatibility.isCompatibleAsIs()).isTrue();
  }

  @Test
  public void testSnapshotIsIncompatibleWithIncompatibleSchema() throws Exception {
    SortKeySerializer oldSerializer = new SortKeySerializer(schema, sortOrder);
    SortKeySerializer.SortKeySerializerSnapshot oldSnapshot =
        roundTrip(oldSerializer.snapshotConfiguration());

    // change str field to a long type
    Schema newSchema =
        new Schema(
            Types.NestedField.optional(1, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(2, "str", Types.LongType.get()),
            Types.NestedField.optional(3, "int", Types.IntegerType.get()),
            Types.NestedField.optional(4, "boolean", Types.BooleanType.get()));
    SortOrder newSortOrder = SortOrder.builderFor(newSchema).asc("str").asc("int").build();
    // switch sort field order
    SortKeySerializer newSerializer = new SortKeySerializer(newSchema, newSortOrder);

    TypeSerializerSchemaCompatibility<SortKey> resultCompatibility =
        oldSnapshot.resolveSchemaCompatibility(newSerializer);
    assertThat(resultCompatibility.isIncompatible()).isTrue();
  }

  @Test
  public void testSnapshotIsIncompatibleWithAddSortField() throws Exception {
    SortKeySerializer oldSerializer = new SortKeySerializer(schema, sortOrder);
    SortKeySerializer.SortKeySerializerSnapshot oldSnapshot =
        roundTrip(oldSerializer.snapshotConfiguration());

    // removed str field from sort order
    SortOrder newSortOrder =
        SortOrder.builderFor(schema).asc("str").asc("int").desc("boolean").build();
    SortKeySerializer newSerializer = new SortKeySerializer(schema, newSortOrder);

    TypeSerializerSchemaCompatibility<SortKey> resultCompatibility =
        oldSnapshot.resolveSchemaCompatibility(newSerializer);
    assertThat(resultCompatibility.isIncompatible()).isTrue();
  }

  @Test
  public void testSnapshotIsIncompatibleWithRemoveSortField() throws Exception {
    SortKeySerializer oldSerializer = new SortKeySerializer(schema, sortOrder);
    SortKeySerializer.SortKeySerializerSnapshot oldSnapshot =
        roundTrip(oldSerializer.snapshotConfiguration());

    // remove str field from sort order
    SortOrder newSortOrder = SortOrder.builderFor(schema).asc("int").build();
    SortKeySerializer newSerializer = new SortKeySerializer(schema, newSortOrder);

    TypeSerializerSchemaCompatibility<SortKey> resultCompatibility =
        oldSnapshot.resolveSchemaCompatibility(newSerializer);
    assertThat(resultCompatibility.isIncompatible()).isTrue();
  }

  @Test
  public void testSnapshotIsIncompatibleWithSortFieldsOrderChange() throws Exception {
    SortKeySerializer oldSerializer = new SortKeySerializer(schema, sortOrder);
    SortKeySerializer.SortKeySerializerSnapshot oldSnapshot =
        roundTrip(oldSerializer.snapshotConfiguration());

    // switch sort field order
    SortOrder newSortOrder = SortOrder.builderFor(schema).asc("int").asc("str").build();
    SortKeySerializer newSerializer = new SortKeySerializer(schema, newSortOrder);

    TypeSerializerSchemaCompatibility<SortKey> resultCompatibility =
        oldSnapshot.resolveSchemaCompatibility(newSerializer);
    assertThat(resultCompatibility.isIncompatible()).isTrue();
  }

  /** Copied from Flink {@code AvroSerializerSnapshotTest} */
  private static SortKeySerializer.SortKeySerializerSnapshot roundTrip(
      TypeSerializerSnapshot<SortKey> original) throws IOException {
    // writeSnapshot();
    DataOutputSerializer out = new DataOutputSerializer(1024);
    original.writeSnapshot(out);
    // init
    SortKeySerializer.SortKeySerializerSnapshot restored =
        new SortKeySerializer.SortKeySerializerSnapshot();
    // readSnapshot();
    DataInputView in = new DataInputDeserializer(out.wrapAsByteBuffer());
    restored.readSnapshot(restored.getCurrentVersion(), in, original.getClass().getClassLoader());
    return restored;
  }
}
