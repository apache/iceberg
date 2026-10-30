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
package org.apache.iceberg.flink.sink.dynamic;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Collections;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestDynamicRecordInternalSerializer {

  @RegisterExtension
  static final HadoopCatalogExtension CATALOG_EXTENSION = new HadoopCatalogExtension("db", "table");

  @Test
  void testCurrentTypeSerializerSnapshotVersion() {
    TypeSerializer<DynamicRecordInternal> serializer = createSerializer();
    assertThat(serializer).isNotNull().isInstanceOf(DynamicRecordInternalSerializer.class);
    TypeSerializerSnapshot<DynamicRecordInternal> snapshot = serializer.snapshotConfiguration();
    assertThat(snapshot.getCurrentVersion()).isEqualTo(1);
  }

  @Test
  void testCurrentTypeSerializerSnapshotCompatibility() {
    TypeSerializer<DynamicRecordInternal> serializer = createSerializer();
    assertThat(serializer).isNotNull().isInstanceOf(DynamicRecordInternalSerializer.class);
    TypeSerializerSnapshot<DynamicRecordInternal> snapshot = serializer.snapshotConfiguration();
    assertThat(
            snapshot
                .resolveSchemaCompatibility(serializer.snapshotConfiguration())
                .isCompatibleAsIs())
        .isTrue();
  }

  @Test
  void testRestoreFromOldVersion() throws IOException {
    // Create a serialized snapshot of the TypeSerializer
    final int oldVersion = 0;
    OldTypeSerializerSnapshot oldTypeSerializerSnapshot = new OldTypeSerializerSnapshot(oldVersion);
    assertThat(oldTypeSerializerSnapshot.getCurrentVersion()).isEqualTo(oldVersion);
    DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(128);
    oldTypeSerializerSnapshot.writeSnapshot(dataOutputSerializer);

    // Load the serialized state
    DynamicRecordInternalSerializer.DynamicRecordInternalTypeSerializerSnapshot restoreSnapshot =
        (DynamicRecordInternalSerializer.DynamicRecordInternalTypeSerializerSnapshot)
            createSerializer().snapshotConfiguration();
    restoreSnapshot.readSnapshot(
        oldVersion,
        new DataInputDeserializer(dataOutputSerializer.getSharedBuffer()),
        getClass().getClassLoader());
    // Check that it matches the original one
    assertThat(restoreSnapshot.getCurrentVersion()).isEqualTo(oldVersion);
    assertThat(
            restoreSnapshot
                .resolveSchemaCompatibility(oldTypeSerializerSnapshot)
                .isCompatibleAsIs())
        .isTrue();
    TypeSerializer<DynamicRecordInternal> restoreSerializer = restoreSnapshot.restoreSerializer();
    assertThat(restoreSerializer).isInstanceOf(DynamicRecordInternalSerializer.class);
    assertThat(((DynamicRecordInternalSerializer) restoreSerializer).getSerializerCache())
        .isNotNull();

    // Compare against the latest version of a snapshot
    TypeSerializerSnapshot<DynamicRecordInternal> latestVersion =
        createSerializer().snapshotConfiguration();
    assertThat(latestVersion.getCurrentVersion()).isEqualTo(1);
    assertThat(
            latestVersion
                .resolveSchemaCompatibility(oldTypeSerializerSnapshot)
                .isCompatibleAfterMigration())
        .isTrue();
    assertThat(
            latestVersion.resolveSchemaCompatibility(restoreSnapshot).isCompatibleAfterMigration())
        .isTrue();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void copyPreservesValues(boolean reuse) {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()));
    long value = 123L;
    GenericRowData row = GenericRowData.of(value);
    DynamicRecordInternal copied = copy(schema, row, reuse);

    row.setField(0, value + 1);

    assertThat(copied.rowData().getLong(0)).isEqualTo(value);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void copyPreservesNestedValues(boolean reuse) {
    Schema schema =
        new Schema(
            required(
                1, "nested", Types.StructType.of(required(2, "data", Types.BinaryType.get()))));
    byte[] value = new byte[] {1, 2, 3};
    byte[] expected = value.clone();
    GenericRowData row = GenericRowData.of(GenericRowData.of(value));
    DynamicRecordInternal copied = copy(schema, row, reuse);

    value[0] += 1;

    assertThat(copied.rowData().getRow(0, 1).getBinary(0)).isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void copyPreservesRowKind(boolean reuse) {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()));
    GenericRowData row = GenericRowData.of(123L);
    row.setRowKind(RowKind.UPDATE_BEFORE);
    DynamicRecordInternal copied = copy(schema, row, reuse);

    row.setRowKind(RowKind.UPDATE_AFTER);

    assertThat(copied.rowData().getRowKind()).isEqualTo(RowKind.UPDATE_BEFORE);
  }

  private DynamicRecordInternal copy(Schema schema, RowData row, boolean reuse) {
    DynamicRecordInternalSerializer serializer = createSerializer();
    DynamicRecordInternal record =
        new DynamicRecordInternal(
            "table",
            "main",
            schema,
            row,
            PartitionSpec.unpartitioned(),
            0,
            false,
            Collections.emptySet());
    return reuse ? serializer.copy(record, serializer.createInstance()) : serializer.copy(record);
  }

  private DynamicRecordInternalSerializer createSerializer() {
    return new DynamicRecordInternalSerializer(
        new TableSerializerCache(CATALOG_EXTENSION.catalogLoader(), 1), true);
  }

  private static class OldTypeSerializerSnapshot
      extends DynamicRecordInternalSerializer.DynamicRecordInternalTypeSerializerSnapshot {

    private final int version;

    OldTypeSerializerSnapshot(int version) {
      this.version = version;
    }

    @Override
    public int getCurrentVersion() {
      return version;
    }
  }
}
