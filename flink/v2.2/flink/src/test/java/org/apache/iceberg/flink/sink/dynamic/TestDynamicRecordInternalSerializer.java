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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

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
