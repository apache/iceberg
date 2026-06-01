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
package org.apache.iceberg.vortex;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.data.vortex.GenericVortexReaders;
import org.apache.iceberg.data.vortex.GenericVortexWriter;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestVortexUuid {
  private static final Schema SCHEMA =
      new Schema(required(1, "id", Types.LongType.get()), required(2, "uid", Types.UUIDType.get()));

  @Test
  public void testIcebergUuidMapsToArrowUuidExtension() {
    org.apache.arrow.vector.types.pojo.Schema arrowSchema = VortexSchemas.toArrowSchema(SCHEMA);
    Field uuidField = arrowSchema.findField("uid");
    assertThat(VortexSchemas.isUuidField(uuidField)).isTrue();
  }

  @Test
  public void testArrowUuidExtensionMapsBackToIcebergUuid() {
    Schema roundTrip = VortexSchemas.convert(VortexSchemas.toArrowSchema(SCHEMA));
    assertThat(roundTrip.findType("uid")).isEqualTo(Types.UUIDType.get());
  }

  @Test
  public void testRelocatedArrowUuidExtensionMapsBackToIcebergUuid() {
    dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Schema vortexSchema =
        VortexSchemas.toVortexArrowSchema(SCHEMA);
    dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field uuidField =
        vortexSchema.findField("uid");

    assertThat(VortexSchemas.isUuidField(uuidField)).isTrue();
    assertThat(VortexSchemas.convert(vortexSchema).findType("uid")).isEqualTo(Types.UUIDType.get());
    assertThat(
            VortexSchemas.isUuidField(VortexSchemas.toArrowSchema(vortexSchema).findField("uid")))
        .isTrue();
  }

  @Test
  public void testWriteAndReadUuidRoundTrip() {
    UUID uuid1 = UUID.fromString("00000000-0000-0000-0000-000000000001");
    UUID uuid2 = UUID.fromString("11111111-2222-3333-4444-555555555555");

    org.apache.arrow.vector.types.pojo.Schema arrowSchema = VortexSchemas.toArrowSchema(SCHEMA);

    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      root.allocateNew();
      GenericVortexWriter writer = (GenericVortexWriter) GenericVortexWriter.buildWriter(SCHEMA);
      writer.write(makeRecord(1L, uuid1), root, 0);
      writer.write(makeRecord(2L, uuid2), root, 1);
      root.setRowCount(2);

      // Read back using the GenericVortexReader's primitive dispatch through GenericVortexReaders.
      VortexRowReader<Record> reader =
          GenericVortexReader.buildReader(SCHEMA, VortexSchemas.toArrowSchema(SCHEMA));
      Record row0 = reader.read(root, 0);
      Record row1 = reader.read(root, 1);

      assertThat(row0.get(0)).isEqualTo(1L);
      assertThat(row0.get(1)).isEqualTo(uuid1);
      assertThat(row1.get(0)).isEqualTo(2L);
      assertThat(row1.get(1)).isEqualTo(uuid2);
    }
  }

  @Test
  public void testGenericUuidReaderHandlesPlainFixedSizeBinary() {
    // When the arrow.uuid extension type is not registered, Vortex returns FixedSizeBinary(16).
    // The reader should still produce a UUID by reading the underlying bytes.
    UUID uuid = UUID.fromString("deadbeef-1234-5678-9abc-0123456789ab");
    org.apache.arrow.vector.types.pojo.Schema fixedSchema =
        new org.apache.arrow.vector.types.pojo.Schema(
            java.util.List.of(
                new Field(
                    "uid",
                    new org.apache.arrow.vector.types.pojo.FieldType(
                        false,
                        new org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary(16),
                        null),
                    null)));

    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(fixedSchema, allocator)) {
      root.allocateNew();
      ((org.apache.arrow.vector.FixedSizeBinaryVector) root.getVector(0))
          .setSafe(0, org.apache.iceberg.util.UUIDUtil.convert(uuid));
      root.setRowCount(1);

      assertThat(GenericVortexReaders.uuids().read(root.getVector(0), 0)).isEqualTo(uuid);
    }
  }

  private static Record makeRecord(long id, UUID uuid) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.set(0, id);
    record.set(1, uuid);
    return record;
  }
}
