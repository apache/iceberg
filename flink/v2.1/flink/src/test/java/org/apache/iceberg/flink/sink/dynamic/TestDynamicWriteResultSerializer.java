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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.flink.sink.WriteResultSerializer;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class TestDynamicWriteResultSerializer {

  private static final DataFile DATA_FILE =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-1.parquet")
          .withFileSizeInBytes(0)
          .withMetrics(
              new Metrics(
                  42L,
                  null,
                  ImmutableMap.of(1, 5L),
                  ImmutableMap.of(1, 0L),
                  null,
                  ImmutableMap.of(1, ByteBuffer.allocate(1)),
                  ImmutableMap.of(1, ByteBuffer.allocate(1))))
          .build();
  private static final TableKey TABLE_KEY = new TableKey("table", "branch");

  @Test
  void testRoundtrip() throws IOException {
    DynamicWriteResult dynamicWriteResult =
        new DynamicWriteResult(TABLE_KEY, 1, WriteResult.builder().addDataFiles(DATA_FILE).build());

    DynamicWriteResultSerializer serializer = new DynamicWriteResultSerializer();
    DynamicWriteResult copy =
        serializer.deserialize(serializer.getVersion(), serializer.serialize(dynamicWriteResult));

    assertThat(copy.writeResult().dataFiles()).hasSize(1);
    DataFile dataFile = copy.writeResult().dataFiles()[0];
    // DataFile doesn't implement equals, but we can still do basic checks
    assertThat(dataFile.path()).isEqualTo("/path/to/data-1.parquet");
    assertThat(dataFile.recordCount()).isEqualTo(42L);
  }

  @Test
  void testUnsupportedVersion() {
    DynamicWriteResult dynamicWriteResult =
        new DynamicWriteResult(TABLE_KEY, 1, WriteResult.builder().addDataFiles(DATA_FILE).build());

    DynamicWriteResultSerializer serializer = new DynamicWriteResultSerializer();
    assertThatThrownBy(() -> serializer.deserialize(-1, serializer.serialize(dynamicWriteResult)))
        .hasMessage("Unrecognized version or corrupt state: -1")
        .isInstanceOf(IOException.class);
  }

  @Test
  void testDeserializeV1Format() throws IOException {
    WriteResult writeResult = WriteResult.builder().addDataFiles(DATA_FILE).build();
    int expectedSpecId = 3;

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
    // V1 format: WriteTarget serialization (tableName, branch, schemaId, specId, upsertMode,
    // equalityFields) followed by WriteResult bytes
    view.writeUTF(TABLE_KEY.tableName());
    view.writeUTF(TABLE_KEY.branch());
    view.writeInt(1); // schemaId
    view.writeInt(expectedSpecId); // specId
    view.writeBoolean(false); // upsertMode
    view.writeInt(0); // equalityFields count
    view.write(new WriteResultSerializer().serialize(writeResult));

    DynamicWriteResultSerializer serializer = new DynamicWriteResultSerializer();
    DynamicWriteResult deserialized = serializer.deserialize(1, out.toByteArray());

    assertThat(deserialized.key()).isEqualTo(TABLE_KEY);
    assertThat(deserialized.specId()).isEqualTo(expectedSpecId);
    assertThat(deserialized.writeResult().dataFiles()).hasSize(1);
    assertThat(deserialized.writeResult().dataFiles()[0].path())
        .isEqualTo("/path/to/data-1.parquet");
    assertThat(deserialized.writeResult().dataFiles()[0].recordCount()).isEqualTo(42L);
  }

  /**
   * Verifies that a checkpoint produced by the buggy 1.11.0 (new TableKey + specId format, tagged
   * version 1) can be correctly restored on the fixed version. The fixed deserializer detects the
   * format by sniffing for the Java serialization magic bytes (0xACED) 4 bytes after tableName and
   * branch.
   */
  @Test
  void testBuggyV1BytesRestoredWithFixedDeserializer() throws IOException {
    WriteResult writeResult = WriteResult.builder().addDataFiles(DATA_FILE).build();
    int specId = 3;

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
    view.writeUTF(TABLE_KEY.tableName());
    view.writeUTF(TABLE_KEY.branch());
    view.writeInt(specId);
    view.write(new WriteResultSerializer().serialize(writeResult));
    byte[] buggyV1Bytes = out.toByteArray();

    DynamicWriteResultSerializer fixedSerializer = new DynamicWriteResultSerializer();
    DynamicWriteResult deserialized = fixedSerializer.deserialize(1, buggyV1Bytes);

    assertThat(deserialized.key()).isEqualTo(TABLE_KEY);
    assertThat(deserialized.specId()).isEqualTo(specId);
    assertThat(deserialized.writeResult().dataFiles()).hasSize(1);
    assertThat(deserialized.writeResult().dataFiles()[0].path())
        .isEqualTo("/path/to/data-1.parquet");
    assertThat(deserialized.writeResult().dataFiles()[0].recordCount()).isEqualTo(42L);
  }

  /**
   * Verifies that the old V1 format (WriteTarget layout) with {@code specId=0} is not misidentified
   * as the new format. When {@code specId=0}, the 4 bytes after tableName+branch are {@code
   * 0x00000000} (schemaId=0), and the 2 bytes after that are the first 2 bytes of the actual specId
   * — which must not match the Java serialization magic {@code 0xACED}.
   */
  @Test
  void testOldV1FormatWithZeroSpecIdNotMisdetected() throws IOException {
    WriteResult writeResult = WriteResult.builder().addDataFiles(DATA_FILE).build();
    int expectedSpecId = 0;

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
    view.writeUTF(TABLE_KEY.tableName());
    view.writeUTF(TABLE_KEY.branch());
    view.writeInt(0); // schemaId
    view.writeInt(expectedSpecId); // specId
    view.writeBoolean(false); // upsertMode
    view.writeInt(0); // equalityFields count
    view.write(new WriteResultSerializer().serialize(writeResult));

    DynamicWriteResultSerializer serializer = new DynamicWriteResultSerializer();
    DynamicWriteResult deserialized = serializer.deserialize(1, out.toByteArray());

    assertThat(deserialized.key()).isEqualTo(TABLE_KEY);
    assertThat(deserialized.specId()).isEqualTo(expectedSpecId);
    assertThat(deserialized.writeResult().dataFiles()).hasSize(1);
    assertThat(deserialized.writeResult().dataFiles()[0].path())
        .isEqualTo("/path/to/data-1.parquet");
    assertThat(deserialized.writeResult().dataFiles()[0].recordCount()).isEqualTo(42L);
  }
}
