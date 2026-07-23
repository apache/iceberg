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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.iceberg.flink.sink.WriteResultSerializer;
import org.apache.iceberg.io.WriteResult;

class DynamicWriteResultSerializer implements SimpleVersionedSerializer<DynamicWriteResult> {

  private static final int VERSION_1 = 1;
  private static final int VERSION_2 = 2;
  private static final int JAVA_SERIALIZATION_MAGIC = 0xACED;
  private static final WriteResultSerializer WRITE_RESULT_SERIALIZER = new WriteResultSerializer();

  @Override
  public int getVersion() {
    return VERSION_2;
  }

  @Override
  public byte[] serialize(DynamicWriteResult writeResult) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
    writeResult.key().serializeTo(view);
    view.writeInt(writeResult.specId());
    byte[] result = WRITE_RESULT_SERIALIZER.serialize(writeResult.writeResult());
    view.write(result);
    return out.toByteArray();
  }

  @Override
  public DynamicWriteResult deserialize(int version, byte[] serialized) throws IOException {
    if (version == VERSION_1) {
      return deserializeV1(serialized);
    } else if (version == VERSION_2) {
      return deserializeV2(serialized);
    }

    throw new IOException("Unrecognized version or corrupt state: " + version);
  }

  /**
   * Deserializes version-1 bytes, which are ambiguous: Iceberg 1.10.x wrote the old layout
   * tagged as version 1, while the buggy 1.11.0 wrote the new layout also tagged as version 1.
   * This method distinguishes the two by sniffing for the Java serialization magic bytes
   * ({@code 0xACED}) at the expected position where the {@code WriteResult} payload would begin
   * in the new format.
   */
  private DynamicWriteResult deserializeV1(byte[] serialized) throws IOException {
    DataInputDeserializer view = new DataInputDeserializer(serialized);
    view.readUTF();
    view.readUTF();

    if (isV1NewFormat(view)) {
      return deserializeV2(serialized);
    } else {
      return deserializeV1WriteTarget(serialized);
    }
  }

  private boolean isV1NewFormat(DataInputDeserializer view) throws IOException {
    if (view.available() < 6) {
      return false;
    }
    view.skipBytes(4);
    int magic = view.readUnsignedShort();
    return magic == JAVA_SERIALIZATION_MAGIC;
  }

  private DynamicWriteResult deserializeV1WriteTarget(byte[] serialized) throws IOException {
    DataInputDeserializer view = new DataInputDeserializer(serialized);
    WriteTarget key = WriteTarget.deserializeFrom(view);
    byte[] resultBuf = new byte[view.available()];
    view.read(resultBuf);
    WriteResult writeResult = WRITE_RESULT_SERIALIZER.deserialize(VERSION_1, resultBuf);
    return new DynamicWriteResult(
        new TableKey(key.tableName(), key.branch()), key.specId(), writeResult);
  }

  private DynamicWriteResult deserializeV2(byte[] serialized) throws IOException {
    DataInputDeserializer view = new DataInputDeserializer(serialized);
    TableKey key = TableKey.deserializeFrom(view);
    int specId = view.readInt();
    byte[] resultBuf = new byte[view.available()];
    view.read(resultBuf);
    WriteResult writeResult = WRITE_RESULT_SERIALIZER.deserialize(VERSION_1, resultBuf);
    return new DynamicWriteResult(key, specId, writeResult);
  }
}
