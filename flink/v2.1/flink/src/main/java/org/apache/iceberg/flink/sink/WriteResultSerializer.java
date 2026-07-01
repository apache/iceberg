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
package org.apache.iceberg.flink.sink;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.InstantiationUtil;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

@Internal
public class WriteResultSerializer implements SimpleVersionedSerializer<WriteResult> {
  private static final int VERSION = 2;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(WriteResult writeResult) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);

    byte[] result = InstantiationUtil.serializeObject(writeResult);
    view.writeInt(result.length);
    view.write(result);

    Map<String, String> metadata = WriteObserverMetadataHolder.getAndClear();
    if (metadata != null && !metadata.isEmpty()) {
      view.writeBoolean(true);
      view.writeInt(metadata.size());
      for (Map.Entry<String, String> entry : metadata.entrySet()) {
        view.writeUTF(entry.getKey());
        view.writeUTF(entry.getValue());
      }
    } else {
      view.writeBoolean(false);
    }

    return out.toByteArray();
  }

  @Override
  public WriteResult deserialize(int version, byte[] serialized) throws IOException {
    if (version == 1) {
      return deserializeV1(serialized);
    } else if (version == 2) {
      return deserializeV2(serialized);
    }
    throw new IOException("Unrecognized version or corrupt state: " + version);
  }

  private WriteResult deserializeV1(byte[] serialized) throws IOException {
    DataInputDeserializer view = new DataInputDeserializer(serialized);
    byte[] resultBuf = new byte[serialized.length];
    view.read(resultBuf);
    try {
      return InstantiationUtil.deserializeObject(
          resultBuf, WriteResultSerializer.class.getClassLoader());
    } catch (ClassNotFoundException cnc) {
      throw new IOException("Could not deserialize the WriteResult object", cnc);
    }
  }

  private WriteResult deserializeV2(byte[] serialized) throws IOException {
    DataInputDeserializer view = new DataInputDeserializer(serialized);

    int resultLen = view.readInt();
    byte[] resultBuf = new byte[resultLen];
    view.read(resultBuf);
    WriteResult writeResult;
    try {
      writeResult =
          InstantiationUtil.deserializeObject(
              resultBuf, WriteResultSerializer.class.getClassLoader());
    } catch (ClassNotFoundException cnc) {
      throw new IOException("Could not deserialize the WriteResult object", cnc);
    }

    boolean hasMetadata = view.readBoolean();
    if (hasMetadata) {
      int mapSize = view.readInt();
      Map<String, String> metadata = Maps.newHashMapWithExpectedSize(mapSize);
      for (int i = 0; i < mapSize; i++) {
        metadata.put(view.readUTF(), view.readUTF());
      }
      WriteObserverMetadataHolder.set(metadata);
    }

    return writeResult;
  }
}
