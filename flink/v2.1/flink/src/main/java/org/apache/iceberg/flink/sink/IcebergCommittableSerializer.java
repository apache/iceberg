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
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * This serializer is used for serializing the {@link IcebergCommittable} objects between the Writer
 * and the Aggregator operator and between the Aggregator and the Committer as well.
 *
 * <p>In both cases only the respective part is serialized.
 */
public class IcebergCommittableSerializer implements SimpleVersionedSerializer<IcebergCommittable> {
  private static final int VERSION = 2;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(IcebergCommittable committable) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
    view.writeUTF(committable.jobId());
    view.writeUTF(committable.operatorId());
    view.writeLong(committable.checkpointId());
    view.writeInt(committable.manifest().length);
    view.write(committable.manifest());

    Map<String, String> metadata = committable.observerMetadata();
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
  public IcebergCommittable deserialize(int version, byte[] serialized) throws IOException {
    if (version == 1) {
      return deserializeV1(serialized);
    } else if (version == 2) {
      return deserializeV2(serialized);
    }
    throw new IOException("Unrecognized version or corrupt state: " + version);
  }

  private IcebergCommittable deserializeV1(byte[] serialized) throws IOException {
    DataInputDeserializer view = new DataInputDeserializer(serialized);
    String jobId = view.readUTF();
    String operatorId = view.readUTF();
    long checkpointId = view.readLong();
    int manifestLen = view.readInt();
    byte[] manifestBuf = new byte[manifestLen];
    view.read(manifestBuf);
    return new IcebergCommittable(manifestBuf, jobId, operatorId, checkpointId);
  }

  private IcebergCommittable deserializeV2(byte[] serialized) throws IOException {
    DataInputDeserializer view = new DataInputDeserializer(serialized);
    String jobId = view.readUTF();
    String operatorId = view.readUTF();
    long checkpointId = view.readLong();
    int manifestLen = view.readInt();
    byte[] manifestBuf = new byte[manifestLen];
    view.read(manifestBuf);

    Map<String, String> metadata = null;
    boolean hasMetadata = view.readBoolean();
    if (hasMetadata) {
      int mapSize = view.readInt();
      metadata = Maps.newHashMapWithExpectedSize(mapSize);
      for (int i = 0; i < mapSize; i++) {
        metadata.put(view.readUTF(), view.readUTF());
      }
    }

    return new IcebergCommittable(manifestBuf, jobId, operatorId, checkpointId, metadata);
  }
}
