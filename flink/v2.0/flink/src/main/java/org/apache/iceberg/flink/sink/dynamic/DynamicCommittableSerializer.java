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

/**
 * This serializer is used for serializing the {@link DynamicCommittable} objects between the {@link
 * DynamicWriter} and the {@link DynamicWriteResultAggregator} operator and for sending it down to
 * the {@link DynamicCommitter}.
 */
class DynamicCommittableSerializer implements SimpleVersionedSerializer<DynamicCommittable> {

  private static final int VERSION_1 = 1;
  private static final int VERSION_2 = 2;

  @Override
  public int getVersion() {
    return VERSION_2;
  }

  @Override
  public byte[] serialize(DynamicCommittable committable) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
    committable.key().serializeTo(view);
    view.writeUTF(committable.jobId());
    view.writeUTF(committable.operatorId());
    view.writeLong(committable.checkpointId());

    view.writeInt(committable.manifests().length);
    for (int i = 0; i < committable.manifests().length; i++) {
      view.writeInt(committable.manifests()[i].length);
      view.write(committable.manifests()[i]);
    }

    return out.toByteArray();
  }

  @Override
  public DynamicCommittable deserialize(int version, byte[] serialized) throws IOException {
    if (version == VERSION_1) {
      return deserializeV1(serialized);
    } else if (version == VERSION_2) {
      return deserializeV2(serialized);
    }

    throw new IOException("Unrecognized version or corrupt state: " + version);
  }

  private DynamicCommittable deserializeV1(byte[] serialized) throws IOException {
    DataInputDeserializer view = new DataInputDeserializer(serialized);
    WriteTarget key = WriteTarget.deserializeFrom(view);
    String jobId = view.readUTF();
    String operatorId = view.readUTF();
    long checkpointId = view.readLong();
    int manifestLen = view.readInt();
    byte[] manifestBuf;
    manifestBuf = new byte[manifestLen];
    view.read(manifestBuf);
    return new DynamicCommittable(
        new TableKey(key.tableName(), key.branch()),
        new byte[][] {manifestBuf},
        jobId,
        operatorId,
        checkpointId);
  }

  private DynamicCommittable deserializeV2(byte[] serialized) throws IOException {
    DataInputDeserializer view = new DataInputDeserializer(serialized);
    TableKey key = TableKey.deserializeFrom(view);
    String jobId = view.readUTF();
    String operatorId = view.readUTF();
    long checkpointId = view.readLong();

    byte[][] manifestsBuf = new byte[view.readInt()][];
    for (int i = 0; i < manifestsBuf.length; i++) {
      byte[] manifest = new byte[view.readInt()];
      view.read(manifest);
      manifestsBuf[i] = manifest;
    }

    return new DynamicCommittable(key, manifestsBuf, jobId, operatorId, checkpointId);
  }
}
