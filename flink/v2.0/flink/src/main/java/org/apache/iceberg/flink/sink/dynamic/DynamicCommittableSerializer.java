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

/**
 * This serializer is used for serializing the {@link DynamicCommittable} objects between the {@link
 * DynamicWriter} and the {@link DynamicWriteResultAggregator} operator and for sending it down to
 * the {@link DynamicCommitter}.
 */
class DynamicCommittableSerializer implements SimpleVersionedSerializer<DynamicCommittable> {

  private static final int VERSION = 1;
  private static final WriteResultSerializer WRITE_RESULT_SERIALIZER = new WriteResultSerializer();

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(DynamicCommittable committable) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
    committable.key().serializeTo(view);
    view.writeUTF(committable.jobId());
    view.writeUTF(committable.operatorId());
    view.writeLong(committable.checkpointId());

    byte[] result = WRITE_RESULT_SERIALIZER.serialize(committable.writeResult());
    view.write(result);

    return out.toByteArray();
  }

  @Override
  public DynamicCommittable deserialize(int version, byte[] serialized) throws IOException {
    if (version == 1) {
      DataInputDeserializer view = new DataInputDeserializer(serialized);
      TableKey key = TableKey.deserializeFrom(view);
      String jobId = view.readUTF();
      String operatorId = view.readUTF();
      long checkpointId = view.readLong();

      byte[] resultBuf = new byte[view.available()];
      view.read(resultBuf);
      WriteResult writeResult = WRITE_RESULT_SERIALIZER.deserialize(version, resultBuf);

      return new DynamicCommittable(key, writeResult, jobId, operatorId, checkpointId);
    }

    throw new IOException("Unrecognized version or corrupt state: " + version);
  }
}
