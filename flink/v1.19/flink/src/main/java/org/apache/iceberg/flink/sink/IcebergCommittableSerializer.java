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
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

/**
 * This serializer is used for serializing the {@link IcebergCommittable} objects between the Writer
 * and the Aggregator operator and between the Aggregator and the Committer as well.
 *
 * <p>In both cases only the respective part is serialized.
 */
class IcebergCommittableSerializer implements SimpleVersionedSerializer<IcebergCommittable> {
  private static final int VERSION = 1;

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
    return out.toByteArray();
  }

  @Override
  public IcebergCommittable deserialize(int version, byte[] serialized) throws IOException {
    if (version == 1) {
      DataInputDeserializer view = new DataInputDeserializer(serialized);
      String jobId = view.readUTF();
      String operatorId = view.readUTF();
      long checkpointId = view.readLong();
      int manifestLen = view.readInt();
      byte[] manifestBuf;
      manifestBuf = new byte[manifestLen];
      view.read(manifestBuf);
      return new IcebergCommittable(manifestBuf, jobId, operatorId, checkpointId);
    }
    throw new IOException("Unrecognized version or corrupt state: " + version);
  }
}
