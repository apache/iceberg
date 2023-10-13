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
package org.apache.iceberg.flink.sink.committer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.InstantiationUtil;
import org.apache.iceberg.io.WriteResult;

/**
 * This serializer is used for serializing the {@link SinkV2Committable} objects between the Writer
 * and the Aggregator operator and between the Aggregator and the Committer as well.
 *
 * <p>In both cases only the respective part is serialized.
 */
public class SinkV2CommittableSerializer implements SimpleVersionedSerializer<SinkV2Committable> {
  private static final int VERSION_1 = 1;
  private static final int WRITE_RESULT = 0;
  private static final int AGGREGATE = 1;

  @Override
  public int getVersion() {
    return VERSION_1;
  }

  @Override
  public byte[] serialize(SinkV2Committable committable) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);

    if (committable.writeResult() != null) {
      view.writeInt(WRITE_RESULT);
      byte[] result = InstantiationUtil.serializeObject(committable.writeResult());
      view.writeInt(result.length);
      view.write(result);
    } else {
      view.writeInt(AGGREGATE);
      view.writeUTF(committable.jobId());
      view.writeUTF(committable.operatorId());
      view.writeLong(committable.checkpointId());
      view.writeInt(committable.manifest().length);
      view.write(committable.manifest());
    }

    return out.toByteArray();
  }

  @Override
  public SinkV2Committable deserialize(int version, byte[] serialized) throws IOException {
    switch (version) {
      case VERSION_1:
        DataInputDeserializer view = new DataInputDeserializer(serialized);
        int committableType = view.readInt();
        switch (committableType) {
          case WRITE_RESULT:
            int resultLen = view.readInt();
            byte[] resultBuf = new byte[resultLen];
            view.read(resultBuf);
            try {
              WriteResult result =
                  InstantiationUtil.deserializeObject(
                      resultBuf, SinkV2CommittableSerializer.class.getClassLoader());
              return new SinkV2Committable(result);
            } catch (ClassNotFoundException cnc) {
              throw new IOException("Could not deserialize the WriteResult object", cnc);
            }
          case AGGREGATE:
            String jobId = view.readUTF();
            String operatorId = view.readUTF();
            long checkpointId = view.readLong();
            int manifestLen = view.readInt();
            byte[] manifestBuf;
            manifestBuf = new byte[manifestLen];
            view.read(manifestBuf);
            return new SinkV2Committable(manifestBuf, jobId, operatorId, checkpointId);
          default:
            throw new IOException("Unrecognized committable type: " + committableType);
        }
      default:
        throw new IOException("Unrecognized version or corrupt state: " + version);
    }
  }
}
