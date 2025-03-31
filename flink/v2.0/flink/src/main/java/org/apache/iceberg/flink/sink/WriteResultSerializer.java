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
import org.apache.flink.util.InstantiationUtil;
import org.apache.iceberg.io.WriteResult;

class WriteResultSerializer implements SimpleVersionedSerializer<WriteResult> {
  private static final int VERSION = 1;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(WriteResult writeResult) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
    byte[] result = InstantiationUtil.serializeObject(writeResult);
    view.write(result);
    return out.toByteArray();
  }

  @Override
  public WriteResult deserialize(int version, byte[] serialized) throws IOException {
    if (version == 1) {
      DataInputDeserializer view = new DataInputDeserializer(serialized);
      byte[] resultBuf = new byte[serialized.length];
      view.read(resultBuf);
      try {
        return InstantiationUtil.deserializeObject(
            resultBuf, IcebergCommittableSerializer.class.getClassLoader());
      } catch (ClassNotFoundException cnc) {
        throw new IOException("Could not deserialize the WriteResult object", cnc);
      }
    }
    throw new IOException("Unrecognized version or corrupt state: " + version);
  }
}
