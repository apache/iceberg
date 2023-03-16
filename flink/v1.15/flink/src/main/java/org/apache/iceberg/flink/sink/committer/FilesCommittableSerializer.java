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

public class FilesCommittableSerializer implements SimpleVersionedSerializer<FilesCommittable> {
  private static final int VERSION_1 = 1;

  @Override
  public int getVersion() {
    return VERSION_1;
  }

  @Override
  public byte[] serialize(FilesCommittable committable) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);

    view.writeUTF(committable.jobID());
    view.writeLong(committable.checkpointId());
    view.writeInt(committable.subtaskId());
    view.writeInt(committable.manifest().length);
    view.write(committable.manifest());
    return out.toByteArray();
  }

  @Override
  public FilesCommittable deserialize(int version, byte[] serialized) throws IOException {
    switch (version) {
      case VERSION_1:
        DataInputDeserializer view = new DataInputDeserializer(serialized);
        String jobID = view.readUTF();
        long checkpointId = view.readLong();
        int subtaskId = view.readInt();
        int len = view.readInt();
        byte[] buf = new byte[len];
        view.read(buf);
        return new FilesCommittable(buf, jobID, checkpointId, subtaskId);
      default:
        throw new IOException("Unrecognized version or corrupt state: " + version);
    }
  }
}
