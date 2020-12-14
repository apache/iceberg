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

package org.apache.iceberg.flink.source.split;

import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

public class IcebergSourceSplitStatusSerializer implements SimpleVersionedSerializer<IcebergSourceSplitStatus> {

  public static final IcebergSourceSplitStatusSerializer INSTANCE = new IcebergSourceSplitStatusSerializer();

  private static final int VERSION = 1;

  private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
      ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(IcebergSourceSplitStatus splitStatus) throws IOException {
    if (splitStatus.serializedFormCache() != null) {
      return splitStatus.serializedFormCache();
    }
    return serializeV1(splitStatus);
  }

  @Override
  public IcebergSourceSplitStatus deserialize(int version, byte[] serialized) throws IOException {
    switch (version) {
      case 1:
        return deserializeV1(serialized);
      default:
        throw new IOException("Unknown version: " + version);
    }
  }

  private byte[] serializeV1(IcebergSourceSplitStatus splitState) throws IOException {
    final DataOutputSerializer out = SERIALIZER_CACHE.get();
    out.writeUTF(splitState.status().name());
    if (splitState.assignedSubtaskId() == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeInt(splitState.assignedSubtaskId());
    }
    final byte[] result = out.getCopyOfBuffer();
    out.clear();
    splitState.serializedFormCache(result);
    return result;
  }

  private IcebergSourceSplitStatus deserializeV1(byte[] serialized) throws IOException {
    final DataInputDeserializer in = new DataInputDeserializer(serialized);
    final IcebergSourceSplitStatus.Status status = IcebergSourceSplitStatus.Status.valueOf(in.readUTF());
    final boolean hasAssignedSubtaskId = in.readBoolean();
    if (hasAssignedSubtaskId) {
      final int subtaskId = in.readInt();
      return new IcebergSourceSplitStatus(status, subtaskId);
    } else {
      return new IcebergSourceSplitStatus(status);
    }
  }
}
