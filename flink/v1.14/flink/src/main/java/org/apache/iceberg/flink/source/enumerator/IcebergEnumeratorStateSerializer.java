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
package org.apache.iceberg.flink.source.enumerator;

import java.io.IOException;
import java.util.Collection;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitSerializer;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatus;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

@Internal
public class IcebergEnumeratorStateSerializer
    implements SimpleVersionedSerializer<IcebergEnumeratorState> {

  public static final IcebergEnumeratorStateSerializer INSTANCE =
      new IcebergEnumeratorStateSerializer();

  private static final int VERSION = 1;

  private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
      ThreadLocal.withInitial(() -> new DataOutputSerializer(1024));

  private final IcebergEnumeratorPositionSerializer positionSerializer =
      IcebergEnumeratorPositionSerializer.INSTANCE;
  private final IcebergSourceSplitSerializer splitSerializer =
      IcebergSourceSplitSerializer.INSTANCE;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(IcebergEnumeratorState enumState) throws IOException {
    return serializeV1(enumState);
  }

  @Override
  public IcebergEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
    switch (version) {
      case 1:
        return deserializeV1(serialized);
      default:
        throw new IOException("Unknown version: " + version);
    }
  }

  private byte[] serializeV1(IcebergEnumeratorState enumState) throws IOException {
    DataOutputSerializer out = SERIALIZER_CACHE.get();

    out.writeBoolean(enumState.lastEnumeratedPosition() != null);
    if (enumState.lastEnumeratedPosition() != null) {
      out.writeInt(positionSerializer.getVersion());
      byte[] positionBytes = positionSerializer.serialize(enumState.lastEnumeratedPosition());
      out.writeInt(positionBytes.length);
      out.write(positionBytes);
    }

    out.writeInt(splitSerializer.getVersion());
    out.writeInt(enumState.pendingSplits().size());
    for (IcebergSourceSplitState splitState : enumState.pendingSplits()) {
      byte[] splitBytes = splitSerializer.serialize(splitState.split());
      out.writeInt(splitBytes.length);
      out.write(splitBytes);
      out.writeUTF(splitState.status().name());
    }

    byte[] result = out.getCopyOfBuffer();
    out.clear();
    return result;
  }

  private IcebergEnumeratorState deserializeV1(byte[] serialized) throws IOException {
    DataInputDeserializer in = new DataInputDeserializer(serialized);

    IcebergEnumeratorPosition enumeratorPosition = null;
    if (in.readBoolean()) {
      int version = in.readInt();
      byte[] positionBytes = new byte[in.readInt()];
      in.read(positionBytes);
      enumeratorPosition = positionSerializer.deserialize(version, positionBytes);
    }

    int splitSerializerVersion = in.readInt();
    int splitCount = in.readInt();
    Collection<IcebergSourceSplitState> pendingSplits = Lists.newArrayListWithCapacity(splitCount);
    for (int i = 0; i < splitCount; ++i) {
      byte[] splitBytes = new byte[in.readInt()];
      in.read(splitBytes);
      IcebergSourceSplit split = splitSerializer.deserialize(splitSerializerVersion, splitBytes);
      String statusName = in.readUTF();
      pendingSplits.add(
          new IcebergSourceSplitState(split, IcebergSourceSplitStatus.valueOf(statusName)));
    }
    return new IcebergEnumeratorState(enumeratorPosition, pendingSplits);
  }
}
