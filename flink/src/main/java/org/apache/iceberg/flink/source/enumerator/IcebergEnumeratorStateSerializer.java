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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitSerializer;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatus;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatusSerializer;

public class IcebergEnumeratorStateSerializer implements SimpleVersionedSerializer<IcebergEnumeratorState> {

  public static final IcebergEnumeratorStateSerializer INSTANCE = new IcebergEnumeratorStateSerializer();

  private static final int VERSION = 1;

  private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
      ThreadLocal.withInitial(() -> new DataOutputSerializer(1024));

  private final IcebergSourceSplitSerializer splitSerializer = IcebergSourceSplitSerializer.INSTANCE;
  private final IcebergSourceSplitStatusSerializer splitStateSerializer = IcebergSourceSplitStatusSerializer.INSTANCE;

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
    final DataOutputSerializer out = SERIALIZER_CACHE.get();

    out.writeBoolean(enumState.lastEnumeratedSnapshotId().isPresent());
    if (enumState.lastEnumeratedSnapshotId().isPresent()) {
      out.writeLong(enumState.lastEnumeratedSnapshotId().get());
    }

    out.writeInt(splitSerializer.getVersion());
    out.writeInt(splitStateSerializer.getVersion());

    out.writeInt(enumState.pendingSplits().size());
    for (Map.Entry<IcebergSourceSplit, IcebergSourceSplitStatus> e : enumState.pendingSplits().entrySet()) {
      final byte[] splitBytes = splitSerializer.serialize(e.getKey());
      out.writeInt(splitBytes.length);
      out.write(splitBytes);
      final byte[] splitStatusBytes = splitStateSerializer.serialize(e.getValue());
      out.writeInt(splitStatusBytes.length);
      out.write(splitStatusBytes);
    }

    final byte[] result = out.getCopyOfBuffer();
    out.clear();
    return result;
  }

  private IcebergEnumeratorState deserializeV1(byte[] serialized) throws IOException {
    final DataInputDeserializer in = new DataInputDeserializer(serialized);

    Optional<Long> lastEnumeratedSnapshotId = Optional.empty();
    if (in.readBoolean()) {
      lastEnumeratedSnapshotId = Optional.of(in.readLong());
    }

    final int splitSerializerVersion = in.readInt();
    final int splitStatusSerializerVersion = in.readInt();

    final int splitCount = in.readInt();
    final Map<IcebergSourceSplit, IcebergSourceSplitStatus> pendingSplits = new HashMap<>(splitCount);
    for (int i = 0; i < splitCount; ++i) {
      final byte[] splitBytes = new byte[in.readInt()];
      in.read(splitBytes);
      IcebergSourceSplit split = splitSerializer.deserialize(splitSerializerVersion, splitBytes);
      final byte[] splitStatusBytes = new byte[in.readInt()];
      in.read(splitStatusBytes);
      IcebergSourceSplitStatus splitStatus = splitStateSerializer
          .deserialize(splitStatusSerializerVersion, splitStatusBytes);
      pendingSplits.put(split, splitStatus);
    }
    return new IcebergEnumeratorState(lastEnumeratedSnapshotId, pendingSplits);
  }
}
