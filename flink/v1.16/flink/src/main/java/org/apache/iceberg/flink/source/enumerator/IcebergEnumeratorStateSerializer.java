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
import org.apache.flink.annotation.VisibleForTesting;
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

  private static final int VERSION = 2;

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
    return serializeV2(enumState);
  }

  @Override
  public IcebergEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
    switch (version) {
      case 1:
        return deserializeV1(serialized);
      case 2:
        return deserializeV2(serialized);
      default:
        throw new IOException("Unknown version: " + version);
    }
  }

  @VisibleForTesting
  byte[] serializeV1(IcebergEnumeratorState enumState) throws IOException {
    DataOutputSerializer out = SERIALIZER_CACHE.get();
    serializeEnumeratorPosition(out, enumState.lastEnumeratedPosition(), positionSerializer);
    serializePendingSplits(out, enumState.pendingSplits(), splitSerializer);
    byte[] result = out.getCopyOfBuffer();
    out.clear();
    return result;
  }

  @VisibleForTesting
  IcebergEnumeratorState deserializeV1(byte[] serialized) throws IOException {
    DataInputDeserializer in = new DataInputDeserializer(serialized);
    IcebergEnumeratorPosition enumeratorPosition =
        deserializeEnumeratorPosition(in, positionSerializer);
    Collection<IcebergSourceSplitState> pendingSplits =
        deserializePendingSplits(in, splitSerializer);
    return new IcebergEnumeratorState(enumeratorPosition, pendingSplits);
  }

  @VisibleForTesting
  byte[] serializeV2(IcebergEnumeratorState enumState) throws IOException {
    DataOutputSerializer out = SERIALIZER_CACHE.get();
    serializeEnumeratorPosition(out, enumState.lastEnumeratedPosition(), positionSerializer);
    serializePendingSplits(out, enumState.pendingSplits(), splitSerializer);
    serializeEnumerationSplitCountHistory(out, enumState.enumerationSplitCountHistory());
    byte[] result = out.getCopyOfBuffer();
    out.clear();
    return result;
  }

  @VisibleForTesting
  IcebergEnumeratorState deserializeV2(byte[] serialized) throws IOException {
    DataInputDeserializer in = new DataInputDeserializer(serialized);
    IcebergEnumeratorPosition enumeratorPosition =
        deserializeEnumeratorPosition(in, positionSerializer);
    Collection<IcebergSourceSplitState> pendingSplits =
        deserializePendingSplits(in, splitSerializer);
    int[] enumerationSplitCountHistory = deserializeEnumerationSplitCountHistory(in);
    return new IcebergEnumeratorState(
        enumeratorPosition, pendingSplits, enumerationSplitCountHistory);
  }

  private static void serializeEnumeratorPosition(
      DataOutputSerializer out,
      IcebergEnumeratorPosition enumeratorPosition,
      IcebergEnumeratorPositionSerializer positionSerializer)
      throws IOException {
    out.writeBoolean(enumeratorPosition != null);
    if (enumeratorPosition != null) {
      out.writeInt(positionSerializer.getVersion());
      byte[] positionBytes = positionSerializer.serialize(enumeratorPosition);
      out.writeInt(positionBytes.length);
      out.write(positionBytes);
    }
  }

  private static IcebergEnumeratorPosition deserializeEnumeratorPosition(
      DataInputDeserializer in, IcebergEnumeratorPositionSerializer positionSerializer)
      throws IOException {
    IcebergEnumeratorPosition enumeratorPosition = null;
    if (in.readBoolean()) {
      int version = in.readInt();
      byte[] positionBytes = new byte[in.readInt()];
      in.read(positionBytes);
      enumeratorPosition = positionSerializer.deserialize(version, positionBytes);
    }
    return enumeratorPosition;
  }

  private static void serializePendingSplits(
      DataOutputSerializer out,
      Collection<IcebergSourceSplitState> pendingSplits,
      IcebergSourceSplitSerializer splitSerializer)
      throws IOException {
    out.writeInt(splitSerializer.getVersion());
    out.writeInt(pendingSplits.size());
    for (IcebergSourceSplitState splitState : pendingSplits) {
      byte[] splitBytes = splitSerializer.serialize(splitState.split());
      out.writeInt(splitBytes.length);
      out.write(splitBytes);
      out.writeUTF(splitState.status().name());
    }
  }

  private static Collection<IcebergSourceSplitState> deserializePendingSplits(
      DataInputDeserializer in, IcebergSourceSplitSerializer splitSerializer) throws IOException {
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
    return pendingSplits;
  }

  private static void serializeEnumerationSplitCountHistory(
      DataOutputSerializer out, int[] enumerationSplitCountHistory) throws IOException {
    out.writeInt(enumerationSplitCountHistory.length);
    if (enumerationSplitCountHistory.length > 0) {
      for (int i = 0; i < enumerationSplitCountHistory.length; ++i) {
        out.writeInt(enumerationSplitCountHistory[i]);
      }
    }
  }

  private static int[] deserializeEnumerationSplitCountHistory(DataInputDeserializer in)
      throws IOException {
    int historySize = in.readInt();
    int[] history = new int[historySize];
    if (historySize > 0) {
      for (int i = 0; i < historySize; ++i) {
        history[i] = in.readInt();
      }
    }

    return history;
  }
}
