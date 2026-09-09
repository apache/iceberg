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
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.InstantiationUtil;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Serializes {@link SinkWriteResult}.
 *
 * <ul>
 *   <li>Version 1: the {@link WriteResult} only, as {@link WriteResultSerializer} writes it. The
 *       default write path keeps writing this version, so that it stays readable by earlier
 *       releases, for instance from in-flight data of an unaligned checkpoint.
 *   <li>Version 2: the whole {@link SinkWriteResult}, written by the DV-only write path only.
 * </ul>
 *
 * Both versions are read by either instance.
 */
@Internal
public class SinkWriteResultSerializer implements SimpleVersionedSerializer<SinkWriteResult> {
  private static final int WRITE_RESULT_VERSION = 1;
  private static final int SINK_WRITE_RESULT_VERSION = 2;

  private final int writeVersion;

  private SinkWriteResultSerializer(int writeVersion) {
    this.writeVersion = writeVersion;
  }

  /** Serializer of the default write path, which only carries files. */
  public static SinkWriteResultSerializer filesOnly() {
    return new SinkWriteResultSerializer(WRITE_RESULT_VERSION);
  }

  /** Serializer of the DV-only write path. */
  public static SinkWriteResultSerializer dvOnly() {
    return new SinkWriteResultSerializer(SINK_WRITE_RESULT_VERSION);
  }

  @Override
  public int getVersion() {
    return writeVersion;
  }

  @Override
  public byte[] serialize(SinkWriteResult result) throws IOException {
    Object serialized = result;
    if (writeVersion == WRITE_RESULT_VERSION) {
      Preconditions.checkArgument(
          result.deleteKeys().isEmpty()
              && result.liveRows().isEmpty()
              && result.positionDeletes().isEmpty()
              && !result.isBaseline(),
          "Cannot serialize the payload of the DV-only write path as version %s",
          WRITE_RESULT_VERSION);
      serialized = result.writeResult();
    }

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
    view.write(InstantiationUtil.serializeObject(serialized));
    return out.toByteArray();
  }

  @Override
  public SinkWriteResult deserialize(int version, byte[] serialized) throws IOException {
    return switch (version) {
      case WRITE_RESULT_VERSION -> new SinkWriteResult(readObject(serialized, WriteResult.class));
      case SINK_WRITE_RESULT_VERSION -> readObject(serialized, SinkWriteResult.class);
      default -> throw new IOException("Unrecognized version or corrupt state: " + version);
    };
  }

  private static <T> T readObject(byte[] serialized, Class<T> type) throws IOException {
    DataInputDeserializer view = new DataInputDeserializer(serialized);
    byte[] resultBuf = new byte[serialized.length];
    view.read(resultBuf);
    try {
      return type.cast(
          InstantiationUtil.deserializeObject(
              resultBuf, SinkWriteResultSerializer.class.getClassLoader()));
    } catch (ClassNotFoundException e) {
      throw new IOException("Could not deserialize the " + type.getSimpleName() + " object", e);
    }
  }
}
