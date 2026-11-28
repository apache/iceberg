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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Serializes {@link SinkWriteResult}.
 *
 * <ul>
 *   <li>Version 1: the {@link WriteResult} only, as {@link WriteResultSerializer} writes it. The
 *       standard write path keeps writing this version, so that it stays readable by earlier
 *       releases, for instance from in-flight data of an unaligned checkpoint.
 *   <li>Version 2: the whole {@link SinkWriteResult}, written by the DV-only write path only. The
 *       rows refer to their data file by its index in a dictionary written ahead of them, so the
 *       path, spec and partition of a file are written once rather than once per row. The data
 *       sequence number is not written, since the DV-only write path never knows it.
 * </ul>
 *
 * Both versions are read by either instance.
 */
@Internal
public class SinkWriteResultSerializer implements SimpleVersionedSerializer<SinkWriteResult> {
  private static final int VERSION_1 = 1;
  private static final int VERSION_2 = 2;

  private final int writeVersion;

  private SinkWriteResultSerializer(int writeVersion) {
    this.writeVersion = writeVersion;
  }

  /** Serializer of the standard write path, which only carries the {@link WriteResult}. */
  public static SinkWriteResultSerializer standard() {
    return new SinkWriteResultSerializer(VERSION_1);
  }

  /** Serializer of the DV-only write path. */
  public static SinkWriteResultSerializer dvOnly() {
    return new SinkWriteResultSerializer(VERSION_2);
  }

  @Override
  public int getVersion() {
    return writeVersion;
  }

  @Override
  public byte[] serialize(SinkWriteResult result) throws IOException {
    return writeVersion == VERSION_2 ? serializeV2(result) : serializeV1(result);
  }

  @Override
  public SinkWriteResult deserialize(int version, byte[] serialized) throws IOException {
    return switch (version) {
      case VERSION_1 -> deserializeV1(serialized);
      case VERSION_2 -> deserializeV2(serialized);
      default -> throw new IOException("Unrecognized version or corrupt state: " + version);
    };
  }

  private static byte[] serializeV1(SinkWriteResult result) throws IOException {
    Preconditions.checkArgument(
        result.kind() == SinkWriteResult.Kind.WRITER
            && result.deleteKeys().isEmpty()
            && result.liveRows().isEmpty(),
        "Cannot serialize the payload of the DV-only write path as version %s",
        VERSION_1);
    return InstantiationUtil.serializeObject(result.writeResult());
  }

  private static SinkWriteResult deserializeV1(byte[] serialized) throws IOException {
    return new SinkWriteResult(readWriteResult(serialized));
  }

  private static byte[] serializeV2(SinkWriteResult result) throws IOException {
    ByteArrayOutputStream binaryOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(binaryOut);

    out.writeUTF(result.kind().name());
    switch (result.kind()) {
      case WRITER -> {
        writeBytes(out, InstantiationUtil.serializeObject(result.writeResult()));
        writeKeys(out, result.deleteKeys());
        writeLiveRows(out, result.liveRows());
      }
    }

    return binaryOut.toByteArray();
  }

  private static SinkWriteResult deserializeV2(byte[] serialized) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));

    SinkWriteResult.Kind kind = SinkWriteResult.Kind.valueOf(in.readUTF());
    return switch (kind) {
      case WRITER -> {
        WriteResult writeResult = readWriteResult(readBytes(in));
        List<SerializedEqualityValues> deleteKeys = readKeys(in);
        List<PkIndexEntry> liveRows = readLiveRows(in);
        yield new SinkWriteResult(writeResult, deleteKeys, liveRows);
      }
    };
  }

  private static void writeKeys(DataOutputStream out, List<SerializedEqualityValues> keys)
      throws IOException {
    out.writeInt(keys.size());
    for (SerializedEqualityValues key : keys) {
      writeBytes(out, key.data());
    }
  }

  private static List<SerializedEqualityValues> readKeys(DataInputStream in) throws IOException {
    int count = in.readInt();
    List<SerializedEqualityValues> keys = Lists.newArrayListWithCapacity(count);
    for (int i = 0; i < count; i++) {
      keys.add(new SerializedEqualityValues(readBytes(in)));
    }

    return keys;
  }

  private static void writeLiveRows(DataOutputStream out, List<PkIndexEntry> rows)
      throws IOException {
    Map<String, Integer> fileIndexes = Maps.newHashMap();
    List<DataFileRef> files = Lists.newArrayList();
    for (PkIndexEntry row : rows) {
      String path = row.position().dataFilePath();
      if (!fileIndexes.containsKey(path)) {
        fileIndexes.put(path, files.size());
        files.add(DataFileRef.of(row.position()));
      }
    }

    out.writeInt(files.size());
    for (DataFileRef file : files) {
      file.write(out);
    }

    out.writeInt(rows.size());
    for (PkIndexEntry row : rows) {
      DVPosition position = row.position();
      Preconditions.checkArgument(
          position.dataSequenceNumber() == PkIndexEntry.UNKNOWN_SEQUENCE,
          "Cannot serialize a row with data sequence number %s: the DV-only write path does not "
              + "carry it",
          position.dataSequenceNumber());
      out.writeInt(fileIndexes.get(position.dataFilePath()));
      out.writeLong(position.position());
      writeBytes(out, row.key().data());
    }
  }

  private static List<PkIndexEntry> readLiveRows(DataInputStream in) throws IOException {
    int fileCount = in.readInt();
    List<DataFileRef> files = Lists.newArrayListWithCapacity(fileCount);
    for (int i = 0; i < fileCount; i++) {
      files.add(DataFileRef.read(in));
    }

    int rowCount = in.readInt();
    List<PkIndexEntry> rows = Lists.newArrayListWithCapacity(rowCount);
    for (int i = 0; i < rowCount; i++) {
      DataFileRef file = files.get(in.readInt());
      long position = in.readLong();
      SerializedEqualityValues key = new SerializedEqualityValues(readBytes(in));
      rows.add(new PkIndexEntry(key, file.position(position)));
    }

    return rows;
  }

  private static WriteResult readWriteResult(byte[] serialized) throws IOException {
    try {
      return InstantiationUtil.deserializeObject(
          serialized, SinkWriteResultSerializer.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new IOException("Could not deserialize the WriteResult object", e);
    }
  }

  private static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private static byte[] readBytes(DataInputStream in) throws IOException {
    byte[] bytes = new byte[in.readInt()];
    in.readFully(bytes);
    return bytes;
  }

  /** What the rows of one data file have in common, written once for all of them. */
  private static final class DataFileRef {
    private final String path;
    private final int specId;
    private final byte[] partition;

    private DataFileRef(String path, int specId, byte[] partition) {
      this.path = path;
      this.specId = specId;
      this.partition = partition;
    }

    static DataFileRef of(DVPosition position) {
      return new DataFileRef(position.dataFilePath(), position.specId(), position.partition());
    }

    static DataFileRef read(DataInputStream in) throws IOException {
      String path = in.readUTF();
      int specId = in.readInt();
      byte[] partition = readBytes(in);
      return new DataFileRef(path, specId, partition);
    }

    void write(DataOutputStream out) throws IOException {
      out.writeUTF(path);
      out.writeInt(specId);
      writeBytes(out, partition);
    }

    DVPosition position(long rowPosition) {
      return new DVPosition(path, rowPosition, specId, partition, PkIndexEntry.UNKNOWN_SEQUENCE);
    }
  }
}
