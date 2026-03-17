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
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.InstantiationUtil;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskParser;
import org.apache.iceberg.flink.util.SerializerHelper;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

@Internal
public class IcebergSourceSplit implements SourceSplit, Serializable {
  private static final long serialVersionUID = 1L;
  private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
      ThreadLocal.withInitial(() -> new DataOutputSerializer(1024));

  private final CombinedScanTask task;

  private int fileOffset;
  private long recordOffset;

  /** FileIO from the scan */
  @Nullable private final FileIO fileIO;

  // The splits are frequently serialized into checkpoints.
  // Caching the byte representation makes repeated serialization cheap.
  @Nullable private transient byte[] serializedBytesCache;

  private IcebergSourceSplit(
      CombinedScanTask task, int fileOffset, long recordOffset, @Nullable FileIO fileIO) {
    this.task = task;
    this.fileOffset = fileOffset;
    this.recordOffset = recordOffset;
    this.fileIO = fileIO;
  }

  public static IcebergSourceSplit fromCombinedScanTask(CombinedScanTask combinedScanTask) {
    return fromCombinedScanTask(combinedScanTask, 0, 0L);
  }

  public static IcebergSourceSplit fromCombinedScanTask(
      CombinedScanTask combinedScanTask, int fileOffset, long recordOffset) {
    return new IcebergSourceSplit(combinedScanTask, fileOffset, recordOffset, null);
  }

  public static IcebergSourceSplit fromCombinedScanTask(
      CombinedScanTask combinedScanTask, @Nullable FileIO fileIO) {
    return new IcebergSourceSplit(combinedScanTask, 0, 0L, fileIO);
  }

  /** Returns the FileIO from the scan */
  @Nullable
  public FileIO fileIO() {
    return fileIO;
  }

  public CombinedScanTask task() {
    return task;
  }

  public int fileOffset() {
    return fileOffset;
  }

  public long recordOffset() {
    return recordOffset;
  }

  @Override
  public String splitId() {
    return MoreObjects.toStringHelper(this).add("files", toString(task.files())).toString();
  }

  public void updatePosition(int newFileOffset, long newRecordOffset) {
    // invalidate the cache after position change
    serializedBytesCache = null;
    fileOffset = newFileOffset;
    recordOffset = newRecordOffset;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("files", toString(task.files()))
        .add("fileOffset", fileOffset)
        .add("recordOffset", recordOffset)
        .toString();
  }

  private String toString(Collection<FileScanTask> files) {
    return Iterables.toString(
        files.stream()
            .map(
                fileScanTask ->
                    MoreObjects.toStringHelper(fileScanTask)
                        .add("file", fileScanTask.file().location())
                        .add("start", fileScanTask.start())
                        .add("length", fileScanTask.length())
                        .toString())
            .collect(Collectors.toList()));
  }

  byte[] serializeV1() throws IOException {
    if (serializedBytesCache == null) {
      serializedBytesCache = InstantiationUtil.serializeObject(this);
    }

    return serializedBytesCache;
  }

  static IcebergSourceSplit deserializeV1(byte[] serialized) throws IOException {
    try {
      return InstantiationUtil.deserializeObject(
          serialized, IcebergSourceSplit.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to deserialize the split.", e);
    }
  }

  byte[] serializeV2() throws IOException {
    return serialize(2);
  }

  byte[] serializeV3() throws IOException {
    return serialize(3);
  }

  byte[] serializeV4() throws IOException {
    return serialize(4);
  }

  private byte[] serialize(int version) throws IOException {
    if (serializedBytesCache == null) {
      DataOutputSerializer out = SERIALIZER_CACHE.get();
      Collection<FileScanTask> fileScanTasks = task.tasks();
      Preconditions.checkArgument(
          fileOffset >= 0 && fileOffset < fileScanTasks.size(),
          "Invalid file offset: %s. Should be within the range of [0, %s)",
          fileOffset,
          fileScanTasks.size());

      out.writeInt(fileOffset);
      out.writeLong(recordOffset);
      out.writeInt(fileScanTasks.size());

      for (FileScanTask fileScanTask : fileScanTasks) {
        String taskJson = ScanTaskParser.toJson(fileScanTask);
        writeTaskJson(out, taskJson, version);
      }

      if (version >= 4) {
        if (fileIO != null) {
          out.writeBoolean(true);
          byte[] fileIOBytes = InstantiationUtil.serializeObject(fileIO);
          out.writeInt(fileIOBytes.length);
          out.write(fileIOBytes);
        } else {
          out.writeBoolean(false);
        }
      }

      serializedBytesCache = out.getCopyOfBuffer();
      out.clear();
    }

    return serializedBytesCache;
  }

  private static void writeTaskJson(DataOutputSerializer out, String taskJson, int version)
      throws IOException {
    switch (version) {
      case 2:
        out.writeUTF(taskJson);
        break;
      case 3:
      case 4:
        SerializerHelper.writeLongUTF(out, taskJson);
        break;
      default:
        throw new IllegalArgumentException("Unsupported version: " + version);
    }
  }

  static IcebergSourceSplit deserializeV2(byte[] serialized, boolean caseSensitive)
      throws IOException {
    return deserialize(serialized, caseSensitive, 2);
  }

  static IcebergSourceSplit deserializeV3(byte[] serialized, boolean caseSensitive)
      throws IOException {
    return deserialize(serialized, caseSensitive, 3);
  }

  static IcebergSourceSplit deserializeV4(byte[] serialized, boolean caseSensitive)
      throws IOException {
    return deserialize(serialized, caseSensitive, 4);
  }

  private static IcebergSourceSplit deserialize(
      byte[] serialized, boolean caseSensitive, int version) throws IOException {
    DataInputDeserializer in = new DataInputDeserializer(serialized);
    int fileOffset = in.readInt();
    long recordOffset = in.readLong();
    int taskCount = in.readInt();

    List<FileScanTask> tasks = Lists.newArrayListWithCapacity(taskCount);
    for (int i = 0; i < taskCount; ++i) {
      String taskJson = readTaskJson(in, version);
      FileScanTask task = ScanTaskParser.fromJson(taskJson, caseSensitive);
      tasks.add(task);
    }

    FileIO fileIO = null;
    if (version >= 4) {
      if (in.readBoolean()) {
        int length = in.readInt();
        byte[] fileIOBytes = new byte[length];
        in.read(fileIOBytes);
        try {
          fileIO =
              InstantiationUtil.deserializeObject(
                  fileIOBytes, IcebergSourceSplit.class.getClassLoader());
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("Failed to deserialize FileIO from split", e);
        }
      }
    }

    return new IcebergSourceSplit(
        new BaseCombinedScanTask(tasks), fileOffset, recordOffset, fileIO);
  }

  private static String readTaskJson(DataInputDeserializer in, int version) throws IOException {
    switch (version) {
      case 2:
        return in.readUTF();
      case 3:
      case 4:
        return SerializerHelper.readLongUTF(in);
      default:
        throw new IllegalArgumentException("Unsupported version: " + version);
    }
  }
}
