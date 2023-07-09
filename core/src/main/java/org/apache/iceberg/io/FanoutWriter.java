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
package org.apache.iceberg.io;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.StructLikeMap;

/**
 * A writer capable of writing to multiple specs and partitions that keeps files for each seen
 * spec/partition pair open until this writer is closed.
 *
 * <p>As opposed to {@link ClusteredWriter}, this writer does not require the incoming records to be
 * clustered by partition spec and partition as all files are kept open. As a consequence, this
 * writer may potentially consume substantially more memory compared to {@link ClusteredWriter}. Use
 * this writer only when clustering by spec/partition is not possible (e.g. streaming).
 */
abstract class FanoutWriter<T, R> implements PartitioningWriter<T, R> {

  private final Map<Integer, StructLikeMap<FileWriter<T, R>>> writers = Maps.newHashMap();
  private boolean closed = false;

  protected abstract FileWriter<T, R> newWriter(PartitionSpec spec, StructLike partition);

  protected abstract void addResult(R result);

  protected abstract R aggregatedResult();

  @Override
  public void write(T row, PartitionSpec spec, StructLike partition) {
    FileWriter<T, R> writer = writer(spec, partition);
    writer.write(row);
  }

  private FileWriter<T, R> writer(PartitionSpec spec, StructLike partition) {
    Map<StructLike, FileWriter<T, R>> specWriters =
        writers.computeIfAbsent(spec.specId(), id -> StructLikeMap.create(spec.partitionType()));
    FileWriter<T, R> writer = specWriters.get(partition);

    if (writer == null) {
      // copy the partition key as the key object may be reused
      StructLike copiedPartition = StructCopy.copy(partition);
      writer = newWriter(spec, copiedPartition);
      specWriters.put(copiedPartition, writer);
    }

    return writer;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closeWriters();
      this.closed = true;
    }
  }

  private void closeWriters() throws IOException {
    for (Map<StructLike, FileWriter<T, R>> specWriters : writers.values()) {
      for (FileWriter<T, R> writer : specWriters.values()) {
        writer.close();
        addResult(writer.result());
      }

      specWriters.clear();
    }

    writers.clear();
  }

  @Override
  public final R result() {
    Preconditions.checkState(closed, "Cannot get result from unclosed writer");
    return aggregatedResult();
  }

  /** @deprecated will be removed in 1.5.0 */
  @Deprecated
  protected EncryptedOutputFile newOutputFile(
      OutputFileFactory fileFactory, PartitionSpec spec, StructLike partition) {
    Preconditions.checkArgument(
        spec.isUnpartitioned() || partition != null,
        "Partition must not be null when creating output file for partitioned spec");
    if (spec.isUnpartitioned() || partition == null) {
      return fileFactory.newOutputFile();
    } else {
      return fileFactory.newOutputFile(spec, partition);
    }
  }
}
