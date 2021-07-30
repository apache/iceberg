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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.StructLikeMap;

/**
 * A writer capable of writing to multiple specs and partitions that keeps writers for each
 * seen spec/partition pair open until this writer is closed.
 */
public abstract class FanoutWriter<T, R> implements PartitionAwareWriter<T, R> {

  private final Map<Integer, Map<StructLike, Writer<T, R>>> writers = Maps.newHashMap();
  private boolean closed = false;

  protected abstract Writer<T, R> newWriter(PartitionSpec spec, StructLike partition);

  protected abstract void add(R result);

  protected abstract R aggregatedResult();

  @Override
  public CharSequence currentPath(PartitionSpec spec, StructLike partition) {
    return ((RollingWriter<?, ?, ?>) writer(spec, partition)).currentPath();
  }

  @Override
  public long currentPosition(PartitionSpec spec, StructLike partition) {
    return ((RollingWriter<?, ?, ?>) writer(spec, partition)).currentRows();
  }

  @Override
  public void write(T row, PartitionSpec spec, StructLike partition) throws IOException {
    Writer<T, R> writer = writer(spec, partition);
    writer.write(row);
  }

  private Writer<T, R> writer(PartitionSpec spec, StructLike partition) {
    Map<StructLike, Writer<T, R>> specWriters = writers.computeIfAbsent(
        spec.specId(),
        id -> StructLikeMap.create(spec.partitionType()));
    Writer<T, R> writer = specWriters.get(partition);

    if (writer == null) {
      // copy the partition key as the key object is reused
      StructLike copiedPartition = partition != null ? StructCopy.copy(partition) : null;
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
    for (Map<StructLike, Writer<T, R>> specWriters : writers.values()) {
      for (Writer<T, R> writer : specWriters.values()) {
        writer.close();

        R result = writer.result();
        add(result);
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
}
