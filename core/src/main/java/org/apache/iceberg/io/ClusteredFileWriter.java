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
import java.util.Comparator;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;

/**
 * A writer capable of writing to multiple specs and partitions that requires the incoming records
 * to be clustered by partition spec and partition.
 * <p>
 * As opposed to {@link FanoutFileWriter}, this writer keeps at most one file open to reduce
 * the memory consumption.
 */
abstract class ClusteredFileWriter<T, R> implements PartitionAwareFileWriter<T, R> {

  private final Set<Integer> completedSpecs = Sets.newHashSet();

  private PartitionSpec currentSpec = null;
  private Comparator<StructLike> partitionComparator = null;
  private Set<StructLike> completedPartitions = null;
  private StructLike currentPartition = null;
  private FileWriter<T, R> currentWriter = null;

  private boolean closed = false;

  protected abstract FileWriter<T, R> newWriter(PartitionSpec spec, StructLike partition);

  protected abstract void addResult(R result);

  protected abstract R aggregatedResult();

  @Override
  public void write(T row, PartitionSpec spec, StructLike partition) throws IOException {
    if (!spec.equals(currentSpec)) {
      if (currentSpec != null) {
        closeCurrent();
        completedSpecs.add(currentSpec.specId());
        completedPartitions.clear();
      }

      if (completedSpecs.contains(spec.specId())) {
        throw new IllegalStateException("Already closed files for spec: " + spec.specId());
      }

      Types.StructType partitionType = spec.partitionType();

      currentSpec = spec;
      partitionComparator = Comparators.forType(partitionType);
      completedPartitions = StructLikeSet.create(partitionType);
      // copy the partition key as the key object is reused
      currentPartition = partition != null ? StructCopy.copy(partition) : null;
      currentWriter = newWriter(currentSpec, currentPartition);

    } else if (partition != currentPartition && partitionComparator.compare(partition, currentPartition) != 0) {
      closeCurrent();
      completedPartitions.add(currentPartition);

      if (completedPartitions.contains(partition)) {
        String path = spec.partitionToPath(partition);
        throw new IllegalStateException("Already closed files for partition: " + path);
      }

      // copy the partition key as the key object is reused
      currentPartition = partition != null ? StructCopy.copy(partition) : null;
      currentWriter = newWriter(currentSpec, currentPartition);
    }

    currentWriter.write(row);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closeCurrent();
      this.closed = true;
    }
  }

  private void closeCurrent() throws IOException {
    if (currentWriter != null) {
      currentWriter.close();

      R result = currentWriter.result();
      addResult(result);

      this.currentWriter = null;
    }
  }

  @Override
  public final R result() {
    Preconditions.checkState(closed, "Cannot get result from unclosed writer");
    return aggregatedResult();
  }
}
