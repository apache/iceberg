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
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.StructLikeSet;

/**
 * A writer capable of writing to multiple specs and partitions that requires the incoming records
 * to be clustered by partition spec and by partition within each spec.
 *
 * <p>As opposed to {@link FanoutWriter}, this writer keeps at most one file open to reduce the
 * memory consumption. Prefer using this writer whenever the incoming records can be clustered by
 * spec/partition.
 */
abstract class ClusteredWriter<T, R> implements PartitioningWriter<T, R> {

  private static final String NOT_CLUSTERED_ROWS_ERROR_MSG_TEMPLATE =
      "Incoming records violate the writer assumption that records are clustered by spec and "
          + "by partition within each spec. Either cluster the incoming records or switch to fanout writers.\n"
          + "Encountered records that belong to already closed files:\n";

  private final Set<Integer> completedSpecIds = Sets.newHashSet();

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
  public void write(T row, PartitionSpec spec, StructLike partition) {
    if (!spec.equals(currentSpec)) {
      if (currentSpec != null) {
        closeCurrentWriter();
        completedSpecIds.add(currentSpec.specId());
        completedPartitions.clear();
      }

      if (completedSpecIds.contains(spec.specId())) {
        String errorCtx = String.format("spec %s", spec);
        throw new IllegalStateException(NOT_CLUSTERED_ROWS_ERROR_MSG_TEMPLATE + errorCtx);
      }

      StructType partitionType = spec.partitionType();

      this.currentSpec = spec;
      this.partitionComparator = Comparators.forType(partitionType);
      this.completedPartitions = StructLikeSet.create(partitionType);
      // copy the partition key as the key object may be reused
      this.currentPartition = StructCopy.copy(partition);
      this.currentWriter = newWriter(currentSpec, currentPartition);

    } else if (partition != currentPartition
        && partitionComparator.compare(partition, currentPartition) != 0) {
      closeCurrentWriter();
      completedPartitions.add(currentPartition);

      if (completedPartitions.contains(partition)) {
        String errorCtx =
            String.format("partition '%s' in spec %s", spec.partitionToPath(partition), spec);
        throw new IllegalStateException(NOT_CLUSTERED_ROWS_ERROR_MSG_TEMPLATE + errorCtx);
      }

      // copy the partition key as the key object may be reused
      this.currentPartition = StructCopy.copy(partition);
      this.currentWriter = newWriter(currentSpec, currentPartition);
    }

    currentWriter.write(row);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closeCurrentWriter();
      this.closed = true;
    }
  }

  private void closeCurrentWriter() {
    if (currentWriter != null) {
      try {
        currentWriter.close();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close current writer", e);
      }

      addResult(currentWriter.result());

      this.currentWriter = null;
    }
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
