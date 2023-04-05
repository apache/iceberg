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

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * This partitioner will redirect elements to writers deterministically so that each writer only
 * targets 1 bucket. If the number of writers > number of buckets each partitioner will keep a state
 * of multiple writers per bucket as evenly as possible, and will round-robin the requests across
 * them.
 */
class BucketPartitioner implements Partitioner<Integer> {

  private final int maxBuckets;

  private final int[] currentWriterOffset;

  BucketPartitioner(PartitionSpec partitionSpec) {
    // The current implementation only supports _ONE_ bucket
    List<PartitionField> bucketTransforms =
        partitionSpec.fields().stream()
            .filter(f -> f.transform().dedupName().contains("bucket")).collect(Collectors.toList());

    Preconditions.checkArgument(
        bucketTransforms.size() == 1,
        "Expected 1 Bucket transform in the provided PartitionSpec, found: " + bucketTransforms.size());

    // Extracting the max number of buckets defined in the partition spec
    String transformName = bucketTransforms.get(0).transform().dedupName();
    Optional<Integer> maxBucketsOpt = BucketPartitionKeySelector.extractInteger(transformName);

    Preconditions.checkArgument(
        maxBucketsOpt.isPresent(),
        "Could not extract the max number of buckets from the transform name ("
            + transformName
            + ")");

    this.maxBuckets = maxBucketsOpt.get();
    this.currentWriterOffset = new int[this.maxBuckets];
  }

  @Override
  public int partition(Integer bucketId, int numPartitions) {
    if (numPartitions > maxBuckets) {
      return getPartitionIndex(bucketId, numPartitions);
    } else {
      return bucketId % numPartitions;
    }
  }

  private int getPartitionIndex(int bucketId, int numPartitions) {
    int currentOffset = currentWriterOffset[bucketId];
    // When numPartitions is not evenly divisible by maxBuckets
    int extraPad = bucketId < (numPartitions % maxBuckets) ? 1 : 0;
    int maxWriterNumOffsets = (numPartitions / maxBuckets) + extraPad;

    // Reset the offset when necessary
    int nextOffset = currentOffset == maxWriterNumOffsets - 1 ? 0 : currentOffset + 1;
    currentWriterOffset[bucketId] = nextOffset;

    return bucketId + (maxBuckets * currentOffset);
  }
}
