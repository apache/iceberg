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

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.iceberg.PartitionSpec;

/**
 * This partitioner will redirect records to writers deterministically based on the Bucket partition
 * spec. It'll attempt to optimize the file size written depending on whether numPartitions is
 * greater, less or equal than the maxNumBuckets. Note: The current implementation only supports ONE
 * bucket in the partition spec.
 */
class BucketPartitioner implements Partitioner<Integer> {

  private final int maxNumBuckets;

  // This always has the OFFSET of the next writer to use for any bucket
  private final int[] currentBucketWriterOffset;

  BucketPartitioner(PartitionSpec partitionSpec) {
    Tuple2<Integer, Integer> bucketFieldInfo =
        BucketPartitionerUtils.getBucketFieldInfo(partitionSpec);

    this.maxNumBuckets = bucketFieldInfo.f1;
    this.currentBucketWriterOffset = new int[this.maxNumBuckets];
  }

  /**
   * If the number of writers <= the number of buckets an evenly distributed number of buckets will
   * be assigned to each writer. Conversely, if the number of writers > the number of buckets the
   * logic is handled by the {@link #getPartitionWritersGreaterThanBuckets
   * getPartitionWritersGreaterThanBuckets} method.
   *
   * @param bucketId the bucketId for each request
   * @param numPartitions the total number of partitions
   * @return the partition index (writer) to use for each request
   */
  @Override
  public int partition(Integer bucketId, int numPartitions) {
    if (numPartitions <= maxNumBuckets) {
      return bucketId % numPartitions;
    } else {
      return getPartitionWritersGreaterThanBuckets(bucketId, numPartitions);
    }
  }

  /*-
   * If the number of writers > the number of buckets each partitioner will keep a state of multiple
   * writers per bucket as evenly as possible, and will round-robin the requests across them, in
   * this case each writer will target no more than one bucket at all times. Example:
   * Configuration: numPartitions (writers) = 3, maxBuckets = 2
   * Expected behavior:
   * - Records for Bucket 0 will be "round robin" between Writers 0 and 2
   * - Records for Bucket 1 will always use Writer 1
   * Notes:
   * - maxNumWritersPerBucket determines when to reset the currentBucketWriterOffset to 0 for this bucketId
   * - When numPartitions is not evenly divisible by maxBuckets, some buckets will have one more writer (extraWriter).
   * In this example Bucket 0 has an "extra writer" to consider before resetting its offset to 0.
   *
   * @param bucketId the bucketId for each request
   * @param numPartitions the total number of partitions
   * @return the partition index (writer) to use for each request
   */
  private int getPartitionWritersGreaterThanBuckets(int bucketId, int numPartitions) {
    int currentOffset = currentBucketWriterOffset[bucketId];
    // Determine if this bucket requires an "extra writer"
    int extraWriter = bucketId < (numPartitions % maxNumBuckets) ? 1 : 0;
    // The max number of writers this bucket can have
    int maxNumWritersPerBucket = (numPartitions / maxNumBuckets) + extraWriter;

    // Increment the writer offset or reset if it's reached the max for this bucket
    int nextOffset = currentOffset == maxNumWritersPerBucket - 1 ? 0 : currentOffset + 1;
    currentBucketWriterOffset[bucketId] = nextOffset;

    return bucketId + (maxNumBuckets * currentOffset);
  }
}
