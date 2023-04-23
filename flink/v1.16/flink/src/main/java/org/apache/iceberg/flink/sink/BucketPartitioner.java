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

/** This partitioner will redirect elements to writers deterministically. */
class BucketPartitioner implements Partitioner<Integer> {

  private final int maxBuckets;

  private final int[] currentWriterForBucket;

  BucketPartitioner(PartitionSpec partitionSpec) {
    Tuple2<Integer, Integer> bucketFieldInfo =
        BucketPartitionerUtils.getBucketFieldInfo(partitionSpec);

    this.maxBuckets = bucketFieldInfo.f1;
    this.currentWriterForBucket = new int[this.maxBuckets];
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
    if (numPartitions <= maxBuckets) {
      return bucketId % numPartitions;
    } else {
      return getPartitionWritersGreaterThanBuckets(bucketId, numPartitions);
    }
  }

  /*-
   * If the number of writers > the number of buckets each partitioner will keep a state of multiple
   * writers per bucket as evenly as possible, and will round-robin the requests across them, in
   * this case each writer will target no more than one bucket at all times.
   * Example: numPartitions (writers) = 3, maxBuckets = 2
   * - Writers 0 and 2 -> Bucket 0
   * - Writer 1 -> Bucket 1
   * - currentWriterForBucket always has the next writer index to use for a bucket
   * - maxNumWritersPerBucket determines when to reset the currentWriterForBucket to 0
   * - When numPartitions is not evenly divisible by maxBuckets, some buckets will have one more writer,
   * this is determined by extraWriter. In this example Bucket 0 has an "extra writer" to consider when
   * resetting to 0.
   *
   * @param bucketId the bucketId for each request
   * @param numPartitions the total number of partitions
   * @return the partition index (writer) to use for each request
   */
  private int getPartitionWritersGreaterThanBuckets(int bucketId, int numPartitions) {
    int currentOffset = currentWriterForBucket[bucketId];
    // When numPartitions is not evenly divisible by maxBuckets
    int extraWriter = bucketId < (numPartitions % maxBuckets) ? 1 : 0;
    int maxNumWritersPerBucket = (numPartitions / maxBuckets) + extraWriter;

    // Reset the offset when necessary
    int nextOffset = currentOffset == maxNumWritersPerBucket - 1 ? 0 : currentOffset + 1;
    currentWriterForBucket[bucketId] = nextOffset;

    return bucketId + (maxBuckets * currentOffset);
  }
}
