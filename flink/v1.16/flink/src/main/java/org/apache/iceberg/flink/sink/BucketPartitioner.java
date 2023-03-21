/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.flink.sink;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * This partitioner will redirect elements to writers deterministically so that each writer only targets 1 bucket.
 * If the number of writers > number of buckets each partitioner will keep a state of multiple writers per bucket as
 * evenly as possible, and will round-robin the requests across them.
 */
public class BucketPartitioner implements Partitioner<Integer> {

    private final Integer maxBuckets;

    private final Map<Integer, Integer> currentBucketOffset;

    BucketPartitioner(PartitionSpec partitionSpec) {
        // The current implementation redirects to writers based on the _FIRST_ bucket found
        Optional<PartitionField> bucket =
                partitionSpec.fields().stream().filter(f -> f.transform().dedupName().contains("bucket")).findFirst();

        if (!bucket.isPresent()) {
            throw new IllegalStateException("No buckets found on the provided PartitionSpec");
        }

        // Extracting the max number of buckets defined in the partition spec
        String transformName = bucket.get().transform().dedupName();
        Optional<Integer> maxBucketsOpt = BucketPartitionKeySelector.extractInteger(transformName);

        if (maxBucketsOpt.isPresent()) {
            this.maxBuckets = maxBucketsOpt.get();
        } else {
            throw new IllegalStateException(
                    "Could not extract the max number of buckets from the transform name (" + transformName + ")");
        }

        this.currentBucketOffset = new HashMap<>(maxBuckets);
    }

    @Override
    public int partition(Integer bucketId, int numPartitions) {
        if (numPartitions > maxBuckets) {
            return getSubpartitionIdx(bucketId, numPartitions);
        } else {
            return bucketId % numPartitions;
        }
    }

    private int getSubpartitionIdx(int bucketId, int numPartitions) {
        int currentOffset = -1;

        if (currentBucketOffset.containsKey(bucketId)) {
            currentOffset = currentBucketOffset.get(bucketId);
            int maxBucketNumOffsets = (numPartitions / maxBuckets) + (bucketId < (numPartitions % maxBuckets) ? 1 : 0);

            // Reset the offset when necessary
            if (currentOffset == maxBucketNumOffsets - 1) {
                currentOffset = -1;
            }
        }

        currentBucketOffset.put(bucketId, ++ currentOffset);

        return bucketId + (maxBuckets * currentOffset);
    }
}
