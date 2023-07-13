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

import static org.apache.iceberg.flink.sink.BucketPartitioner.BUCKET_GREATER_THAN_UPPER_BOUND_MESSAGE;
import static org.apache.iceberg.flink.sink.BucketPartitioner.BUCKET_LESS_THAN_LOWER_BOUND_MESSAGE;
import static org.apache.iceberg.flink.sink.BucketPartitioner.BUCKET_NULL_MESSAGE;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.flink.sink.TestBucketPartitionerUtil.TableSchemaType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestBucketPartitioner {

  static final int DEFAULT_NUM_BUCKETS = 60;

  @ParameterizedTest
  @CsvSource({"ONE_BUCKET,50", "IDENTITY_AND_BUCKET,50", "ONE_BUCKET,60", "IDENTITY_AND_BUCKET,60"})
  public void testPartitioningParallelismGreaterThanBuckets(
      String schemaTypeStr, String numBucketsStr) {
    int numPartitions = 500;
    TableSchemaType tableSchemaType = TableSchemaType.valueOf(schemaTypeStr);
    int numBuckets = Integer.parseInt(numBucketsStr);
    PartitionSpec partitionSpec = tableSchemaType.getPartitionSpec(numBuckets);
    BucketPartitioner bucketPartitioner = new BucketPartitioner(partitionSpec);

    int bucketId = 0;
    for (int expectedIndex = 0; expectedIndex < numPartitions; expectedIndex++) {
      int actualPartitionIndex = bucketPartitioner.partition(bucketId, numPartitions);
      Assertions.assertThat(actualPartitionIndex).isEqualTo(expectedIndex);
      bucketId++;
      if (bucketId == numBuckets) {
        bucketId = 0;
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"ONE_BUCKET,50", "IDENTITY_AND_BUCKET,50", "ONE_BUCKET,60", "IDENTITY_AND_BUCKET,60"})
  public void testPartitioningParallelismEqualLessThanBuckets(
      String schemaTypeStr, String numBucketsStr) {
    int numPartitions = 30;
    TableSchemaType tableSchemaType = TableSchemaType.valueOf(schemaTypeStr);
    int numBuckets = Integer.parseInt(numBucketsStr);
    PartitionSpec partitionSpec = tableSchemaType.getPartitionSpec(numBuckets);
    BucketPartitioner bucketPartitioner = new BucketPartitioner(partitionSpec);

    for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
      int actualPartitionIndex = bucketPartitioner.partition(bucketId, numPartitions);
      Assertions.assertThat(actualPartitionIndex).isEqualTo(bucketId % numPartitions);
    }
  }

  @Test
  public void testPartitionerBucketIdNullFail() {
    PartitionSpec partitionSpec = TableSchemaType.ONE_BUCKET.getPartitionSpec(DEFAULT_NUM_BUCKETS);
    BucketPartitioner bucketPartitioner = new BucketPartitioner(partitionSpec);

    Assertions.assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> bucketPartitioner.partition(null, DEFAULT_NUM_BUCKETS))
        .withMessage(BUCKET_NULL_MESSAGE);
  }

  @Test
  public void testPartitionerMultipleBucketsFail() {
    PartitionSpec partitionSpec = TableSchemaType.TWO_BUCKETS.getPartitionSpec(DEFAULT_NUM_BUCKETS);

    Assertions.assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> new BucketPartitioner(partitionSpec))
        .withMessage(BucketPartitionerUtil.BAD_NUMBER_OF_BUCKETS_ERROR_MESSAGE, 2);
  }

  @Test
  public void testPartitionerBucketIdOutOfRangeFail() {
    PartitionSpec partitionSpec = TableSchemaType.ONE_BUCKET.getPartitionSpec(DEFAULT_NUM_BUCKETS);
    BucketPartitioner bucketPartitioner = new BucketPartitioner(partitionSpec);

    int negativeBucketId = -1;
    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> bucketPartitioner.partition(negativeBucketId, 1))
        .withMessage(BUCKET_LESS_THAN_LOWER_BOUND_MESSAGE, negativeBucketId);

    int tooBigBucketId = DEFAULT_NUM_BUCKETS;
    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> bucketPartitioner.partition(tooBigBucketId, 1))
        .withMessage(BUCKET_GREATER_THAN_UPPER_BOUND_MESSAGE, tooBigBucketId, DEFAULT_NUM_BUCKETS);
  }
}
