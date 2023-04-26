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

import org.apache.iceberg.PartitionSpec;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestBucketPartitioner {

  static final int NUM_BUCKETS = 60;

  @ParameterizedTest
  @EnumSource(
      value = TestBucketPartitionerUtils.TableSchemaType.class,
      names = {"ONE_BUCKET", "IDENTITY_AND_BUCKET"})
  public void testPartitioningParallelismGreaterThanBuckets(
      TestBucketPartitionerUtils.TableSchemaType tableSchemaType) {
    final int numPartitions = 500;

    PartitionSpec partitionSpec =
        TestBucketPartitionerUtils.getPartitionSpec(tableSchemaType, NUM_BUCKETS);

    BucketPartitioner bucketPartitioner = new BucketPartitioner(partitionSpec);

    for (int expectedIdx = 0, bucketId = 0; expectedIdx < numPartitions; expectedIdx++) {
      int actualIdx = bucketPartitioner.partition(bucketId, numPartitions);
      Assertions.assertThat(actualIdx).isEqualTo(expectedIdx);
      if (++bucketId == NUM_BUCKETS) {
        bucketId = 0;
      }
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = TestBucketPartitionerUtils.TableSchemaType.class,
      names = {"ONE_BUCKET", "IDENTITY_AND_BUCKET"})
  public void testPartitioningParallelismEqualLessThanBuckets(
      TestBucketPartitionerUtils.TableSchemaType tableSchemaType) {
    final int numPartitions = 30;

    PartitionSpec partitionSpec =
        TestBucketPartitionerUtils.getPartitionSpec(tableSchemaType, NUM_BUCKETS);

    BucketPartitioner bucketPartitioner = new BucketPartitioner(partitionSpec);

    for (int bucketId = 0; bucketId < NUM_BUCKETS; bucketId++) {
      int actualIdx = bucketPartitioner.partition(bucketId, numPartitions);
      Assertions.assertThat(actualIdx).isEqualTo(bucketId % numPartitions);
    }
  }

  @ParameterizedTest
  @EnumSource(value = TestBucketPartitionerUtils.TableSchemaType.class, names = "TWO_BUCKETS")
  public void testPartitionerMultipleBucketsFail(
      TestBucketPartitionerUtils.TableSchemaType tableSchemaType) {
    PartitionSpec partitionSpec =
        TestBucketPartitionerUtils.getPartitionSpec(tableSchemaType, NUM_BUCKETS);

    Assertions.assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> new BucketPartitioner(partitionSpec))
        .withMessageContaining(BucketPartitionerUtils.BAD_NUMBER_OF_BUCKETS_ERROR_MESSAGE);
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, NUM_BUCKETS})
  public void testPartitionerBucketIdOutOfRangeFail(int bucketId) {
    PartitionSpec partitionSpec =
        TestBucketPartitionerUtils.getPartitionSpec(
            TestBucketPartitionerUtils.TableSchemaType.ONE_BUCKET, NUM_BUCKETS);

    BucketPartitioner bucketPartitioner = new BucketPartitioner(partitionSpec);

    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> bucketPartitioner.partition(bucketId, 1))
        .withMessageContaining("out of range");
  }
}
