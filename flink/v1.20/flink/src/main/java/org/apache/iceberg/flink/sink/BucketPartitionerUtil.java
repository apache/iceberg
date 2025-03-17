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
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.transforms.PartitionSpecVisitor;

final class BucketPartitionerUtil {
  static final String BAD_NUMBER_OF_BUCKETS_ERROR_MESSAGE =
      "Invalid number of buckets: %s (must be 1)";

  private BucketPartitionerUtil() {}

  /**
   * Determines whether the PartitionSpec has one and only one Bucket definition
   *
   * @param partitionSpec the partition spec in question
   * @return whether the PartitionSpec has only one Bucket
   */
  static boolean hasOneBucketField(PartitionSpec partitionSpec) {
    List<Tuple2<Integer, Integer>> bucketFields = getBucketFields(partitionSpec);
    return bucketFields != null && bucketFields.size() == 1;
  }

  /**
   * Extracts the Bucket definition from a PartitionSpec.
   *
   * @param partitionSpec the partition spec in question
   * @return the Bucket definition in the form of a tuple (fieldId, maxNumBuckets)
   */
  private static Tuple2<Integer, Integer> getBucketFieldInfo(PartitionSpec partitionSpec) {
    List<Tuple2<Integer, Integer>> bucketFields = getBucketFields(partitionSpec);
    Preconditions.checkArgument(
        bucketFields.size() == 1,
        BucketPartitionerUtil.BAD_NUMBER_OF_BUCKETS_ERROR_MESSAGE,
        bucketFields.size());
    return bucketFields.get(0);
  }

  static int getBucketFieldId(PartitionSpec partitionSpec) {
    return getBucketFieldInfo(partitionSpec).f0;
  }

  static int getMaxNumBuckets(PartitionSpec partitionSpec) {
    return getBucketFieldInfo(partitionSpec).f1;
  }

  private static List<Tuple2<Integer, Integer>> getBucketFields(PartitionSpec spec) {
    return PartitionSpecVisitor.visit(spec, new BucketPartitionSpecVisitor()).stream()
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private static class BucketPartitionSpecVisitor
      implements PartitionSpecVisitor<Tuple2<Integer, Integer>> {
    @Override
    public Tuple2<Integer, Integer> identity(int fieldId, String sourceName, int sourceId) {
      return null;
    }

    @Override
    public Tuple2<Integer, Integer> bucket(
        int fieldId, String sourceName, int sourceId, int numBuckets) {
      return new Tuple2<>(fieldId, numBuckets);
    }

    @Override
    public Tuple2<Integer, Integer> truncate(
        int fieldId, String sourceName, int sourceId, int width) {
      return null;
    }

    @Override
    public Tuple2<Integer, Integer> year(int fieldId, String sourceName, int sourceId) {
      return null;
    }

    @Override
    public Tuple2<Integer, Integer> month(int fieldId, String sourceName, int sourceId) {
      return null;
    }

    @Override
    public Tuple2<Integer, Integer> day(int fieldId, String sourceName, int sourceId) {
      return null;
    }

    @Override
    public Tuple2<Integer, Integer> hour(int fieldId, String sourceName, int sourceId) {
      return null;
    }

    @Override
    public Tuple2<Integer, Integer> alwaysNull(int fieldId, String sourceName, int sourceId) {
      return null;
    }

    @Override
    public Tuple2<Integer, Integer> unknown(
        int fieldId, String sourceName, int sourceId, String transform) {
      return null;
    }
  }
}
