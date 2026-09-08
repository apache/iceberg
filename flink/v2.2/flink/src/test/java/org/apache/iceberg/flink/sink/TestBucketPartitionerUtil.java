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
import java.util.UUID;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.BucketUtil;

final class TestBucketPartitionerUtil {

  enum TableSchemaType {
    ONE_BUCKET {
      @Override
      public int bucketPartitionColumnPosition() {
        return 0;
      }

      @Override
      public PartitionSpec getPartitionSpec(int numBuckets) {
        return PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).bucket("data", numBuckets).build();
      }
    },
    IDENTITY_AND_BUCKET {
      @Override
      public int bucketPartitionColumnPosition() {
        return 1;
      }

      @Override
      public PartitionSpec getPartitionSpec(int numBuckets) {
        return PartitionSpec.builderFor(SimpleDataUtil.SCHEMA)
            .identity("id")
            .bucket("data", numBuckets)
            .build();
      }
    },
    TWO_BUCKETS {
      @Override
      public int bucketPartitionColumnPosition() {
        return 1;
      }

      @Override
      public PartitionSpec getPartitionSpec(int numBuckets) {
        return PartitionSpec.builderFor(SimpleDataUtil.SCHEMA)
            .bucket("id", numBuckets)
            .bucket("data", numBuckets)
            .build();
      }
    };

    public abstract int bucketPartitionColumnPosition();

    public abstract PartitionSpec getPartitionSpec(int numBuckets);
  }

  private TestBucketPartitionerUtil() {}

  /**
   * Utility method to generate rows whose values will "hash" to a range of bucketIds (from 0 to
   * numBuckets - 1)
   *
   * @param numRowsPerBucket how many different rows should be generated per bucket
   * @param numBuckets max number of buckets to consider
   * @return the list of rows whose data "hashes" to the desired bucketId
   */
  static List<RowData> generateRowsForBucketIdRange(int numRowsPerBucket, int numBuckets) {
    List<RowData> rows = Lists.newArrayListWithCapacity(numBuckets * numRowsPerBucket);
    // For some of our tests, this order of the generated rows matters
    for (int i = 0; i < numRowsPerBucket; i++) {
      for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
        String value = generateValueForBucketId(bucketId, numBuckets);
        rows.add(GenericRowData.of(1, StringData.fromString(value)));
      }
    }
    return rows;
  }

  /**
   * Utility method to generate a UUID string that will "hash" to a desired bucketId
   *
   * @param bucketId the desired bucketId
   * @return the string data that "hashes" to the desired bucketId
   */
  private static String generateValueForBucketId(int bucketId, int numBuckets) {
    while (true) {
      String uuid = UUID.randomUUID().toString();
      if (computeBucketId(numBuckets, uuid) == bucketId) {
        return uuid;
      }
    }
  }

  /**
   * Utility that performs the same hashing/bucketing mechanism used by Bucket.java
   *
   * @param numBuckets max number of buckets to consider
   * @param value the string to compute the bucketId from
   * @return the computed bucketId
   */
  static int computeBucketId(int numBuckets, String value) {
    return (BucketUtil.hash(value) & Integer.MAX_VALUE) % numBuckets;
  }
}
