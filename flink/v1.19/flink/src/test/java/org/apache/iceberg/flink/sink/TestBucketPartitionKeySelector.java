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
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.sink.TestBucketPartitionerUtil.TableSchemaType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class TestBucketPartitionKeySelector {

  @ParameterizedTest
  @EnumSource(
      value = TableSchemaType.class,
      names = {"ONE_BUCKET", "IDENTITY_AND_BUCKET"})
  public void testCorrectKeySelection(TableSchemaType tableSchemaType) {
    int numBuckets = 60;

    PartitionSpec partitionSpec = tableSchemaType.getPartitionSpec(numBuckets);
    BucketPartitionKeySelector keySelector =
        new BucketPartitionKeySelector(
            partitionSpec, SimpleDataUtil.SCHEMA, SimpleDataUtil.ROW_TYPE);

    TestBucketPartitionerUtil.generateRowsForBucketIdRange(2, numBuckets)
        .forEach(
            rowData -> {
              int expectedBucketId =
                  TestBucketPartitionerUtil.computeBucketId(
                      numBuckets, rowData.getString(1).toString());
              Integer key = keySelector.getKey(rowData);
              Assertions.assertThat(key).isEqualTo(expectedBucketId);
            });
  }

  @Test
  public void testKeySelectorMultipleBucketsFail() {
    PartitionSpec partitionSpec = TableSchemaType.TWO_BUCKETS.getPartitionSpec(1);

    Assertions.assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(
            () ->
                new BucketPartitionKeySelector(
                    partitionSpec, SimpleDataUtil.SCHEMA, SimpleDataUtil.ROW_TYPE))
        .withMessage(BucketPartitionerUtil.BAD_NUMBER_OF_BUCKETS_ERROR_MESSAGE, 2);
  }
}
