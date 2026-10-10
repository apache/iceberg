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

import java.util.stream.IntStream;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.RowDataWrapper;

/**
 * A {@link KeySelector} that extracts the bucketId from a data row's bucket partition as the key.
 * To be used with the {@link BucketPartitioner}.
 */
class BucketPartitionKeySelector implements KeySelector<RowData, Integer> {

  private final Schema schema;
  private final PartitionKey partitionKey;
  private final RowType flinkSchema;
  private final int bucketFieldPosition;

  private transient RowDataWrapper rowDataWrapper;

  BucketPartitionKeySelector(PartitionSpec partitionSpec, Schema schema, RowType flinkSchema) {
    this.schema = schema;
    this.partitionKey = new PartitionKey(partitionSpec, schema);
    this.flinkSchema = flinkSchema;
    this.bucketFieldPosition = getBucketFieldPosition(partitionSpec);
  }

  private int getBucketFieldPosition(PartitionSpec partitionSpec) {
    int bucketFieldId = BucketPartitionerUtil.getBucketFieldId(partitionSpec);
    return IntStream.range(0, partitionSpec.fields().size())
        .filter(i -> partitionSpec.fields().get(i).fieldId() == bucketFieldId)
        .toArray()[0];
  }

  private RowDataWrapper lazyRowDataWrapper() {
    if (rowDataWrapper == null) {
      rowDataWrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
    }

    return rowDataWrapper;
  }

  @Override
  public Integer getKey(RowData rowData) {
    partitionKey.partition(lazyRowDataWrapper().wrap(rowData));
    return partitionKey.get(bucketFieldPosition, Integer.class);
  }
}
