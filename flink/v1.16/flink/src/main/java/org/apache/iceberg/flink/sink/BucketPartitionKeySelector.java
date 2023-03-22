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

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.RowDataWrapper;

/**
 * Very similar to the {@link PartitionKeySelector}, but optimized to extract and return an Integer
 * bucketId as the key.
 */
public class BucketPartitionKeySelector implements KeySelector<RowData, Integer> {

  private final Schema schema;
  private final PartitionKey partitionKey;
  private final RowType flinkSchema;
  private static final Pattern intPattern = Pattern.compile("\\d+");

  private transient RowDataWrapper rowDataWrapper;

  BucketPartitionKeySelector(PartitionSpec spec, Schema schema, RowType flinkSchema) {
    this.schema = schema;
    this.partitionKey = new PartitionKey(spec, schema);
    this.flinkSchema = flinkSchema;
  }

  /**
   * Construct the {@link RowDataWrapper} lazily here because few members in it are not
   * serializable. In this way, we don't have to serialize them with forcing.
   */
  private RowDataWrapper lazyRowDataWrapper() {
    if (rowDataWrapper == null) {
      rowDataWrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
    }
    return rowDataWrapper;
  }

  @Override
  public Integer getKey(RowData rowData) throws Exception {
    partitionKey.partition(lazyRowDataWrapper().wrap(rowData));
    Optional<Integer> bucketId = BucketPartitionKeySelector.extractInteger(partitionKey.toPath());

    if (!bucketId.isPresent()) {
      throw new IllegalStateException("Unable to extract bucket in partition key: " + partitionKey);
    }

    return bucketId.get();
  }

  /**
   * Utility method to extract a bucketId from a string value. Input examples: bucket[5] or
   * bucket_data[11], would return 5 and 11 bucket Ids respectively.
   *
   * @param value the string with the Bucket Id
   * @return the Bucket Id as an Integer or Empty if not found
   */
  public static Optional<Integer> extractInteger(String value) {
    Matcher match = intPattern.matcher(value);
    return match.find() ? Optional.of(Integer.parseInt(match.group())) : Optional.empty();
  }
}
