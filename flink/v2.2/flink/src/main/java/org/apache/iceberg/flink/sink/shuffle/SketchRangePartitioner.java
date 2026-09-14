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
package org.apache.iceberg.flink.sink.shuffle;

import java.util.Comparator;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.types.Comparators;

class SketchRangePartitioner implements Partitioner<RowData> {
  private final SortKey sortKey;
  private final Comparator<StructLike> comparator;
  private final SortKey[] rangeBounds;
  private final RowDataWrapper rowDataWrapper;

  SketchRangePartitioner(Schema schema, SortOrder sortOrder, SortKey[] rangeBounds) {
    this.sortKey = new SortKey(schema, sortOrder);
    this.comparator = Comparators.forType(SortKeyUtil.sortKeySchema(schema, sortOrder).asStruct());
    this.rangeBounds = rangeBounds;
    this.rowDataWrapper = new RowDataWrapper(FlinkSchemaUtil.convert(schema), schema.asStruct());
  }

  @Override
  public int partition(RowData row, int numPartitions) {
    // reuse the sortKey and rowDataWrapper
    sortKey.wrap(rowDataWrapper.wrap(row));
    return SketchUtil.partition(sortKey, numPartitions, rangeBounds, comparator);
  }
}
