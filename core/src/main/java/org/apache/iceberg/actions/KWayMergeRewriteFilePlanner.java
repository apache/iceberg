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
package org.apache.iceberg.actions;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * A planner for K-way merge rewriting that extends bin-pack planning with sort-key-aware file
 * ordering.
 *
 * <p>Files are sorted by their lower bounds on the first sort field before bin-packing so that
 * adjacent files in key space are packed into the same group, minimizing cross-group overlap.
 */
public class KWayMergeRewriteFilePlanner extends BinPackRewriteFilePlanner {

  public KWayMergeRewriteFilePlanner(
      Table table, Expression filter, Long snapshotId, boolean caseSensitive) {
    super(table, filter, snapshotId, caseSensitive);
  }

  @Override
  protected TableScan buildTableScan() {
    return super.buildTableScan().includeColumnStats();
  }

  @Override
  protected Iterable<List<FileScanTask>> planFileGroups(Iterable<FileScanTask> tasks) {
    SortOrder sortOrder = table().sortOrder();
    Preconditions.checkArgument(
        !sortOrder.isUnsorted(),
        "K-way merge requires a table sort order, but table %s is unsorted. "
            + "Use the SORT strategy to sort the files first.",
        table().name());

    int sortFieldId = sortOrder.fields().get(0).sourceId();
    Types.NestedField sortField = table().schema().findField(sortFieldId);
    Type.PrimitiveType sortFieldType = sortField.type().asPrimitiveType();
    Comparator<Object> valueComparator = Comparators.forType(sortFieldType);

    List<FileScanTask> taskList = Lists.newArrayList(tasks);

    taskList.sort(
        Comparator.comparing(
            (FileScanTask task) -> {
              Map<Integer, ByteBuffer> lowerBounds = task.file().lowerBounds();
              if (lowerBounds == null || !lowerBounds.containsKey(sortFieldId)) {
                return null;
              }
              return Conversions.fromByteBuffer(sortFieldType, lowerBounds.get(sortFieldId));
            },
            Comparator.nullsLast(valueComparator)));

    return super.planFileGroups(taskList);
  }
}
