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
package org.apache.iceberg.spark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * This class computes the net changes across multiple snapshots. It is different from {@link
 * RemoveNetCarryoverIterator}, which only nets changes across the "same" row. Here we net changes
 * based on the identifier columns when an INSERT is immediately followed by a DELETE (even if it's
 * across different snapshots). It takes a row iterator, and assumes the following:
 *
 * <ul>
 *   <li>The row iterator is partitioned by identifier fields.
 *   <li>The row iterator is sorted by identifier fields, change order, and change type. The change
 *       order is 1-to-1 mapping to snapshot id.
 * </ul>
 */
public class RemoveNoopPairIterator extends ChangelogIterator {

  private final List<Integer> identifierFieldIdx;

  private Row cachedRow;

  public RemoveNoopPairIterator(
      Iterator<Row> rowIterator, StructType rowType, String[] identifierFields) {
    super(rowIterator, rowType);
    this.identifierFieldIdx =
        Arrays.stream(identifierFields).map(rowType::fieldIndex).collect(Collectors.toList());
  }

  @Override
  public boolean hasNext() {
    return cachedRow != null || rowIterator().hasNext();
  }

  @Override
  public Row next() {
    if (cachedRow == null) {
      cachedRow = rowIterator().next();
    }

    // Early return in case of DELETE change type.
    if (changeType(cachedRow).equals(DELETE) || !rowIterator().hasNext()) {
      Row prevCachedRow = cachedRow;
      cachedRow = null;
      return prevCachedRow;
    }

    Row nextRow = rowIterator().next();
    if (!sameLogicalRow(cachedRow, nextRow) || changeType(nextRow).equals(INSERT)) {
      Row prevCachedRow = cachedRow;
      cachedRow = nextRow;
      return prevCachedRow;
    }

    cachedRow = null;
    return null;
  }

  private boolean sameLogicalRow(Row currentRow, Row nextRow) {
    for (int idx : identifierFieldIdx) {
      if (isDifferentValue(currentRow, nextRow, idx)) {
        return false;
      }
    }
    return true;
  }
}
