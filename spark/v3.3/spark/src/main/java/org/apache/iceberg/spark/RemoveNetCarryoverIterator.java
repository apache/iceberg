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

import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * This class computes the net changes across multiple snapshots. It takes a row iterator, and
 * assumes the following:
 *
 * <ul>
 *   <li>The row iterator is partitioned by the primary key.
 *   <li>The row iterator is sorted by the primary key, change order, and change type. The change
 *       order is 1-to-1 mapping to snapshot id.
 * </ul>
 */
public class RemoveNetCarryoverIterator extends RemoveCarryoverIterator {

  private Row cachedNextRow = null;
  private final List<Row> cachedRows = Lists.newArrayList();

  protected RemoveNetCarryoverIterator(Iterator<Row> rowIterator, StructType rowType) {
    super(rowIterator, rowType);
  }

  @Override
  public boolean hasNext() {
    if (!cachedRows.isEmpty()) {
      return true;
    }

    if (cachedNextRow != null) {
      return true;
    }

    return rowIterator().hasNext();
  }

  @Override
  public Row next() {
    // if there are cached rows, return one of them from the beginning
    if (!cachedRows.isEmpty()) {
      return cachedRows.remove(0);
    }

    Row currentRow = getCurrentRow();

    // if there is no next row, return the current row
    if (!rowIterator().hasNext()) {
      return currentRow;
    }

    Row nextRow = rowIterator().next();

    cachedRows.add(currentRow);
    // if they are the same row, remove the cached row or stack to the cached row
    while (isSameRecord(currentRow, nextRow)) {
      if (matched(currentRow, nextRow)) {
        // if matched, remove both rows, remove the last row in the cache
        cachedRows.remove(cachedRows.size() - 1);
        nextRow = null;
      } else {
        // stack the row into the cache
        cachedRows.add(nextRow);
        nextRow = null;
      }

      // break the loop if there is no next row or the cache is empty
      if (cachedRows.isEmpty() || !rowIterator().hasNext()) {
        break;
      }

      // get the current row from the cache, and get the next row from the iterator for the next
      // loop
      currentRow = cachedRows.get(cachedRows.size() - 1);
      nextRow = rowIterator().next();
    }

    // if they are different rows, hit the boundary, cache the next row
    cachedNextRow = nextRow;
    return null;
  }

  private Row getCurrentRow() {
    Row currentRow;
    if (cachedNextRow != null) {
      currentRow = cachedNextRow;
      cachedNextRow = null;
    } else {
      currentRow = rowIterator().next();
    }
    return currentRow;
  }

  private boolean matched(Row currentRow, Row nextRow) {
    return (nextRow.getString(changeTypeIndex()).equals(INSERT)
            && currentRow.getString(changeTypeIndex()).equals(DELETE))
        || (nextRow.getString(changeTypeIndex()).equals(DELETE)
            && currentRow.getString(changeTypeIndex()).equals(INSERT));
  }
}
