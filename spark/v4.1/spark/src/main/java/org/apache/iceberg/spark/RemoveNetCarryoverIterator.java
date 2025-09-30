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
import java.util.Set;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * This class computes the net changes across multiple snapshots. It is different from {@link
 * org.apache.iceberg.spark.RemoveCarryoverIterator}, which only removes carry-over rows within a
 * single snapshot. It takes a row iterator, and assumes the following:
 *
 * <ul>
 *   <li>The row iterator is partitioned by all columns.
 *   <li>The row iterator is sorted by all columns, change order, and change type. The change order
 *       is 1-to-1 mapping to snapshot id.
 * </ul>
 */
public class RemoveNetCarryoverIterator extends ChangelogIterator {

  private final int[] indicesToIdentifySameRow;

  private Row cachedNextRow;
  private Row cachedRow;
  private long cachedRowCount;

  protected RemoveNetCarryoverIterator(Iterator<Row> rowIterator, StructType rowType) {
    super(rowIterator, rowType);
    this.indicesToIdentifySameRow = generateIndicesToIdentifySameRow();
  }

  @Override
  public boolean hasNext() {
    if (cachedRowCount > 0) {
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
    if (cachedRowCount > 0) {
      cachedRowCount--;
      return cachedRow;
    }

    cachedRow = getCurrentRow();
    // return it directly if there is no more rows
    if (!rowIterator().hasNext()) {
      return cachedRow;
    }
    cachedRowCount = 1;

    cachedNextRow = rowIterator().next();

    // pull rows from the iterator until two consecutive rows are different
    while (isSameRecord(cachedRow, cachedNextRow, indicesToIdentifySameRow)) {
      if (oppositeChangeType(cachedRow, cachedNextRow)) {
        // two rows with opposite change types means no net changes, remove both
        cachedRowCount--;
      } else {
        // two rows with same change types means potential net changes, cache the next row
        cachedRowCount++;
      }

      // stop pulling rows if there is no more rows or the next row is different
      if (cachedRowCount <= 0 || !rowIterator().hasNext()) {
        // reset the cached next row if there is no more rows
        cachedNextRow = null;
        break;
      }

      cachedNextRow = rowIterator().next();
    }

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

  private boolean oppositeChangeType(Row currentRow, Row nextRow) {
    return (changeType(nextRow).equals(INSERT) && changeType(currentRow).equals(DELETE))
        || (changeType(nextRow).equals(DELETE) && changeType(currentRow).equals(INSERT));
  }

  private int[] generateIndicesToIdentifySameRow() {
    Set<Integer> metadataColumnIndices =
        Sets.newHashSet(
            rowType().fieldIndex(MetadataColumns.CHANGE_ORDINAL.name()),
            rowType().fieldIndex(MetadataColumns.COMMIT_SNAPSHOT_ID.name()),
            changeTypeIndex());
    return generateIndicesToIdentifySameRow(rowType().size(), metadataColumnIndices);
  }
}
