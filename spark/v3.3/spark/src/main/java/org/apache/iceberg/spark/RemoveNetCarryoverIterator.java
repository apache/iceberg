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
import org.apache.iceberg.MetadataColumns;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * This class computes the net changes across multiple snapshots. It takes a row iterator, and
 * assumes the following:
 *
 * <ul>
 *   <li>The row iterator is partitioned by all columns.
 *   <li>The row iterator is sorted by all columns, change order, and change type. The change order
 *       is 1-to-1 mapping to snapshot id.
 * </ul>
 */
public class RemoveNetCarryoverIterator extends RemoveCarryoverIterator {

  private final int[] indicesToIdentifySameRow;

  private Row cachedNextRow = null;
  private Row cachedRow = null;
  private long cachedRowCount = 0;

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

    Row currentRow = getCurrentRow();

    // return it directly if the current row is the last row
    if (!rowIterator().hasNext()) {
      return currentRow;
    }

    Row nextRow = rowIterator().next();

    cachedRow = currentRow;
    cachedRowCount = 1;

    // pull rows from the iterator until two consecutive rows are different
    while (isSameRecord(currentRow, nextRow)) {
      if (oppositeChangeType(currentRow, nextRow)) {
        // two rows with opposite change types means no net changes
        cachedRowCount--;
        nextRow = null;
      } else {
        // two rows with same change types means potential net changes
        nextRow = null;
        cachedRowCount++;
      }

      // stop pulling rows if there is no more rows or the next row is different
      if (cachedRowCount <= 0 || !rowIterator().hasNext()) {
        break;
      }

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

  private boolean oppositeChangeType(Row currentRow, Row nextRow) {
    return (nextRow.getString(changeTypeIndex()).equals(INSERT)
            && currentRow.getString(changeTypeIndex()).equals(DELETE))
        || (nextRow.getString(changeTypeIndex()).equals(DELETE)
            && currentRow.getString(changeTypeIndex()).equals(INSERT));
  }

  private int[] generateIndicesToIdentifySameRow() {
    int changeOrdinalIndex = rowType().fieldIndex(MetadataColumns.CHANGE_ORDINAL.name());
    int snapshotIdIndex = rowType().fieldIndex(MetadataColumns.COMMIT_SNAPSHOT_ID.name());

    int[] indices = new int[rowType().size() - 3];

    for (int i = 0, j = 0; i < indices.length; i++) {
      if (i != changeTypeIndex() && i != changeOrdinalIndex && i != snapshotIdIndex) {
        indices[j] = i;
        j++;
      }
    }
    return indices;
  }

  @Override
  protected int[] indicesToIdentifySameRow() {
    return indicesToIdentifySameRow;
  }
}
