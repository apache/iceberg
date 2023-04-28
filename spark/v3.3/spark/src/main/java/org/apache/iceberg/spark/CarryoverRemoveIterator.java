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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * An iterator that removes the carry-over rows from changelog tables within a single Spark task. It
 * assumes that rows are partitioned by identifier(or all) columns, and it is sorted by both
 * identifier(or all) columns and change type.
 *
 * <p>Carry-over rows are the result of a removal and insertion of the same row within an operation
 * because of the copy-on-write mechanism. For example, given a file which contains row1 (id=1,
 * data='a') and row2 (id=2, data='b'). A copy-on-write delete of row2 would require erasing this
 * file and preserving row1 in a new file. The change-log table would report this as follows,
 * despite it not being an actual change to the table.
 *
 * <ul>
 *   <li>(id=1, data='a', op='DELETE')
 *   <li>(id=1, data='a', op='INSERT')
 *   <li>(id=2, data='b', op='DELETE')
 * </ul>
 *
 * The iterator finds the carry-over rows and removes them from the result. For example, the above
 * rows will be converted to:
 *
 * <ul>
 *   <li>(id=2, data='b', op='DELETE')
 * </ul>
 */
class CarryoverRemoveIterator extends ChangelogIterator {
  private final Iterator<Row> rowIterator;
  private final int[] indicesForIdentifySameRow;

  private Row deletedRow = null;
  private long deletedRowCount = 0;
  private Row nextCachedRow = null;

  CarryoverRemoveIterator(Iterator<Row> rowIterator, StructType rowType) {
    super(rowIterator, rowType);
    this.rowIterator = rowIterator;
    this.indicesForIdentifySameRow = generateIndicesForIdentifySameRow(rowType.size());
  }

  @Override
  public boolean hasNext() {
    if (hasDeleteRow() || nextCachedRow != null) {
      return true;
    }
    return rowIterator.hasNext();
  }

  @Override
  public Row next() {
    if (popupDeleteRow()) {
      deletedRowCount--;
      return deletedRow;
    }

    Row currentRow = currentRow();

    if (currentRow.getString(changeTypeIndex()).equals(DELETE) && rowIterator.hasNext()) {
      // cache the delete row if there is 0 delete row cached
      if (!hasDeleteRow()) {
        deletedRow = currentRow;
        deletedRowCount++;
      }

      Row nextRow = rowIterator.next();

      if (isSameRecord(currentRow, nextRow)) {
        if (nextRow.getString(changeTypeIndex()).equals(INSERT)) {
          deletedRowCount--;
        } else {
          deletedRowCount++;
        }
      } else {
        // mark the boundary since the next row is not the same record as the current row
        nextCachedRow = nextRow;
      }

      currentRow = null;
    }

    return currentRow;
  }

  /**
   * Pop up the delete rows if there are delete rows cached and the next row is not the same record
   * or there is no next row.
   */
  private boolean popupDeleteRow() {
    return hitBoundary() && hasDeleteRow();
  }

  private boolean hitBoundary() {
    return !rowIterator.hasNext() || nextCachedRow != null;
  }

  private boolean hasDeleteRow() {
    return deletedRowCount > 0;
  }

  private Row currentRow() {
    if (nextCachedRow != null) {
      Row currentRow = nextCachedRow;
      nextCachedRow = null;
      return currentRow;
    } else if (hasDeleteRow()) {
      return deletedRow;
    } else {
      return rowIterator.next();
    }
  }

  private int[] generateIndicesForIdentifySameRow(int columnSize) {
    int[] indices = new int[columnSize - 1];
    for (int i = 0; i < indices.length; i++) {
      if (i < changeTypeIndex()) {
        indices[i] = i;
      } else {
        indices[i] = i + 1;
      }
    }
    return indices;
  }

  protected boolean isSameRecord(Row currentRow, Row nextRow) {
    for (int idx : indicesForIdentifySameRow) {
      if (!isColumnSame(currentRow, nextRow, idx)) {
        return false;
      }
    }

    return true;
  }
}
