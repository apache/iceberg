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
import java.util.Objects;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

/**
 * An iterator that transforms rows from changelog tables within a single Spark task. It assumes
 * that rows are sorted by identifier columns and change type.
 *
 * <p>It removes the carry-over rows. Carry-over rows are the result of a removal and insertion of
 * the same row within an operation because of the copy-on-write mechanism. For example, given a
 * file which contains row1 (id=1, data='a') and row2 (id=2, data='b'). A copy-on-write delete of
 * row2 would require erasing this file and preserving row1 in a new file. The change-log table
 * would report this as (id=1, data='a', op='DELETE') and (id=1, data='a', op='INSERT'), despite it
 * not being an actual change to the table. The iterator finds the carry-over rows and removes them
 * from the result.
 *
 * <p>This iterator also finds delete/insert rows which represent an update, and converts them into
 * update records. For example, these two rows
 *
 * <ul>
 *   <li>(id=1, data='a', op='DELETE')
 *   <li>(id=1, data='b', op='INSERT')
 * </ul>
 *
 * <p>will be marked as update-rows:
 *
 * <ul>
 *   <li>(id=1, data='a', op='UPDATE_BEFORE')
 *   <li>(id=1, data='b', op='UPDATE_AFTER')
 * </ul>
 */
public class ChangelogIterator implements Iterator<Row> {
  private static final String DELETE = ChangelogOperation.DELETE.name();
  private static final String INSERT = ChangelogOperation.INSERT.name();
  private static final String UPDATE_BEFORE = ChangelogOperation.UPDATE_BEFORE.name();
  private static final String UPDATE_AFTER = ChangelogOperation.UPDATE_AFTER.name();

  private final Iterator<Row> rowIterator;
  private final int changeTypeIndex;
  private final List<Integer> identifierFieldIdx;
  private final int[] indicesForIdentifySameRow;

  private Row cachedRow = null;

  private ChangelogIterator(
      Iterator<Row> rowIterator,
      int changeTypeIndex,
      List<Integer> identifierFieldIdx,
      int columnSize) {
    this.rowIterator = rowIterator;
    this.changeTypeIndex = changeTypeIndex;
    this.identifierFieldIdx = identifierFieldIdx;
    this.indicesForIdentifySameRow = generateIndicesForIdentifySameRow(columnSize);
  }

  /**
   * Creates an iterator for records of a changelog table.
   *
   * @param rowIterator the iterator of rows from a changelog table
   * @param changeTypeIndex the index of the change type column
   * @param identifierFieldIdx the indices of the identifier columns, which determine if rows are
   *     the same
   * @param columnSize the number of columns in the table
   * @return a new {@link ChangelogIterator} instance concatenated with the null-removal iterator
   */
  public static Iterator<Row> create(
      Iterator<Row> rowIterator,
      int changeTypeIndex,
      List<Integer> identifierFieldIdx,
      int columnSize) {
    ChangelogIterator changelogIterator =
        new ChangelogIterator(rowIterator, changeTypeIndex, identifierFieldIdx, columnSize);
    return Iterators.filter(changelogIterator, Objects::nonNull);
  }

  @Override
  public boolean hasNext() {
    if (cachedRow != null) {
      return true;
    }
    return rowIterator.hasNext();
  }

  @Override
  public Row next() {
    // if there is an updated cached row, return it directly
    if (cachedUpdateRecord(cachedRow)) {
      Row row = cachedRow;
      cachedRow = null;
      return row;
    }

    Row currentRow = currentRow();

    if (currentRow.getString(changeTypeIndex).equals(DELETE) && rowIterator.hasNext()) {
      Row nextRow = rowIterator.next();
      cachedRow = nextRow;

      if (isUpdateOrCarryoverRecord(currentRow, nextRow)) {
        if (isCarryoverRecord(currentRow, nextRow)) {
          // set carry-over rows to null for filtering out later
          currentRow = null;
          cachedRow = null;
        } else {
          currentRow = modify(currentRow, changeTypeIndex, UPDATE_BEFORE);
          cachedRow = modify(nextRow, changeTypeIndex, UPDATE_AFTER);
        }
      }
    }

    return currentRow;
  }

  private Row modify(Row row, int valueIndex, Object value) {
    if (row instanceof GenericRow) {
      GenericRow genericRow = (GenericRow) row;
      genericRow.values()[valueIndex] = value;
      return genericRow;
    } else {
      Object[] values = new Object[row.size()];
      for (int index = 0; index < row.size(); index++) {
        values[index] = row.get(index);
      }
      values[valueIndex] = value;
      return RowFactory.create(values);
    }
  }

  private int[] generateIndicesForIdentifySameRow(int columnSize) {
    int[] indices = new int[columnSize - 1];
    for (int i = 0; i < indices.length; i++) {
      if (i < changeTypeIndex) {
        indices[i] = i;
      } else {
        indices[i] = i + 1;
      }
    }
    return indices;
  }

  private boolean isCarryoverRecord(Row currentRow, Row nextRow) {
    for (int idx : indicesForIdentifySameRow) {
      if (!isColumnSame(currentRow, nextRow, idx)) {
        return false;
      }
    }

    return true;
  }

  private boolean cachedUpdateRecord(Row cachedRecord) {
    return cachedRecord != null
        && !cachedRecord.getString(changeTypeIndex).equals(DELETE)
        && !cachedRecord.getString(changeTypeIndex).equals(INSERT);
  }

  private Row currentRow() {
    if (cachedRow != null) {
      Row row = cachedRow;
      cachedRow = null;
      return row;
    } else {
      return rowIterator.next();
    }
  }

  private boolean isUpdateOrCarryoverRecord(Row currentRow, Row nextRow) {
    return sameLogicalRow(currentRow, nextRow)
        && currentRow.getString(changeTypeIndex).equals(DELETE)
        && nextRow.getString(changeTypeIndex).equals(INSERT);
  }

  private boolean sameLogicalRow(Row currentRow, Row nextRow) {
    for (int idx : identifierFieldIdx) {
      if (!isColumnSame(currentRow, nextRow, idx)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isColumnSame(Row currentRow, Row nextRow, int idx) {
    return Objects.equals(nextRow.get(idx), currentRow.get(idx));
  }
}
