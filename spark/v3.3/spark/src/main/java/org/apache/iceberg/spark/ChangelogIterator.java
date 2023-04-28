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
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructType;

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
  protected static final String DELETE = ChangelogOperation.DELETE.name();
  protected static final String INSERT = ChangelogOperation.INSERT.name();
  protected static final String UPDATE_BEFORE = ChangelogOperation.UPDATE_BEFORE.name();
  protected static final String UPDATE_AFTER = ChangelogOperation.UPDATE_AFTER.name();

  private final Iterator<Row> rowIterator;
  private final int changeTypeIndex;
  private final String[] identifierFields;
  private final int[] indicesForIdentifySameRow;
  private List<Integer> identifierFieldIdx = null;

  private Row cachedRow = null;

  protected ChangelogIterator(
      Iterator<Row> rowIterator, StructType rowType, String[] identifierFields) {
    this.rowIterator = rowIterator;
    this.changeTypeIndex = rowType.fieldIndex(MetadataColumns.CHANGE_TYPE.name());
    this.identifierFields = identifierFields;
    if (identifierFields != null) {
      this.identifierFieldIdx =
          Arrays.stream(identifierFields).map(rowType::fieldIndex).collect(Collectors.toList());
    }
    this.indicesForIdentifySameRow = generateIndicesForIdentifySameRow(rowType.size());
  }

  protected int changeTypeIndex() {
    return changeTypeIndex;
  }

  /**
   * Creates an iterator for records of a changelog table.
   *
   * @param rowIterator the iterator of rows from a changelog table
   * @param rowType the schema of the rows
   * @param identifierFields the names of the identifier columns, which determine if rows are the
   *     same
   * @return a new {@link ChangelogIterator} instance concatenated with the null-removal iterator
   */
  public static Iterator<Row> create(
      Iterator<Row> rowIterator, StructType rowType, String[] identifierFields) {
    Iterator<Row> carryoverRemoveIterator = createCarryoverRemoveIterator(rowIterator, rowType);
    ChangelogIterator changelogIterator =
        new ChangelogIterator(carryoverRemoveIterator, rowType, identifierFields);
    return Iterators.filter(changelogIterator, Objects::nonNull);
  }

  public static Iterator<Row> createCarryoverRemoveIterator(
      Iterator<Row> rowIterator, StructType rowType) {
    CarryoverRemoveIterator changelogIterator = new CarryoverRemoveIterator(rowIterator, rowType);
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

      if (sameLogicalRow(currentRow, nextRow)) {
        String nextRowChangeType = nextRow.getString(changeTypeIndex);

        Preconditions.checkState(
            nextRowChangeType.equals(INSERT),
            "The next row should be an INSERT row, but it is %s. That means there are multiple"
                + " rows with the same value of identifier fields(%s). Please make sure the rows are unique.",
            nextRowChangeType,
            identifierFields);

        currentRow = modify(currentRow, changeTypeIndex, UPDATE_BEFORE);
        cachedRow = modify(nextRow, changeTypeIndex, UPDATE_AFTER);
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

  protected boolean isSameRecord(Row currentRow, Row nextRow) {
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
