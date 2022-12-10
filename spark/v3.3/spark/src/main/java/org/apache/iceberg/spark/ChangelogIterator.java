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

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.ChangelogOperation;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

/**
 * An iterator that transforms rows within a single task.
 *
 * <p>It marks the carry-over rows to null to be filtered out later. Carry-over rows are unchanged
 * rows in a snapshot but showed as delete-rows and insert-rows in a changelog table due to the
 * copy-on-write(COW) mechanism. For example, there are row1 (id=1, data='a') and row2 (id=2,
 * data='b') in a data file, if we only delete row2, the COW will copy row1 to a new data file and
 * delete the whole old data file. The changelog table will have two delete-rows(row1 and row2), and
 * one insert-row(row1). Row1 is a carry-over row.
 *
 * <p>The iterator marks the delete-row and insert-row to be the update-rows. For example, these two
 * rows
 *
 * <ul>
 *   <li>(id=1, data='a', op='DELETE')
 *   <li>(id=1, data='b', op='INSERT')
 * </ul>
 *
 * will be marked as update-rows:
 *
 * <ul>
 *   <li>(id=1, data='a', op='UPDATE_BEFORE')
 *   <li>(id=1, data='b', op='UPDATE_AFTER')
 * </ul>
 */
public class ChangelogIterator implements Iterator<Row>, Serializable {
  private static final String DELETE = ChangelogOperation.DELETE.name();
  private static final String INSERT = ChangelogOperation.INSERT.name();

  private final Iterator<Row> rowIterator;
  private final int changeTypeIndex;
  private final List<Integer> partitionIdx;

  private Row cachedRow = null;

  public ChangelogIterator(
      Iterator<Row> rowIterator, int changeTypeIndex, List<Integer> partitionIdx) {
    this.rowIterator = rowIterator;
    this.changeTypeIndex = changeTypeIndex;
    this.partitionIdx = partitionIdx;
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
    if (updated(cachedRow)) {
      Row row = cachedRow;
      cachedRow = null;
      return row;
    }

    Row currentRow = currentRow();

    if (rowIterator.hasNext()) {
      GenericRowWithSchema nextRow = (GenericRowWithSchema) rowIterator.next();
      cachedRow = nextRow;

      if (updateOrCarryoverRecord(currentRow, nextRow)) {
        Row[] rows = update((GenericRowWithSchema) currentRow, nextRow);

        currentRow = rows[0];
        cachedRow = rows[1];
      }
    }

    return currentRow;
  }

  private Row[] update(GenericRowWithSchema currentRow, GenericRowWithSchema nextRow) {
    GenericInternalRow deletedRow = new GenericInternalRow(currentRow.values());
    GenericInternalRow insertedRow = new GenericInternalRow(nextRow.values());

    // set the change_type to the same value
    deletedRow.update(changeTypeIndex, "");
    insertedRow.update(changeTypeIndex, "");

    if (deletedRow.equals(insertedRow)) {
      // set carry-over rows to null to be filtered out later
      return new Row[] {null, null};
    } else {
      deletedRow.update(changeTypeIndex, ChangelogOperation.UPDATE_BEFORE.name());
      insertedRow.update(changeTypeIndex, ChangelogOperation.UPDATE_AFTER.name());

      return new Row[] {
        RowFactory.create(deletedRow.values()), RowFactory.create(insertedRow.values())
      };
    }
  }

  private boolean updated(Row row) {
    return row != null
        && !row.getString(changeTypeIndex).equals(DELETE)
        && !row.getString(changeTypeIndex).equals(INSERT);
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

  private boolean updateOrCarryoverRecord(Row currentRow, Row nextRow) {
    return withinPartition(currentRow, nextRow)
        && currentRow.getString(changeTypeIndex).equals(DELETE)
        && nextRow.getString(changeTypeIndex).equals(INSERT);
  }

  private boolean withinPartition(Row currentRow, Row nextRow) {
    for (int idx : partitionIdx) {
      if (!nextRow.get(idx).equals(currentRow.get(idx))) {
        return false;
      }
    }
    return true;
  }
}
