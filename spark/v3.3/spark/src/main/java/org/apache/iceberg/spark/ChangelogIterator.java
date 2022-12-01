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

public class ChangelogIterator implements Iterator<Row>, Serializable {
  private static final String DELETE = ChangelogOperation.DELETE.name();
  private static final String INSERT = ChangelogOperation.INSERT.name();

  private final Iterator<Row> rowIterator;
  private final int changeTypeIndex;
  private final List<Integer> partitionIdx;
  private final boolean markUpdatedRows;

  private Row cachedRow = null;

  public ChangelogIterator(
      Iterator<Row> rowIterator,
      int changeTypeIndex,
      List<Integer> partitionIdx,
      boolean markUpdatedRows) {
    this.rowIterator = rowIterator;
    this.changeTypeIndex = changeTypeIndex;
    this.partitionIdx = partitionIdx;
    this.markUpdatedRows = markUpdatedRows;
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
    // if there is a processed cached row, return it directly
    if (cachedRow != null
        && !cachedRow.getString(changeTypeIndex).equals(DELETE)
        && !cachedRow.getString(changeTypeIndex).equals(INSERT)) {
      Row row = cachedRow;
      cachedRow = null;
      return row;
    }

    Row currentRow = currentRow();

    if (rowIterator.hasNext()) {
      GenericRowWithSchema nextRow = (GenericRowWithSchema) rowIterator.next();

      if (withinPartition(currentRow, nextRow)
          && currentRow.getString(changeTypeIndex).equals(DELETE)
          && nextRow.getString(changeTypeIndex).equals(INSERT)) {

        GenericInternalRow deletedRow =
            new GenericInternalRow(((GenericRowWithSchema) currentRow).values());
        GenericInternalRow insertedRow = new GenericInternalRow(nextRow.values());

        // set the change_type to the same value
        deletedRow.update(changeTypeIndex, "");
        insertedRow.update(changeTypeIndex, "");

        if (deletedRow.equals(insertedRow)) {
          // remove two carry-over rows
          currentRow = null;
          this.cachedRow = null;
        } else if (markUpdatedRows) {
          // mark the updated rows
          deletedRow.update(changeTypeIndex, ChangelogOperation.UPDATE_BEFORE.name());
          currentRow = RowFactory.create(deletedRow.values());

          insertedRow.update(changeTypeIndex, ChangelogOperation.UPDATE_AFTER.name());
          this.cachedRow = RowFactory.create(insertedRow.values());
        } else {
          this.cachedRow = nextRow;
        }

      } else {
        this.cachedRow = nextRow;
      }
    }

    return currentRow;
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

  private boolean withinPartition(Row currentRow, Row nextRow) {
    for (int i = 0; i < partitionIdx.size(); i++) {
      int idx = partitionIdx.get(i);
      if (!nextRow.get(idx).equals(currentRow.get(idx))) {
        return false;
      }
    }
    return true;
  }
}
