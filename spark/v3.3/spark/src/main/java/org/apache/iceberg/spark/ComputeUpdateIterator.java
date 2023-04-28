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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructType;

/**
 * An iterator that finds delete/insert rows which represent an update, and converts them into
 * update records from changelog tables within a single Spark task. It assumes that rows are sorted
 * by identifier columns and change type.
 *
 * <p>For example, these two rows
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
public class ComputeUpdateIterator extends ChangelogIterator {

  private final String[] identifierFields;
  private final List<Integer> identifierFieldIdx;

  private Row cachedRow = null;

  ComputeUpdateIterator(Iterator<Row> rowIterator, StructType rowType, String[] identifierFields) {
    super(rowIterator, rowType);
    this.identifierFieldIdx =
        Arrays.stream(identifierFields).map(rowType::fieldIndex).collect(Collectors.toList());
    this.identifierFields = identifierFields;
  }

  @Override
  public boolean hasNext() {
    if (cachedRow != null) {
      return true;
    }
    return rowIterator().hasNext();
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

    if (currentRow.getString(changeTypeIndex()).equals(DELETE) && rowIterator().hasNext()) {
      Row nextRow = rowIterator().next();
      cachedRow = nextRow;

      if (sameLogicalRow(currentRow, nextRow)) {
        String nextRowChangeType = nextRow.getString(changeTypeIndex());

        Preconditions.checkState(
            nextRowChangeType.equals(INSERT),
            "The next row should be an INSERT row, but it is %s. That means there are multiple"
                + " rows with the same value of identifier fields([%s]). Please make sure the rows are unique.",
            nextRowChangeType,
            String.join(",", identifierFields));

        currentRow = modify(currentRow, changeTypeIndex(), UPDATE_BEFORE);
        cachedRow = modify(nextRow, changeTypeIndex(), UPDATE_AFTER);
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

  private boolean cachedUpdateRecord(Row cachedRecord) {
    return cachedRecord != null
        && !cachedRecord.getString(changeTypeIndex()).equals(DELETE)
        && !cachedRecord.getString(changeTypeIndex()).equals(INSERT);
  }

  private Row currentRow() {
    if (cachedRow != null) {
      Row row = cachedRow;
      cachedRow = null;
      return row;
    } else {
      return rowIterator().next();
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
}
