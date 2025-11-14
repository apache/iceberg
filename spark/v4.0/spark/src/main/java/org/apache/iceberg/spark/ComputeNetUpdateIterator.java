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
 * An iterator that computes net changes across multiple snapshots by combining the first and last
 * change for each logical row.
 *
 * <p>This iterator produces a single operation per logical row:
 *
 * <ul>
 *   <li>INSERT: row was inserted and not deleted
 *   <li>DELETE: row was deleted and not reinserted
 *   <li>UPDATE_BEFORE/UPDATE_AFTER: row was modified (tracks first DELETE and last INSERT)
 * </ul>
 *
 * <p>When processing multiple changes for the same logical row:
 *
 * <ul>
 *   <li>Multiple DELETEs: only the first is kept
 *   <li>Multiple INSERTs: only the last is kept
 *   <li>DELETE then INSERT: converted to UPDATE_BEFORE and UPDATE_AFTER
 * </ul>
 */
public class ComputeNetUpdateIterator extends ComputeUpdateIterator {

  private Row cachedUpdateRow = null;
  private Row cachedRow = null;

  ComputeNetUpdateIterator(
      Iterator<Row> rowIterator, StructType rowType, String[] identifierFields) {
    super(rowIterator, rowType, identifierFields);
  }

  @Override
  public boolean hasNext() {
    if (cachedUpdateRow != null || cachedRow != null) {
      return true;
    }
    return rowIterator().hasNext();
  }

  @Override
  public Row next() {
    if (cachedUpdateRow != null) {
      Row row = cachedUpdateRow;
      cachedUpdateRow = null;
      return row;
    }

    ChangeAccumulator accumulator = processLogicalRowGroup(currentRow());
    cachedRow = accumulator.nextRow;

    return buildResultRow(accumulator);
  }

  private ChangeAccumulator processLogicalRowGroup(Row currentRow) {
    Row firstDelete = changeType(currentRow).equals(DELETE) ? currentRow : null;
    Row lastInsert = changeType(currentRow).equals(INSERT) ? currentRow : null;

    while (rowIterator().hasNext()) {
      Row nextRow = rowIterator().next();
      if (!sameLogicalRow(currentRow, nextRow)) {
        return new ChangeAccumulator(firstDelete, lastInsert, nextRow);
      }

      if (firstDelete == null && changeType(nextRow).equals(DELETE)) {
        firstDelete = nextRow;
      }
      if (changeType(nextRow).equals(INSERT)) {
        lastInsert = nextRow;
      }
    }

    return new ChangeAccumulator(firstDelete, lastInsert);
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

  private Row buildResultRow(ChangeAccumulator accumulator) {
    if (accumulator.firstDelete != null && accumulator.lastInsert != null) {
      cachedUpdateRow = modify(accumulator.lastInsert, changeTypeIndex(), UPDATE_AFTER);
      return modify(accumulator.firstDelete, changeTypeIndex(), UPDATE_BEFORE);
    } else if (accumulator.firstDelete != null) {
      return accumulator.firstDelete;
    } else {
      return accumulator.lastInsert;
    }
  }

  /**
   * Container for accumulated changes and the next row from a different logical group.
   *
   * <p>Stores the first DELETE and last INSERT for a logical row, plus the next row that belongs to
   * a different logical group (used for look-ahead caching).
   */
  private static class ChangeAccumulator {
    private final Row firstDelete;
    private final Row lastInsert;
    private final Row nextRow;

    ChangeAccumulator(Row firstDelete, Row lastInsert) {
      this.firstDelete = firstDelete;
      this.lastInsert = lastInsert;
      this.nextRow = null;
    }

    ChangeAccumulator(Row firstDelete, Row lastInsert, Row nextRow) {
      this.firstDelete = firstDelete;
      this.lastInsert = lastInsert;
      this.nextRow = nextRow;
    }
  }
}
