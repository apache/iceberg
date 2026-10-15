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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.BaseOrdering;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
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

  private final String identifierFieldNames;
  private final BaseOrdering ordering;
  private final UnsafeProjection updateBefore;
  private final UnsafeProjection updateAfter;

  private InternalRow cachedRow = null;

  ComputeUpdateIterator(
      Iterator<InternalRow> rowIterator, StructType rowType, String[] identifierFields) {
    super(rowIterator, rowType);
    this.ordering =
        createOrdering(Arrays.stream(identifierFields).mapToInt(rowType::fieldIndex).toArray());
    this.updateBefore = changeTypeProjection(UPDATE_BEFORE);
    this.updateAfter = changeTypeProjection(UPDATE_AFTER);
    this.identifierFieldNames = String.join(",", identifierFields);
  }

  @Override
  public boolean hasNext() {
    if (cachedRow != null) {
      return true;
    }
    return rowIterator().hasNext();
  }

  @Override
  public InternalRow next() {
    // if there is an updated cached row, return it directly
    if (cachedUpdateRecord()) {
      InternalRow row = cachedRow;
      cachedRow = null;
      return row;
    }

    // either a cached record which is not an UPDATE or the next record in the iterator.
    // cached rows are always copies, so only rows taken from the iterator need to be copied below
    boolean cached = cachedRow != null;
    InternalRow currentRow = currentRow();

    if (!changeType(currentRow).equals(DELETE)) {
      return currentRow;
    }

    // copy before probing the upstream iterator, which may invalidate the row it just returned
    if (!cached) {
      currentRow = currentRow.copy();
    }

    if (!rowIterator().hasNext()) {
      return currentRow;
    }

    InternalRow nextRow = rowIterator().next();
    if (ordering.compare(currentRow, nextRow) != 0) {
      cachedRow = nextRow.copy();
      return currentRow;
    }

    Preconditions.checkState(
        changeType(nextRow).equals(INSERT),
        "Cannot compute updates because there are multiple rows with the same identifier"
            + " fields([%s]). Please make sure the rows are unique.",
        identifierFieldNames);

    cachedRow = updateAfter.apply(nextRow).copy();
    return updateBefore.apply(currentRow).copy();
  }

  private boolean cachedUpdateRecord() {
    return cachedRow != null && changeType(cachedRow).equals(UPDATE_AFTER);
  }

  private InternalRow currentRow() {
    if (cachedRow != null) {
      InternalRow row = cachedRow;
      cachedRow = null;
      return row;
    } else {
      return rowIterator().next();
    }
  }
}
