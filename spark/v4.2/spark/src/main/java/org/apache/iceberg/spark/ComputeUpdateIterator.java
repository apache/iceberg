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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.BaseOrdering;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.jdk.javaapi.CollectionConverters;

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
  private final BaseOrdering ordering;
  private final UnsafeProjection updateBefore;
  private final UnsafeProjection updateAfter;

  private InternalRow cachedRow = null;

  ComputeUpdateIterator(
      Iterator<InternalRow> rowIterator, StructType rowType, String[] identifierFields) {
    super(rowIterator, rowType);
    this.ordering =
        ordering(Arrays.stream(identifierFields).mapToInt(rowType::fieldIndex).toArray());
    this.updateBefore = updateProjection(UPDATE_BEFORE);
    this.updateAfter = updateProjection(UPDATE_AFTER);
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
  public InternalRow next() {
    // if there is an updated cached row, return it directly
    if (cachedUpdateRecord()) {
      InternalRow row = cachedRow;
      cachedRow = null;
      return row;
    }

    // either a cached record which is not an UPDATE or the next record in the iterator.
    InternalRow currentRow = currentRow();

    if (changeType(currentRow).equals(DELETE)) {
      currentRow = currentRow.copy();
      if (!rowIterator().hasNext()) {
        return currentRow;
      }

      InternalRow nextRow = rowIterator().next();
      if (ordering.compare(currentRow, nextRow) == 0) {
        Preconditions.checkState(
            changeType(nextRow).equals(INSERT),
            "Cannot compute updates because there are multiple rows with the same identifier"
                + " fields([%s]). Please make sure the rows are unique.",
            String.join(",", identifierFields));

        currentRow = updateBefore.apply(currentRow).copy();
        cachedRow = updateAfter.apply(nextRow).copy();
      } else {
        cachedRow = nextRow.copy();
      }
    }

    return currentRow;
  }

  private UnsafeProjection updateProjection(UTF8String changeType) {
    List<Expression> fields = Lists.newArrayListWithCapacity(rowType().size());
    for (int index = 0; index < rowType().size(); index++) {
      fields.add(
          index == changeTypeIndex()
              ? Literal.create(changeType, DataTypes.StringType)
              : new BoundReference(index, rowType().fields()[index].dataType(), true));
    }

    return UnsafeProjection.create(CollectionConverters.asScala(fields).toSeq());
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
