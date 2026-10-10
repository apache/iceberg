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
import java.util.Set;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Ascending$;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.BaseOrdering;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NullsFirst$;
import org.apache.spark.sql.catalyst.expressions.RowOrdering;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.optimizer.InsertMapSortExpression;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.jdk.javaapi.CollectionConverters;

/**
 * An iterator that transforms rows from changelog tables within a single Spark task.
 *
 * <p>Rows retained across iterator advancement must be copied by the caller.
 */
public abstract class ChangelogIterator implements Iterator<InternalRow> {
  protected static final UTF8String DELETE =
      UTF8String.fromString(ChangelogOperation.DELETE.name());
  protected static final UTF8String INSERT =
      UTF8String.fromString(ChangelogOperation.INSERT.name());
  protected static final UTF8String UPDATE_BEFORE =
      UTF8String.fromString(ChangelogOperation.UPDATE_BEFORE.name());
  protected static final UTF8String UPDATE_AFTER =
      UTF8String.fromString(ChangelogOperation.UPDATE_AFTER.name());

  private final Iterator<InternalRow> rowIterator;
  private final int changeTypeIndex;
  private final StructType rowType;

  protected ChangelogIterator(Iterator<InternalRow> rowIterator, StructType rowType) {
    this.rowIterator = rowIterator;
    this.rowType = rowType;
    this.changeTypeIndex = rowType.fieldIndex(MetadataColumns.CHANGE_TYPE.name());
  }

  protected int changeTypeIndex() {
    return changeTypeIndex;
  }

  protected StructType rowType() {
    return rowType;
  }

  protected UTF8String changeType(InternalRow row) {
    UTF8String changeType = row.getUTF8String(changeTypeIndex());
    Preconditions.checkNotNull(changeType, "Change type should not be null");
    return changeType;
  }

  protected Iterator<InternalRow> rowIterator() {
    return rowIterator;
  }

  /**
   * Creates an iterator composing {@link RemoveCarryoverIterator} and {@link ComputeUpdateIterator}
   * to remove carry-over rows and compute update rows
   *
   * @param rowIterator the iterator of rows from a changelog table
   * @param rowType the schema of the rows
   * @param identifierFields the names of the identifier columns, which determine if rows are the
   *     same
   * @return a new iterator instance
   */
  public static Iterator<InternalRow> computeUpdates(
      Iterator<InternalRow> rowIterator, StructType rowType, String[] identifierFields) {
    Iterator<InternalRow> carryoverRemoveIterator = removeCarryovers(rowIterator, rowType);
    ChangelogIterator changelogIterator =
        new ComputeUpdateIterator(carryoverRemoveIterator, rowType, identifierFields);
    return Iterators.filter(changelogIterator, Objects::nonNull);
  }

  /**
   * Creates an iterator that removes carry-over rows from a changelog table.
   *
   * @param rowIterator the iterator of rows from a changelog table
   * @param rowType the schema of the rows
   * @return a new iterator instance
   */
  public static Iterator<InternalRow> removeCarryovers(
      Iterator<InternalRow> rowIterator, StructType rowType) {
    RemoveCarryoverIterator changelogIterator = new RemoveCarryoverIterator(rowIterator, rowType);
    return Iterators.filter(changelogIterator, Objects::nonNull);
  }

  public static Iterator<InternalRow> removeNetCarryovers(
      Iterator<InternalRow> rowIterator, StructType rowType) {
    ChangelogIterator changelogIterator = new RemoveNetCarryoverIterator(rowIterator, rowType);
    return Iterators.filter(changelogIterator, Objects::nonNull);
  }

  BaseOrdering ordering(int[] indices) {
    List<SortOrder> sortOrders = Lists.newArrayListWithCapacity(indices.length);
    for (int index : indices) {
      Expression field = new BoundReference(index, rowType.fields()[index].dataType(), true);
      sortOrders.add(
          new SortOrder(
              InsertMapSortExpression.insertMapSortRecursively(field),
              Ascending$.MODULE$,
              NullsFirst$.MODULE$,
              CollectionConverters.asScala(List.<Expression>of()).toSeq()));
    }

    return RowOrdering.create(
        CollectionConverters.asScala(sortOrders).toSeq(),
        CollectionConverters.asScala(List.<Attribute>of()).toSeq());
  }

  protected static int[] generateIndicesToIdentifySameRow(
      int totalColumnCount, Set<Integer> metadataColumnIndices) {
    int[] indices = new int[totalColumnCount - metadataColumnIndices.size()];

    for (int i = 0, j = 0; i < totalColumnCount; i++) {
      if (!metadataColumnIndices.contains(i)) {
        indices[j] = i;
        j++;
      }
    }
    return indices;
  }
}
