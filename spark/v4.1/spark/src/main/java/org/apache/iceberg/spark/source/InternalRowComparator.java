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
package org.apache.iceberg.spark.source;

import java.util.Comparator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderComparators;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

/**
 * A comparator for Spark {@link InternalRow} objects based on an Iceberg {@link SortOrder}.
 *
 * <p>This comparator adapts Spark's InternalRow to Iceberg's StructLike interface and delegates to
 * Iceberg's existing {@link SortOrderComparators} infrastructure, which provides full support for:
 *
 * <ul>
 *   <li>All Iceberg data types
 *   <li>ASC/DESC sort directions
 *   <li>NULLS_FIRST/NULLS_LAST null ordering
 *   <li>Transform functions (identity, bucket, truncate, etc.)
 * </ul>
 *
 * <p><strong>This class is NOT thread-safe.</strong>
 */
class InternalRowComparator implements Comparator<InternalRow> {
  private final Comparator<StructLike> delegate;
  private final InternalRowWrapper leftWrapper;
  private final InternalRowWrapper rightWrapper;

  /**
   * Creates a comparator for the given sort order and schemas.
   *
   * @param sortOrder the Iceberg sort order to use for comparison
   * @param sparkSchema the Spark schema of the rows to compare
   * @param icebergSchema the Iceberg schema of the rows to compare
   */
  InternalRowComparator(SortOrder sortOrder, StructType sparkSchema, Schema icebergSchema) {
    Preconditions.checkArgument(
        sortOrder.isSorted(), "Cannot create comparator for unsorted order");
    Preconditions.checkNotNull(sparkSchema, "Spark schema cannot be null");
    Preconditions.checkNotNull(icebergSchema, "Iceberg schema cannot be null");

    this.delegate = SortOrderComparators.forSchema(icebergSchema, sortOrder);
    this.leftWrapper = new InternalRowWrapper(sparkSchema, icebergSchema.asStruct());
    this.rightWrapper = new InternalRowWrapper(sparkSchema, icebergSchema.asStruct());
  }

  @Override
  public int compare(InternalRow row1, InternalRow row2) {
    return delegate.compare(leftWrapper.wrap(row1), rightWrapper.wrap(row2));
  }
}
