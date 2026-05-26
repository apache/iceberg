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
package org.apache.iceberg.flink.source.enumerator;

import java.util.Collection;
import javax.annotation.Nullable;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class ContinuousEnumerationResult {

  /**
   * How a result wants the checkpointed lazy-bulk-scan cursor to change when the enumerator commits
   * it.
   */
  enum CursorAction {
    /** Leave the cursor untouched. Used by eager planners and incremental-phase pages. */
    NONE,
    /**
     * Set the cursor to {@link ContinuousEnumerationResult#lazyBulkScanCursor()}. Mid-bulk page.
     */
    SET,
    /** Clear the cursor. Final bulk page transitioning to incremental. */
    CLEAR,
  }

  private final Collection<IcebergSourceSplit> splits;
  private final IcebergEnumeratorPosition fromPosition;
  private final IcebergEnumeratorPosition toPosition;
  private final CursorAction cursorAction;

  @Nullable private final LazyBulkScanCursor lazyBulkScanCursor;

  /**
   * Default constructor used by eager planners and tests. Produces a result with {@link
   * CursorAction#NONE} — the checkpointed cursor is left alone.
   *
   * @param splits should never be null. But it can be an empty collection
   * @param fromPosition can be null
   * @param toPosition should never be null. But it can have null snapshotId and snapshotTimestampMs
   */
  ContinuousEnumerationResult(
      Collection<IcebergSourceSplit> splits,
      IcebergEnumeratorPosition fromPosition,
      IcebergEnumeratorPosition toPosition) {
    this(splits, fromPosition, toPosition, CursorAction.NONE, null);
  }

  /**
   * Mid-bulk page from {@link LazyContinuousSplitPlanner}. The enumerator commits the cursor
   * atomically with the assigner update.
   */
  static ContinuousEnumerationResult forLazyBulkPage(
      Collection<IcebergSourceSplit> splits,
      IcebergEnumeratorPosition fromPosition,
      IcebergEnumeratorPosition toPosition,
      LazyBulkScanCursor lazyBulkScanCursor) {
    Preconditions.checkArgument(
        lazyBulkScanCursor != null, "Mid-bulk page must carry a non-null cursor");
    return new ContinuousEnumerationResult(
        splits, fromPosition, toPosition, CursorAction.SET, lazyBulkScanCursor);
  }

  /**
   * Final bulk page from {@link LazyContinuousSplitPlanner}. The enumerator clears the cursor so
   * subsequent checkpoints carry no mid-bulk state.
   */
  static ContinuousEnumerationResult forLazyBulkClear(
      Collection<IcebergSourceSplit> splits,
      IcebergEnumeratorPosition fromPosition,
      IcebergEnumeratorPosition toPosition) {
    return new ContinuousEnumerationResult(
        splits, fromPosition, toPosition, CursorAction.CLEAR, null);
  }

  private ContinuousEnumerationResult(
      Collection<IcebergSourceSplit> splits,
      IcebergEnumeratorPosition fromPosition,
      IcebergEnumeratorPosition toPosition,
      CursorAction cursorAction,
      @Nullable LazyBulkScanCursor lazyBulkScanCursor) {
    Preconditions.checkArgument(splits != null, "Invalid to splits collection: null");
    Preconditions.checkArgument(toPosition != null, "Invalid end position: null");
    this.splits = splits;
    this.fromPosition = fromPosition;
    this.toPosition = toPosition;
    this.cursorAction = cursorAction;
    this.lazyBulkScanCursor = lazyBulkScanCursor;
  }

  public Collection<IcebergSourceSplit> splits() {
    return splits;
  }

  public IcebergEnumeratorPosition fromPosition() {
    return fromPosition;
  }

  public IcebergEnumeratorPosition toPosition() {
    return toPosition;
  }

  public CursorAction cursorAction() {
    return cursorAction;
  }

  /**
   * The cursor to commit when {@link #cursorAction()} is {@link CursorAction#SET}. Always {@code
   * null} for {@link CursorAction#NONE} and {@link CursorAction#CLEAR}.
   */
  @Nullable
  public LazyBulkScanCursor lazyBulkScanCursor() {
    return lazyBulkScanCursor;
  }
}
