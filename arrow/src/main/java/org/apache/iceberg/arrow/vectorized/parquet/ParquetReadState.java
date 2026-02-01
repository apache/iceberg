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
package org.apache.iceberg.arrow.vectorized.parquet;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class ParquetReadState {
  /** A special row range used when there is no row indexes (hence all rows must be included) */
  private static final RowRange MAX_ROW_RANGE = new RowRange(Long.MIN_VALUE, Long.MAX_VALUE);

  /**
   * A special row range used when the row indexes are present AND all the row ranges have been
   * processed. This serves as a sentinel at the end indicating that all rows come after the last
   * row range should be skipped.
   */
  private static final RowRange END_ROW_RANGE = new RowRange(Long.MAX_VALUE, Long.MIN_VALUE);

  /**
   * The current index over all rows within the column chunk. This is used to check if the current
   * row should be skipped by comparing against the row ranges.
   */
  private long currentRowIndex;

  /** The offset in the current batch to put the next value in value vector */
  private int valueOffset;

  /** The remaining number of values to read in the current page */
  private int valuesToReadInPage;

  /** Iterator over all row ranges, only not-null if column index is present */
  private final Iterator<RowRange> rowRanges;

  /** The current row range */
  private RowRange currentRange;

  /** The remaining number of rows to read in the current batch */
  private int rowsToReadInBatch;

  /** The actual number of rows read in this batch on the current page, including skipped rows */
  private int rowsWithSkipsInThisBatch;

  /**
   * Mapping from read order to actual position in row group. rowIndexes: [0, 1, 2, 4, 5, 7, 8, 9]
   * -> rowRanges: [0-2], [4-5], [7-9] readOrderToRowGroupPosMap: [0 -> 0, 3 -> 4, 5 -> 7]
   */
  private final Map<Long, Long> readOrderToRowGroupPosMap = Maps.newHashMap();

  public ParquetReadState(PrimitiveIterator.OfLong rowIndexes) {
    this.rowRanges = constructRanges(rowIndexes);
    nextRange();
  }

  /** Advance to the next range. */
  void nextRange() {
    if (rowRanges == null) {
      currentRange = MAX_ROW_RANGE;
    } else if (!rowRanges.hasNext()) {
      currentRange = END_ROW_RANGE;
    } else {
      currentRange = rowRanges.next();
    }
  }

  public long currentRangeStart() {
    return currentRange.getStart();
  }

  public long currentRangeEnd() {
    return currentRange.getEnd();
  }

  /** Must be called at the beginning of reading a new page. */
  void resetForNewPage(int totalValuesInPage, long pageFirstRowIndex) {
    this.valuesToReadInPage = totalValuesInPage;
    this.currentRowIndex = pageFirstRowIndex;
  }

  /** Must be called at the beginning of reading a new batch. */
  void resetForNewBatch(int batchSize) {
    this.valueOffset = 0;
    this.rowsToReadInBatch = batchSize;
  }

  /**
   * Construct a list of row ranges from the given `rowIndexes`. For example, suppose the
   * `rowIndexes` are `[0, 1, 2, 4, 5, 7, 8, 9]`, it will be converted into 3 row ranges: `[0-2],
   * [4-5], [7-9]`.
   */
  private Iterator<RowRange> constructRanges(PrimitiveIterator.OfLong rowIndexes) {
    if (rowIndexes == null) {
      return null;
    }

    List<RowRange> ranges = Lists.newArrayList();
    long currentStart = Long.MIN_VALUE;
    long previous = Long.MIN_VALUE;
    long readOrder = 0;

    while (rowIndexes.hasNext()) {
      long idx = rowIndexes.nextLong();
      if (currentStart == Long.MIN_VALUE) {
        currentStart = idx;
      } else if (previous + 1 != idx) {
        RowRange range = new RowRange(currentStart, previous);
        readOrderToRowGroupPosMap.put(readOrder, currentStart);
        readOrder += previous - currentStart + 1;
        ranges.add(range);
        currentStart = idx;
      }
      previous = idx;
    }

    if (previous != Long.MIN_VALUE) {
      ranges.add(new RowRange(currentStart, previous));
      readOrderToRowGroupPosMap.put(readOrder, currentStart);
    }

    return ranges.iterator();
  }

  int getValuesToReadInPage() {
    return valuesToReadInPage;
  }

  void setValuesToReadInPage(int valuesToReadInPage) {
    this.valuesToReadInPage = valuesToReadInPage;
  }

  long getCurrentRowIndex() {
    return currentRowIndex;
  }

  void setCurrentRowIndex(long currentRowIndex) {
    this.currentRowIndex = currentRowIndex;
  }

  int getRowsToReadInBatch() {
    return rowsToReadInBatch;
  }

  public void setRowsToReadInBatch(int rowsToReadInBatch) {
    this.rowsToReadInBatch = rowsToReadInBatch;
  }

  public int getRowsWithSkipsInThisBatch() {
    return rowsWithSkipsInThisBatch;
  }

  public void setRowsWithSkipsInThisBatch(int rowsWithSkipsInThisBatch) {
    this.rowsWithSkipsInThisBatch = rowsWithSkipsInThisBatch;
  }

  public int getValueOffset() {
    return valueOffset;
  }

  public void setValueOffset(int valueOffset) {
    this.valueOffset = valueOffset;
  }

  public Map<Long, Long> getReadOrderToRowGroupPosMap() {
    return readOrderToRowGroupPosMap;
  }

  /** Helper struct to represent a range of row indexes `[start, end]`. */
  private static class RowRange {
    private final long start;
    private final long end;

    RowRange(long start, long end) {
      this.start = start;
      this.end = end;
    }

    public long getStart() {
      return start;
    }

    public long getEnd() {
      return end;
    }
  }
}
