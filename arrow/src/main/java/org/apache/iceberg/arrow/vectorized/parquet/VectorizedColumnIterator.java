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
import java.util.Optional;
import java.util.PrimitiveIterator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.parquet.BaseColumnIterator;
import org.apache.iceberg.parquet.BasePageIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReader;

/**
 * Vectorized version of the ColumnIterator that reads column values in data pages of a column in a
 * row group in a batched fashion.
 */
public class VectorizedColumnIterator extends BaseColumnIterator {

  /** A special row range used when there is no row indexes (hence all rows must be included) */
  private static final RowRange MAX_ROW_RANGE = new RowRange(Long.MIN_VALUE, Long.MAX_VALUE);

  /**
   * A special row range used when the row indexes are present AND all the row ranges have been
   * processed. This serves as a sentinel at the end indicating that all rows come after the last
   * row range should be skipped.
   */
  private static final RowRange END_ROW_RANGE = new RowRange(Long.MAX_VALUE, Long.MIN_VALUE);

  private final VectorizedPageIterator vectorizedPageIterator;
  private int batchSize;
  private int currentOffset;
  private long pageFirstRowIndex;
  private Iterator<RowRange> rowRanges;
  private RowRange currentRange;

  public VectorizedColumnIterator(
      ColumnDescriptor desc, String writerVersion, boolean setArrowValidityVector) {
    super(desc);
    Preconditions.checkArgument(
        desc.getMaxRepetitionLevel() == 0,
        "Only non-nested columns are supported for vectorized reads");
    this.vectorizedPageIterator =
        new VectorizedPageIterator(desc, writerVersion, setArrowValidityVector);
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public void newBatch() {
    this.currentOffset = 0;
  }

  public Dictionary setRowGroupInfo(
      PageReader store,
      Optional<PrimitiveIterator.OfLong> optionalRowIndexes,
      boolean allPagesDictEncoded) {
    // setPageSource can result in a data page read. If that happens, we need
    // to know in advance whether all the pages in the row group are dictionary encoded or not
    this.vectorizedPageIterator.setAllPagesDictEncoded(allPagesDictEncoded);
    super.setPageSource(store, optionalRowIndexes);
    this.rowRanges = constructRanges(rowIndexes);
    nextRange();
    this.currentOffset = 0;
    return dictionary;
  }

  @Override
  protected BasePageIterator pageIterator() {
    return vectorizedPageIterator;
  }

  @Override
  protected void advance() {
    if (triplesRead >= advanceNextPageCount) {
      BasePageIterator pageIterator = pageIterator();
      while (!pageIterator.hasNext()) {
        DataPage page = pageSource.readPage();
        if (page != null) {
          pageIterator.setPage(page);
          this.pageFirstRowIndex = page.getFirstRowIndex().orElse(0L);
          this.advanceNextPageCount += pageIterator.currentPageCount();
          this.numValuesToSkip = 0;
          this.currentRowIndex = this.pageFirstRowIndex;
        } else {
          return;
        }
      }
    }
  }

  public boolean producesDictionaryEncodedVector() {
    return vectorizedPageIterator.producesDictionaryEncodedVector();
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

    while (rowIndexes.hasNext()) {
      long idx = rowIndexes.nextLong();
      if (currentStart == Long.MIN_VALUE) {
        currentStart = idx;
      } else if (previous + 1 != idx) {
        RowRange range = new RowRange(currentStart, previous);
        ranges.add(range);
        currentStart = idx;
      }
      previous = idx;
    }

    if (previous != Long.MIN_VALUE) {
      ranges.add(new RowRange(currentStart, previous));
    }

    return ranges.iterator();
  }

  /** Advance to the next range. */
  private void nextRange() {
    if (rowRanges == null) {
      currentRange = MAX_ROW_RANGE;
    } else if (!rowRanges.hasNext()) {
      currentRange = END_ROW_RANGE;
    } else {
      currentRange = rowRanges.next();
    }
  }

  /** Helper struct to represent a range of row indexes `[start, end]`. */
  private static class RowRange {
    private final long start;
    private final long end;

    RowRange(long start, long end) {
      this.start = start;
      this.end = end;
    }

    long start() {
      return start;
    }

    long end() {
      return end;
    }
  }

  public class ReadState {
    public int currentOffset() {
      return currentOffset;
    }

    public long currentRowIndex() {
      return currentRowIndex;
    }

    public long currentRangeStart() {
      return currentRange.start();
    }

    public long currentRangeEnd() {
      return currentRange.end();
    }

    public void nextRange() {
      VectorizedColumnIterator.this.nextRange();
    }

    public void advanceOffsetAndRowIndex(int offset, long rowIndex) {
      currentOffset = offset;
      currentRowIndex = rowIndex;
    }
  }

  public abstract class BatchReader {
    public void nextBatch(FieldVector fieldVector, int typeWidth, NullabilityHolder holder) {
      int rowsReadSoFar = 0;
      while (rowsReadSoFar < batchSize && hasNext()) {
        advance();
        int rowsInThisBatch =
            nextBatchOf(
                fieldVector,
                batchSize - rowsReadSoFar,
                rowsReadSoFar,
                typeWidth,
                holder,
                new ReadState());
        rowsReadSoFar += rowsInThisBatch;
        triplesRead += rowsInThisBatch;
        fieldVector.setValueCount(currentOffset);
      }
    }

    protected abstract int nextBatchOf(
        FieldVector vector,
        int expectedBatchSize,
        int numValsInVector,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState);
  }

  public class IntegerBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      return vectorizedPageIterator
          .intPageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder, readState);
    }
  }

  public class DictionaryBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      return vectorizedPageIterator.nextBatchDictionaryIds(
          (IntVector) vector, expectedBatchSize, numValsInVector, holder, readState);
    }
  }

  public class LongBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      return vectorizedPageIterator
          .longPageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder, readState);
    }
  }

  public class TimestampMillisBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      return vectorizedPageIterator
          .timestampMillisPageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder, readState);
    }
  }

  public class TimestampInt96BatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      return vectorizedPageIterator
          .timestampInt96PageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder, readState);
    }
  }

  public class FloatBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      return vectorizedPageIterator
          .floatPageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder, readState);
    }
  }

  public class DoubleBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      return vectorizedPageIterator
          .doublePageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder, readState);
    }
  }

  public class FixedSizeBinaryBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      return vectorizedPageIterator
          .fixedSizeBinaryPageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder, readState);
    }
  }

  public class VarWidthTypeBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      return vectorizedPageIterator
          .varWidthTypePageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder, readState);
    }
  }

  public class FixedWidthTypeBinaryBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      return vectorizedPageIterator
          .fixedWidthBinaryPageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder, readState);
    }
  }

  public class BooleanBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      return vectorizedPageIterator
          .booleanPageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder, readState);
    }
  }

  public IntegerBatchReader integerBatchReader() {
    return new IntegerBatchReader();
  }

  public DictionaryBatchReader dictionaryBatchReader() {
    return new DictionaryBatchReader();
  }

  public LongBatchReader longBatchReader() {
    return new LongBatchReader();
  }

  public TimestampMillisBatchReader timestampMillisBatchReader() {
    return new TimestampMillisBatchReader();
  }

  public TimestampInt96BatchReader timestampInt96BatchReader() {
    return new TimestampInt96BatchReader();
  }

  public FloatBatchReader floatBatchReader() {
    return new FloatBatchReader();
  }

  public DoubleBatchReader doubleBatchReader() {
    return new DoubleBatchReader();
  }

  public FixedSizeBinaryBatchReader fixedSizeBinaryBatchReader() {
    return new FixedSizeBinaryBatchReader();
  }

  public VarWidthTypeBatchReader varWidthTypeBatchReader() {
    return new VarWidthTypeBatchReader();
  }

  public FixedWidthTypeBinaryBatchReader fixedWidthTypeBinaryBatchReader() {
    return new FixedWidthTypeBinaryBatchReader();
  }

  public BooleanBatchReader booleanBatchReader() {
    return new BooleanBatchReader();
  }
}
