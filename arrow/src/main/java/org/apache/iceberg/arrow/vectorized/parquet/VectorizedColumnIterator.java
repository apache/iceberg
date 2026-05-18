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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.parquet.BaseColumnIterator;
import org.apache.iceberg.parquet.BasePageIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReader;

/**
 * Vectorized version of the ColumnIterator that reads column values in data pages of a column in a
 * row group in a batched fashion.
 */
public class VectorizedColumnIterator extends BaseColumnIterator {

  private final VectorizedPageIterator vectorizedPageIterator;
  private int batchSize;
  private ParquetReadState readState;

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

  public Dictionary setRowGroupInfo(
      PageReader store, boolean allPagesDictEncoded, ParquetReadState state) {
    // setPageSource can result in a data page read. If that happens, we need
    // to know in advance whether all the pages in the row group are dictionary encoded or not
    this.vectorizedPageIterator.setAllPagesDictEncoded(allPagesDictEncoded);
    this.readState = state;
    super.setPageSource(store);
    return dictionary;
  }

  @Override
  protected BasePageIterator pageIterator() {
    return vectorizedPageIterator;
  }

  @Override
  protected void advance() {
    if (readState.getValuesToReadInPage() == 0) {
      BasePageIterator pageIterator = pageIterator();
      while (!pageIterator.hasNext()) {
        DataPage page = pageSource.readPage();
        if (page != null) {
          pageIterator.setPage(page);
          readState.resetForNewPage(page.getValueCount(), page.getFirstRowIndex().orElse(0L));
          this.advanceNextPageCount += pageIterator.currentPageCount();
        } else {
          return;
        }
      }
    }
  }

  public boolean producesDictionaryEncodedVector() {
    return vectorizedPageIterator.producesDictionaryEncodedVector();
  }

  public abstract class BatchReader {
    public void nextBatch(FieldVector fieldVector, int typeWidth, NullabilityHolder holder) {
      readState.resetForNewBatch(batchSize);
      while (readState.getRowsToReadInBatch() > 0 && hasNext()) {
        advance();
        readState.setRowsWithSkipsInThisBatch(0);
        nextBatchOf(fieldVector, typeWidth, holder, readState);
        triplesRead += readState.getRowsWithSkipsInThisBatch();
        fieldVector.setValueCount(batchSize - readState.getRowsToReadInBatch());
      }
    }

    protected abstract void nextBatchOf(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState state);
  }

  public class IntegerBatchReader extends BatchReader {
    @Override
    protected void nextBatchOf(
        final FieldVector vector,
        final int typeWidth,
        NullabilityHolder holder,
        ParquetReadState state) {
      vectorizedPageIterator.intPageReader().nextBatch(vector, typeWidth, holder, state);
    }
  }

  public class DictionaryBatchReader extends BatchReader {
    @Override
    protected void nextBatchOf(
        final FieldVector vector,
        final int typeWidth,
        NullabilityHolder holder,
        ParquetReadState state) {
      vectorizedPageIterator.nextBatchDictionaryIds((IntVector) vector, holder, state);
    }
  }

  public class LongBatchReader extends BatchReader {
    @Override
    protected void nextBatchOf(
        final FieldVector vector,
        final int typeWidth,
        NullabilityHolder holder,
        ParquetReadState state) {
      vectorizedPageIterator.longPageReader().nextBatch(vector, typeWidth, holder, state);
    }
  }

  public class TimestampMillisBatchReader extends BatchReader {
    @Override
    protected void nextBatchOf(
        final FieldVector vector,
        final int typeWidth,
        NullabilityHolder holder,
        ParquetReadState state) {
      vectorizedPageIterator
          .timestampMillisPageReader()
          .nextBatch(vector, typeWidth, holder, state);
    }
  }

  public class TimestampInt96BatchReader extends BatchReader {
    @Override
    protected void nextBatchOf(
        final FieldVector vector,
        final int typeWidth,
        NullabilityHolder holder,
        ParquetReadState state) {
      vectorizedPageIterator.timestampInt96PageReader().nextBatch(vector, typeWidth, holder, state);
    }
  }

  public class FloatBatchReader extends BatchReader {
    @Override
    protected void nextBatchOf(
        final FieldVector vector,
        final int typeWidth,
        NullabilityHolder holder,
        ParquetReadState state) {
      vectorizedPageIterator.floatPageReader().nextBatch(vector, typeWidth, holder, state);
    }
  }

  public class DoubleBatchReader extends BatchReader {
    @Override
    protected void nextBatchOf(
        final FieldVector vector,
        final int typeWidth,
        NullabilityHolder holder,
        ParquetReadState state) {
      vectorizedPageIterator.doublePageReader().nextBatch(vector, typeWidth, holder, state);
    }
  }

  public class FixedSizeBinaryBatchReader extends BatchReader {
    @Override
    protected void nextBatchOf(
        final FieldVector vector,
        final int typeWidth,
        NullabilityHolder holder,
        ParquetReadState state) {
      vectorizedPageIterator
          .fixedSizeBinaryPageReader()
          .nextBatch(vector, typeWidth, holder, state);
    }
  }

  public class VarWidthTypeBatchReader extends BatchReader {
    @Override
    protected void nextBatchOf(
        final FieldVector vector,
        final int typeWidth,
        NullabilityHolder holder,
        ParquetReadState state) {
      vectorizedPageIterator.varWidthTypePageReader().nextBatch(vector, typeWidth, holder, state);
    }
  }

  public class BooleanBatchReader extends BatchReader {
    @Override
    protected void nextBatchOf(
        final FieldVector vector,
        final int typeWidth,
        NullabilityHolder holder,
        ParquetReadState state) {
      vectorizedPageIterator.booleanPageReader().nextBatch(vector, typeWidth, holder, state);
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

  public BooleanBatchReader booleanBatchReader() {
    return new BooleanBatchReader();
  }
}
