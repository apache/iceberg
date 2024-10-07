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

import java.io.IOException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.arrow.vectorized.parquet.VectorizedColumnIterator.ReadState;
import org.apache.iceberg.parquet.BasePageIterator;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.parquet.ValuesAsBytesReader;
import org.apache.parquet.CorruptDeltaByteArrays;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.values.RequiresPreviousReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;

public class VectorizedPageIterator extends BasePageIterator {
  private final boolean setArrowValidityVector;

  public VectorizedPageIterator(
      ColumnDescriptor desc, String writerVersion, boolean setValidityVector) {
    super(desc, writerVersion);
    this.setArrowValidityVector = setValidityVector;
  }

  private ValuesAsBytesReader plainValuesReader = null;
  private VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader = null;
  private boolean allPagesDictEncoded;
  private VectorizedParquetDefinitionLevelReader vectorizedDefinitionLevelReader;

  private enum DictionaryDecodeMode {
    NONE, // plain encoding
    LAZY,
    EAGER
  }

  private DictionaryDecodeMode dictionaryDecodeMode;

  public void setAllPagesDictEncoded(boolean allDictEncoded) {
    this.allPagesDictEncoded = allDictEncoded;
  }

  @Override
  protected void reset() {
    super.reset();
    this.plainValuesReader = null;
    this.vectorizedDefinitionLevelReader = null;
  }

  @Override
  protected void initDataReader(Encoding dataEncoding, ByteBufferInputStream in, int valueCount) {
    ValuesReader previousReader = plainValuesReader;
    if (dataEncoding.usesDictionary()) {
      if (dictionary == null) {
        throw new ParquetDecodingException(
            "could not read page in col "
                + desc
                + " as the dictionary was missing for encoding "
                + dataEncoding);
      }
      try {
        dictionaryEncodedValuesReader =
            new VectorizedDictionaryEncodedParquetValuesReader(
                desc.getMaxDefinitionLevel(), setArrowValidityVector);
        dictionaryEncodedValuesReader.initFromPage(valueCount, in);
        if (ParquetUtil.isIntType(desc.getPrimitiveType()) || !allPagesDictEncoded) {
          dictionaryDecodeMode = DictionaryDecodeMode.EAGER;
        } else {
          dictionaryDecodeMode = DictionaryDecodeMode.LAZY;
        }
      } catch (IOException e) {
        throw new ParquetDecodingException("could not read page in col " + desc, e);
      }
    } else {
      if (dataEncoding != Encoding.PLAIN) {
        throw new UnsupportedOperationException(
            "Cannot support vectorized reads for column "
                + desc
                + " with "
                + "encoding "
                + dataEncoding
                + ". Disable vectorized reads to read this table/file");
      }
      plainValuesReader = new ValuesAsBytesReader();
      plainValuesReader.initFromPage(valueCount, in);
      dictionaryDecodeMode = DictionaryDecodeMode.NONE;
    }
    if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, dataEncoding)
        && previousReader instanceof RequiresPreviousReader) {
      // previous reader can only be set if reading sequentially
      ((RequiresPreviousReader) plainValuesReader).setPreviousReader(previousReader);
    }
  }

  public boolean producesDictionaryEncodedVector() {
    return dictionaryDecodeMode == DictionaryDecodeMode.LAZY;
  }

  @Override
  protected void initDefinitionLevelsReader(
      DataPageV1 dataPageV1, ColumnDescriptor desc, ByteBufferInputStream in, int triplesCount)
      throws IOException {
    int bitWidth = BytesUtils.getWidthFromMaxInt(desc.getMaxDefinitionLevel());
    this.vectorizedDefinitionLevelReader =
        new VectorizedParquetDefinitionLevelReader(
            bitWidth, desc.getMaxDefinitionLevel(), setArrowValidityVector);
    this.vectorizedDefinitionLevelReader.initFromPage(triplesCount, in);
  }

  @Override
  protected void initDefinitionLevelsReader(DataPageV2 dataPageV2, ColumnDescriptor desc)
      throws IOException {
    int bitWidth = BytesUtils.getWidthFromMaxInt(desc.getMaxDefinitionLevel());
    // do not read the length from the stream. v2 pages handle dividing the page bytes.
    this.vectorizedDefinitionLevelReader =
        new VectorizedParquetDefinitionLevelReader(
            bitWidth, desc.getMaxDefinitionLevel(), false, setArrowValidityVector);
    this.vectorizedDefinitionLevelReader.initFromPage(
        dataPageV2.getValueCount(), dataPageV2.getDefinitionLevels().toInputStream());
  }

  /**
   * Method for reading a batch of dictionary ids from the dictionary encoded data pages. Like
   * definition levels, dictionary ids in Parquet are RLE/bin-packed encoded as well.
   */
  public int nextBatchDictionaryIds(
      final IntVector vector,
      final int expectedBatchSize,
      NullabilityHolder holder,
      ReadState readState) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    vectorizedDefinitionLevelReader
        .dictionaryIdReader()
        .nextDictEncodedBatch(
            vector, -1, actualBatchSize, holder, dictionaryEncodedValuesReader, null, readState);
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  abstract class BasePageReader {
    public int nextBatch(
        FieldVector vector,
        int expectedBatchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      final int actualBatchSize = getActualBatchSize(expectedBatchSize);
      if (actualBatchSize <= 0) {
        return 0;
      }
      if (dictionaryDecodeMode == DictionaryDecodeMode.EAGER) {
        nextDictEncodedVal(vector, actualBatchSize, typeWidth, holder, readState);
      } else {
        nextVal(vector, actualBatchSize, typeWidth, holder, readState);
      }
      triplesRead += actualBatchSize;
      hasNext = triplesRead < triplesCount;
      return actualBatchSize;
    }

    protected abstract void nextVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState);

    protected abstract void nextDictEncodedVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState);
  }

  /** Method for reading a batch of values of INT32 data type */
  class IntPageReader extends BasePageReader {
    @Override
    protected void nextVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .integerReader()
          .nextBatch(vector, typeWidth, batchSize, holder, plainValuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .integerReader()
          .nextDictEncodedBatch(
              vector,
              typeWidth,
              batchSize,
              holder,
              dictionaryEncodedValuesReader,
              dictionary,
              readState);
    }
  }

  /** Method for reading a batch of values of INT64 data type */
  class LongPageReader extends BasePageReader {

    @Override
    protected void nextVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .longReader()
          .nextBatch(vector, typeWidth, batchSize, holder, plainValuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .longReader()
          .nextDictEncodedBatch(
              vector,
              typeWidth,
              batchSize,
              holder,
              dictionaryEncodedValuesReader,
              dictionary,
              readState);
    }
  }

  /**
   * Method for reading a batch of values of TIMESTAMP_MILLIS data type. In iceberg, TIMESTAMP is
   * always represented in micro-seconds. So we multiply values stored in millis with 1000 before
   * writing them to the vector.
   */
  class TimestampMillisPageReader extends BasePageReader {

    @Override
    protected void nextVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .timestampMillisReader()
          .nextBatch(vector, typeWidth, batchSize, holder, plainValuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .timestampMillisReader()
          .nextDictEncodedBatch(
              vector,
              typeWidth,
              batchSize,
              holder,
              dictionaryEncodedValuesReader,
              dictionary,
              readState);
    }
  }

  /** Method for reading a batch of values of TimestampInt96 data type. */
  class TimestampInt96PageReader extends BasePageReader {
    @Override
    protected void nextVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .timestampInt96Reader()
          .nextBatch(vector, typeWidth, batchSize, holder, plainValuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .timestampInt96Reader()
          .nextDictEncodedBatch(
              vector,
              typeWidth,
              batchSize,
              holder,
              dictionaryEncodedValuesReader,
              dictionary,
              readState);
    }
  }

  /** Method for reading a batch of values of FLOAT data type. */
  class FloatPageReader extends BasePageReader {

    @Override
    protected void nextVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .floatReader()
          .nextBatch(vector, typeWidth, batchSize, holder, plainValuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .floatReader()
          .nextDictEncodedBatch(
              vector,
              typeWidth,
              batchSize,
              holder,
              dictionaryEncodedValuesReader,
              dictionary,
              readState);
    }
  }

  /** Method for reading a batch of values of DOUBLE data type */
  class DoublePageReader extends BasePageReader {

    @Override
    protected void nextVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .doubleReader()
          .nextBatch(vector, typeWidth, batchSize, holder, plainValuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .doubleReader()
          .nextDictEncodedBatch(
              vector,
              typeWidth,
              batchSize,
              holder,
              dictionaryEncodedValuesReader,
              dictionary,
              readState);
    }
  }

  private int getActualBatchSize(int expectedBatchSize) {
    return Math.min(expectedBatchSize, triplesCount - triplesRead);
  }

  class FixedSizeBinaryPageReader extends BasePageReader {
    @Override
    protected void nextVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .fixedSizeBinaryReader()
          .nextBatch(vector, typeWidth, batchSize, holder, plainValuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .fixedSizeBinaryReader()
          .nextDictEncodedBatch(
              vector,
              typeWidth,
              batchSize,
              holder,
              dictionaryEncodedValuesReader,
              dictionary,
              readState);
    }
  }

  /** Method for reading a batch of variable width data type (ENUM, JSON, UTF8, BSON). */
  class VarWidthTypePageReader extends BasePageReader {
    @Override
    protected void nextVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .varWidthReader()
          .nextBatch(vector, typeWidth, batchSize, holder, plainValuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .varWidthReader()
          .nextDictEncodedBatch(
              vector,
              typeWidth,
              batchSize,
              holder,
              dictionaryEncodedValuesReader,
              dictionary,
              readState);
    }
  }

  /**
   * Method for reading batches of fixed width binary type (e.g. BYTE[7]). Spark does not support
   * fixed width binary data type. To work around this limitation, the data is read as fixed width
   * binary from parquet and stored in a {@link VarBinaryVector} in Arrow.
   */
  class FixedWidthBinaryPageReader extends BasePageReader {
    @Override
    protected void nextVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .fixedWidthBinaryReader()
          .nextBatch(vector, typeWidth, batchSize, holder, plainValuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .fixedWidthBinaryReader()
          .nextDictEncodedBatch(
              vector,
              typeWidth,
              batchSize,
              holder,
              dictionaryEncodedValuesReader,
              dictionary,
              readState);
    }
  }

  /** Method for reading batches of booleans. */
  class BooleanPageReader extends BasePageReader {
    @Override
    protected void nextVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      vectorizedDefinitionLevelReader
          .booleanReader()
          .nextBatch(vector, typeWidth, batchSize, holder, plainValuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int batchSize,
        int typeWidth,
        NullabilityHolder holder,
        ReadState readState) {
      throw new UnsupportedOperationException();
    }
  }

  IntPageReader intPageReader() {
    return new IntPageReader();
  }

  LongPageReader longPageReader() {
    return new LongPageReader();
  }

  TimestampMillisPageReader timestampMillisPageReader() {
    return new TimestampMillisPageReader();
  }

  TimestampInt96PageReader timestampInt96PageReader() {
    return new TimestampInt96PageReader();
  }

  FloatPageReader floatPageReader() {
    return new FloatPageReader();
  }

  DoublePageReader doublePageReader() {
    return new DoublePageReader();
  }

  FixedSizeBinaryPageReader fixedSizeBinaryPageReader() {
    return new FixedSizeBinaryPageReader();
  }

  VarWidthTypePageReader varWidthTypePageReader() {
    return new VarWidthTypePageReader();
  }

  FixedWidthBinaryPageReader fixedWidthBinaryPageReader() {
    return new FixedWidthBinaryPageReader();
  }

  BooleanPageReader booleanPageReader() {
    return new BooleanPageReader();
  }
}
