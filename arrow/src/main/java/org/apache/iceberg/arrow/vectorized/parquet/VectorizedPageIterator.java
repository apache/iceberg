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
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.parquet.BasePageIterator;
import org.apache.iceberg.parquet.ParquetUtil;
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

  private VectorizedValuesReader valuesReader = null;
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
    this.valuesReader = null;
    this.vectorizedDefinitionLevelReader = null;
  }

  @Override
  protected void initDataReader(Encoding dataEncoding, ByteBufferInputStream in, int valueCount) {
    ValuesReader previousReader = (ValuesReader) valuesReader;
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
      switch (dataEncoding) {
        case PLAIN:
          valuesReader = new VectorizedPlainValuesReader();
          break;
        case DELTA_BINARY_PACKED:
          valuesReader = new VectorizedDeltaEncodedValuesReader();
          break;
        default:
          throw new UnsupportedOperationException(
              "Cannot support vectorized reads for column "
                  + desc
                  + " with "
                  + "encoding "
                  + dataEncoding
                  + ". Disable vectorized reads to read this table/file");
      }
      try {
        valuesReader.initFromPage(valueCount, in);
      } catch (IOException e) {
        throw new ParquetDecodingException(
            "could not read page " + valueCount + " in col " + desc, e);
      }
      dictionaryDecodeMode = DictionaryDecodeMode.NONE;
    }
    if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, dataEncoding)
        && previousReader instanceof RequiresPreviousReader) {
      // previous reader can only be set if reading sequentially
      ((RequiresPreviousReader) valuesReader).setPreviousReader(previousReader);
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
  public void nextBatchDictionaryIds(
      final IntVector vector, NullabilityHolder holder, ParquetReadState readState) {
    vectorizedDefinitionLevelReader
        .dictionaryIdReader()
        .nextDictEncodedBatch(vector, -1, holder, dictionaryEncodedValuesReader, null, readState);
    this.hasNext = readState.getValuesToReadInPage() > 0;
  }

  abstract class BasePageReader {
    public void nextBatch(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
      if (dictionaryDecodeMode == DictionaryDecodeMode.EAGER) {
        nextDictEncodedVal(vector, typeWidth, holder, readState);
      } else {
        nextVal(vector, typeWidth, holder, readState);
      }
      hasNext = readState.getValuesToReadInPage() > 0;
    }

    protected abstract void nextVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState);

    protected abstract void nextDictEncodedVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState);
  }

  /** Method for reading a batch of values of INT32 data type */
  class IntPageReader extends BasePageReader {
    @Override
    protected void nextVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
      vectorizedDefinitionLevelReader
          .integerReader()
          .nextBatch(vector, typeWidth, holder, valuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
      vectorizedDefinitionLevelReader
          .integerReader()
          .nextDictEncodedBatch(
              vector, typeWidth, holder, dictionaryEncodedValuesReader, dictionary, readState);
    }
  }

  /** Method for reading a batch of values of INT64 data type */
  class LongPageReader extends BasePageReader {

    @Override
    protected void nextVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
      vectorizedDefinitionLevelReader
          .longReader()
          .nextBatch(vector, typeWidth, holder, valuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
      vectorizedDefinitionLevelReader
          .longReader()
          .nextDictEncodedBatch(
              vector, typeWidth, holder, dictionaryEncodedValuesReader, dictionary, readState);
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
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
      vectorizedDefinitionLevelReader
          .timestampMillisReader()
          .nextBatch(vector, typeWidth, holder, valuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
      vectorizedDefinitionLevelReader
          .timestampMillisReader()
          .nextDictEncodedBatch(
              vector, typeWidth, holder, dictionaryEncodedValuesReader, dictionary, readState);
    }
  }

  /** Method for reading a batch of values of TimestampInt96 data type. */
  class TimestampInt96PageReader extends BasePageReader {
    @Override
    protected void nextVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
      vectorizedDefinitionLevelReader
          .timestampInt96Reader()
          .nextBatch(vector, typeWidth, holder, valuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
      vectorizedDefinitionLevelReader
          .timestampInt96Reader()
          .nextDictEncodedBatch(
              vector, typeWidth, holder, dictionaryEncodedValuesReader, dictionary, readState);
    }
  }

  /** Method for reading a batch of values of FLOAT data type. */
  class FloatPageReader extends BasePageReader {

    @Override
    protected void nextVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
      vectorizedDefinitionLevelReader
          .floatReader()
          .nextBatch(vector, typeWidth, holder, valuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
      vectorizedDefinitionLevelReader
          .floatReader()
          .nextDictEncodedBatch(
              vector, typeWidth, holder, dictionaryEncodedValuesReader, dictionary, readState);
    }
  }

  /** Method for reading a batch of values of DOUBLE data type */
  class DoublePageReader extends BasePageReader {

    @Override
    protected void nextVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
      vectorizedDefinitionLevelReader
          .doubleReader()
          .nextBatch(vector, typeWidth, holder, valuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
      vectorizedDefinitionLevelReader
          .doubleReader()
          .nextDictEncodedBatch(
              vector, typeWidth, holder, dictionaryEncodedValuesReader, dictionary, readState);
    }
  }

  class FixedSizeBinaryPageReader extends BasePageReader {
    @Override
    protected void nextVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
      vectorizedDefinitionLevelReader
          .fixedSizeBinaryReader()
          .nextBatch(vector, typeWidth, holder, valuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
      vectorizedDefinitionLevelReader
          .fixedSizeBinaryReader()
          .nextDictEncodedBatch(
              vector, typeWidth, holder, dictionaryEncodedValuesReader, dictionary, readState);
    }
  }

  /** Method for reading a batch of variable width data type (ENUM, JSON, UTF8, BSON). */
  class VarWidthTypePageReader extends BasePageReader {
    @Override
    protected void nextVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
      vectorizedDefinitionLevelReader
          .varWidthReader()
          .nextBatch(vector, typeWidth, holder, valuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
      vectorizedDefinitionLevelReader
          .varWidthReader()
          .nextDictEncodedBatch(
              vector, typeWidth, holder, dictionaryEncodedValuesReader, dictionary, readState);
    }
  }

  /** Method for reading batches of booleans. */
  class BooleanPageReader extends BasePageReader {
    @Override
    protected void nextVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
      vectorizedDefinitionLevelReader
          .booleanReader()
          .nextBatch(vector, typeWidth, holder, valuesReader, readState);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int typeWidth, NullabilityHolder holder, ParquetReadState readState) {
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

  BooleanPageReader booleanPageReader() {
    return new BooleanPageReader();
  }
}
