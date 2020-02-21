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
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.parquet.BasePageIterator;
import org.apache.iceberg.parquet.ValuesAsBytesReader;
import org.apache.parquet.CorruptDeltaByteArrays;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.RequiresPreviousReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;

public class VectorizedPageIterator extends BasePageIterator {
  private final boolean setArrowValidityVector;

  public VectorizedPageIterator(ColumnDescriptor desc, String writerVersion, boolean setValidityVector) {
    super(desc, writerVersion);
    this.setArrowValidityVector = setValidityVector;
  }

  private boolean eagerDecodeDictionary;
  private ValuesAsBytesReader plainValuesReader = null;
  private VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader = null;
  private boolean allPagesDictEncoded;

  // Dictionary is set per row group
  public void setDictionaryForColumn(Dictionary dict, boolean allDictEncoded) {
    this.dictionary = dict;
    this.allPagesDictEncoded = allDictEncoded;
  }

  @Override
  protected void reset() {
    this.page = null;
    this.triplesCount = 0;
    this.triplesRead = 0;
    this.repetitionLevels = null;
    this.plainValuesReader = null;
    this.vectorizedDefinitionLevelReader = null;
    this.hasNext = false;
  }

  public int currentPageCount() {
    return triplesCount;
  }

  public boolean hasNext() {
    return hasNext;
  }

  /**
   * Method for reading a batch of dictionary ids from the dicitonary encoded data pages. Like definition levels,
   * dictionary ids in Parquet are RLE/bin-packed encoded as well.
   */
  public int nextBatchDictionaryIds(
      final IntVector vector, final int expectedBatchSize,
      final int numValsInVector,
      NullabilityHolder holder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    ((VectorizedParquetValuesReader) vectorizedDefinitionLevelReader).readBatchOfDictionaryIds(
        vector,
        numValsInVector,
        actualBatchSize,
        holder,
        dictionaryEncodedValuesReader);
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  /**
   * Method for reading a batch of values of INT32 data type
   */
  public int nextBatchIntegers(
      final FieldVector vector, final int expectedBatchSize,
      final int numValsInVector,
      final int typeWidth, NullabilityHolder holder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (eagerDecodeDictionary) {
      ((VectorizedParquetValuesReader) vectorizedDefinitionLevelReader).readBatchOfDictionaryEncodedIntegers(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      ((VectorizedParquetValuesReader) vectorizedDefinitionLevelReader).readBatchOfIntegers(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  /**
   * Method for reading a batch of values of INT64 data type
   */
  public int nextBatchLongs(
      final FieldVector vector, final int expectedBatchSize,
      final int numValsInVector,
      final int typeWidth, NullabilityHolder holder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (eagerDecodeDictionary) {
      ((VectorizedParquetValuesReader) vectorizedDefinitionLevelReader).readBatchOfDictionaryEncodedLongs(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      ((VectorizedParquetValuesReader) vectorizedDefinitionLevelReader).readBatchOfLongs(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  /**
   * Method for reading a batch of values of FLOAT data type.
   */
  public int nextBatchFloats(
      final FieldVector vector, final int expectedBatchSize,
      final int numValsInVector,
      final int typeWidth, NullabilityHolder holder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (eagerDecodeDictionary) {
      ((VectorizedParquetValuesReader) vectorizedDefinitionLevelReader).readBatchOfDictionaryEncodedFloats(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      ((VectorizedParquetValuesReader) vectorizedDefinitionLevelReader).readBatchOfFloats(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  /**
   * Method for reading a batch of values of DOUBLE data type
   */
  public int nextBatchDoubles(
      final FieldVector vector, final int expectedBatchSize,
      final int numValsInVector,
      final int typeWidth, NullabilityHolder holder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (eagerDecodeDictionary) {
      ((VectorizedParquetValuesReader) vectorizedDefinitionLevelReader).readBatchOfDictionaryEncodedDoubles(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      ((VectorizedParquetValuesReader) vectorizedDefinitionLevelReader).readBatchOfDoubles(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  private int getActualBatchSize(int expectedBatchSize) {
    return Math.min(expectedBatchSize, triplesCount - triplesRead);
  }

  /**
   * Method for reading a batch of decimals backed by INT32 and INT64 parquet data types. Since Arrow stores all
   * decimals in 16 bytes, byte arrays are appropriately padded before being written to Arrow data buffers.
   */
  public int nextBatchIntLongBackedDecimal(
      final FieldVector vector, final int expectedBatchSize, final int numValsInVector,
      final int typeWidth, NullabilityHolder nullabilityHolder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (eagerDecodeDictionary) {
      ((VectorizedParquetValuesReader) vectorizedDefinitionLevelReader)
          .readBatchOfDictionaryEncodedIntLongBackedDecimals(
              vector,
              numValsInVector,
              typeWidth,
              actualBatchSize,
              nullabilityHolder,
              dictionaryEncodedValuesReader,
              dictionary);
    } else {
      ((VectorizedParquetValuesReader) vectorizedDefinitionLevelReader).readBatchOfIntLongBackedDecimals(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          nullabilityHolder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  /**
   * Method for reading a batch of decimals backed by fixed length byte array parquet data type. Arrow stores all
   * decimals in 16 bytes. This method provides the necessary padding to the decimals read. Moreover, Arrow interprets
   * the decimals in Arrow buffer as little endian. Parquet stores fixed length decimals as big endian. So, this method
   * uses {@link DecimalVector#setBigEndian(int, byte[])} method so that the data in Arrow vector is indeed little
   * endian.
   */
  public int nextBatchFixedLengthDecimal(
      final FieldVector vector, final int expectedBatchSize, final int numValsInVector,
      final int typeWidth, NullabilityHolder nullabilityHolder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (eagerDecodeDictionary) {
      ((VectorizedParquetValuesReader) vectorizedDefinitionLevelReader).readBatchOfDictionaryEncodedFixedLengthDecimals(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          nullabilityHolder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      ((VectorizedParquetValuesReader) vectorizedDefinitionLevelReader).readBatchOfFixedLengthDecimals(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          nullabilityHolder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  /**
   * Method for reading a batch of variable width data type (ENUM, JSON, UTF8, BSON).
   */
  public int nextBatchVarWidthType(
      final FieldVector vector,
      final int expectedBatchSize,
      final int numValsInVector,
      NullabilityHolder nullabilityHolder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (eagerDecodeDictionary) {
      ((VectorizedParquetValuesReader) vectorizedDefinitionLevelReader).readBatchOfDictionaryEncodedVarWidth(
          vector,
          numValsInVector,
          actualBatchSize,
          nullabilityHolder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      ((VectorizedParquetValuesReader) vectorizedDefinitionLevelReader).readBatchVarWidth(
          vector,
          numValsInVector,
          actualBatchSize,
          nullabilityHolder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  /**
   * Method for reading batches of fixed width binary type (e.g. BYTE[7]). Spark does not support fixed width binary
   * data type. To work around this limitation, the data is read as fixed width binary from parquet and stored in a
   * {@link VarBinaryVector} in Arrow.
   */
  public int nextBatchFixedWidthBinary(
      final FieldVector vector, final int expectedBatchSize, final int numValsInVector,
      final int typeWidth, NullabilityHolder nullabilityHolder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (eagerDecodeDictionary) {
      ((VectorizedParquetValuesReader) vectorizedDefinitionLevelReader).readBatchOfDictionaryEncodedFixedWidthBinary(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          nullabilityHolder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      ((VectorizedParquetValuesReader) vectorizedDefinitionLevelReader).readBatchOfFixedWidthBinary(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          nullabilityHolder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  /**
   * Method for reading batches of booleans.
   */
  public int nextBatchBoolean(
      final FieldVector vector,
      final int expectedBatchSize,
      final int numValsInVector,
      NullabilityHolder nullabilityHolder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    ((VectorizedParquetValuesReader) vectorizedDefinitionLevelReader)
        .readBatchOfBooleans(vector, numValsInVector, actualBatchSize,
            nullabilityHolder, plainValuesReader);
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  @Override
  protected boolean supportsVectorizedReads() {
    return true;
  }

  @Override
  protected BasePageIterator.IntIterator newNonVectorizedDefinitionLevelReader(ValuesReader dlReader) {
    throw new UnsupportedOperationException("Non-vectorized reads not supported");
  }

  @Override
  protected ValuesReader newVectorizedDefinitionLevelReader(ColumnDescriptor desc) {
    int bitwidth = BytesUtils.getWidthFromMaxInt(desc.getMaxDefinitionLevel());
    return new VectorizedParquetValuesReader(bitwidth, desc.getMaxDefinitionLevel(), setArrowValidityVector);
  }

  @Override
  protected void initDataReader(Encoding dataEncoding, ByteBufferInputStream in, int valueCount) {
    ValuesReader previousReader = plainValuesReader;
    this.eagerDecodeDictionary = dataEncoding.usesDictionary() && dictionary != null && !allPagesDictEncoded;
    if (dataEncoding.usesDictionary()) {
      if (dictionary == null) {
        throw new ParquetDecodingException(
            "could not read page in col " + desc + " as the dictionary was missing for encoding " + dataEncoding);
      }
      try {
        dictionaryEncodedValuesReader =
            new VectorizedDictionaryEncodedParquetValuesReader(desc.getMaxDefinitionLevel(), setArrowValidityVector);
        dictionaryEncodedValuesReader.initFromPage(valueCount, in);
      } catch (IOException e) {
        throw new ParquetDecodingException("could not read page in col " + desc, e);
      }
    } else {
      plainValuesReader = new ValuesAsBytesReader();
      plainValuesReader.initFromPage(valueCount, in);
    }
    if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, dataEncoding) &&
        previousReader != null && previousReader instanceof RequiresPreviousReader) {
      // previous reader can only be set if reading sequentially
      ((RequiresPreviousReader) plainValuesReader).setPreviousReader(previousReader);
    }
  }

}
