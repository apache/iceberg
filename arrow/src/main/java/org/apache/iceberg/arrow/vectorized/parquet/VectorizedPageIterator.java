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

import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.parquet.ValuesAsBytesReader;
import org.apache.parquet.CorruptDeltaByteArrays;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.values.RequiresPreviousReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorizedPageIterator {
  private static final Logger LOG = LoggerFactory.getLogger(VectorizedPageIterator.class);

  public VectorizedPageIterator(ColumnDescriptor desc, String writerVersion, int batchSize) {
    this.desc = desc;
    this.writerVersion = writerVersion;
  }

  private final ColumnDescriptor desc;
  private final String writerVersion;

  // iterator state
  private boolean hasNext = false;
  private int triplesRead = 0;

  // page bookkeeping
  private Dictionary dictionary = null;
  private DataPage page = null;
  private int triplesCount = 0;

  // Needed once we add support for complex types. Unused for now.
  private IntIterator repetitionLevels = null;
  private int currentRL = 0;

  private VectorizedParquetValuesReader definitionLevelReader;
  private boolean eagerDecodeDictionary;
  private ValuesAsBytesReader plainValuesReader = null;
  private VectorizedParquetValuesReader dictionaryEncodedValuesReader = null;
  private boolean allPagesDictEncoded;

  public void setPage(DataPage dataPage) {
    this.page = Preconditions.checkNotNull(dataPage, "Cannot read from null page");
    this.page.accept(new DataPage.Visitor<ValuesReader>() {
      @Override
      public ValuesReader visit(DataPageV1 dataPageV1) {
        initFromPage(dataPageV1);
        return null;
      }

      @Override
      public ValuesReader visit(DataPageV2 dataPageV2) {
        initFromPage(dataPageV2);
        return null;
      }
    });
    this.triplesRead = 0;
    advance();
  }

  // Dictionary is set per row group
  public void setDictionaryForColumn(Dictionary dict, boolean allDictEncoded) {
    this.dictionary = dict;
    this.allPagesDictEncoded = allDictEncoded;
  }

  public void reset() {
    this.page = null;
    this.triplesCount = 0;
    this.triplesRead = 0;
    this.repetitionLevels = null;
    this.plainValuesReader = null;
    this.definitionLevelReader = null;
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
   * dictionary ids in Parquet are RLE encoded as well.
   */
  public int nextBatchDictionaryIds(
      final IntVector vector, final int expectedBatchSize,
      final int numValsInVector,
      NullabilityHolder holder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    definitionLevelReader.readBatchOfDictionaryIds(
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
      definitionLevelReader.readBatchOfDictionaryEncodedIntegers(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      definitionLevelReader.readBatchOfIntegers(
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
      definitionLevelReader.readBatchOfDictionaryEncodedLongs(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      definitionLevelReader.readBatchOfLongs(
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
      definitionLevelReader.readBatchOfDictionaryEncodedFloats(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      definitionLevelReader.readBatchOfFloats(
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
      definitionLevelReader.readBatchOfDictionaryEncodedDoubles(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      definitionLevelReader.readBatchOfDoubles(
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
      definitionLevelReader.readBatchOfDictionaryEncodedIntLongBackedDecimals(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          nullabilityHolder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      definitionLevelReader.readBatchOfIntLongBackedDecimals(
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
      definitionLevelReader.readBatchOfDictionaryEncodedFixedLengthDecimals(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          nullabilityHolder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      definitionLevelReader.readBatchOfFixedLengthDecimals(
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
      definitionLevelReader.readBatchOfDictionaryEncodedVarWidth(
          vector,
          numValsInVector,
          actualBatchSize,
          nullabilityHolder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      definitionLevelReader.readBatchVarWidth(
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
      definitionLevelReader.readBatchOfDictionaryEncodedFixedWidthBinary(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          nullabilityHolder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      definitionLevelReader.readBatchOfFixedWidthBinary(
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
    definitionLevelReader.readBatchOfBooleans(vector, numValsInVector, actualBatchSize,
        nullabilityHolder, plainValuesReader);
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  private void advance() {
    if (triplesRead < triplesCount) {
      this.hasNext = true;
    } else {
      this.hasNext = false;
    }
  }

  private void initDataReader(Encoding dataEncoding, ByteBufferInputStream in, int valueCount) {
    ValuesReader previousReader = plainValuesReader;
    this.eagerDecodeDictionary = dataEncoding.usesDictionary() && dictionary != null && !allPagesDictEncoded;
    if (dataEncoding.usesDictionary()) {
      if (dictionary == null) {
        throw new ParquetDecodingException(
            "could not read page in col " + desc + " as the dictionary was missing for encoding " + dataEncoding);
      }
      try {
        dictionaryEncodedValuesReader =
            new VectorizedParquetValuesReader(desc.getMaxDefinitionLevel());
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

  private void initFromPage(DataPageV1 dataPageV1) {
    this.triplesCount = dataPageV1.getValueCount();
    ValuesReader rlReader = dataPageV1.getRlEncoding().getValuesReader(desc, ValuesType.REPETITION_LEVEL);
    ValuesReader dlReader;
    int bitWidth = BytesUtils.getWidthFromMaxInt(desc.getMaxDefinitionLevel());
    this.definitionLevelReader = new VectorizedParquetValuesReader(
        bitWidth, desc.getMaxDefinitionLevel());
    dlReader = this.definitionLevelReader;
    this.repetitionLevels = new ValuesReaderIntIterator(rlReader);
    try {
      BytesInput bytes = dataPageV1.getBytes();
      ByteBufferInputStream in = bytes.toInputStream();
      rlReader.initFromPage(triplesCount, in);
      dlReader.initFromPage(triplesCount, in);
      initDataReader(dataPageV1.getValueEncoding(), in, dataPageV1.getValueCount());
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page " + dataPageV1 + " in col " + desc, e);
    }
  }

  private void initFromPage(DataPageV2 dataPageV2) {
    this.triplesCount = dataPageV2.getValueCount();
    this.repetitionLevels = newRLEIterator(desc.getMaxRepetitionLevel(), dataPageV2.getRepetitionLevels());
    LOG.debug("page data size {} bytes and {} records", dataPageV2.getData().size(), triplesCount);
    try {
      int bitWidth = BytesUtils.getWidthFromMaxInt(desc.getMaxDefinitionLevel());
      initDataReader(dataPageV2.getDataEncoding(), dataPageV2.getData().toInputStream(), triplesCount);
      this.definitionLevelReader = new VectorizedParquetValuesReader(bitWidth, false,
          desc.getMaxDefinitionLevel());
      definitionLevelReader.initFromPage(triplesCount, dataPageV2.getDefinitionLevels().toInputStream());
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page " + dataPageV2 + " in col " + desc, e);
    }
  }

  private IntIterator newRLEIterator(int maxLevel, BytesInput bytes) {
    try {
      if (maxLevel == 0) {
        return new NullIntIterator();
      }
      return new RLEIntIterator(
          new RunLengthBitPackingHybridDecoder(
              BytesUtils.getWidthFromMaxInt(maxLevel),
              bytes.toInputStream()));
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read levels in page for col " + desc, e);
    }
  }

  abstract static class IntIterator {
    abstract int nextInt();
  }

  static class ValuesReaderIntIterator extends IntIterator {
    private final ValuesReader delegate;

    ValuesReaderIntIterator(ValuesReader delegate) {
      this.delegate = delegate;
    }

    @Override
    int nextInt() {
      return delegate.readInteger();
    }
  }

  static class RLEIntIterator extends IntIterator {
    private final RunLengthBitPackingHybridDecoder delegate;

    RLEIntIterator(RunLengthBitPackingHybridDecoder delegate) {
      this.delegate = delegate;
    }

    @Override
    int nextInt() {
      try {
        return delegate.readInt();
      } catch (IOException e) {
        throw new ParquetDecodingException(e);
      }
    }
  }

  private static final class NullIntIterator extends IntIterator {
    @Override
    int nextInt() {
      return 0;
    }
  }
}
