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

package org.apache.iceberg.parquet;

import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.parquet.CorruptDeltaByteArrays;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.values.RequiresPreviousReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static org.apache.parquet.column.ValuesType.DEFINITION_LEVEL;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;
import static org.apache.parquet.column.ValuesType.VALUES;

abstract class PageIterator<T> implements TripleIterator<T> {
  private static final Logger LOG = LoggerFactory.getLogger(PageIterator.class);

  @SuppressWarnings("unchecked")
  static <T> PageIterator<T> newIterator(ColumnDescriptor desc, String writerVersion) {
    switch (desc.getPrimitiveType().getPrimitiveTypeName()) {
      case BOOLEAN:
        return (PageIterator<T>) new PageIterator<Boolean>(desc, writerVersion) {
          @Override
          public Boolean next() {
            return nextBoolean();
          }
        };
      case INT32:
        return (PageIterator<T>) new PageIterator<Integer>(desc, writerVersion) {
          @Override
          public Integer next() {
            return nextInteger();
          }
        };
      case INT64:
        return (PageIterator<T>) new PageIterator<Long>(desc, writerVersion) {
          @Override
          public Long next() {
            return nextLong();
          }
        };
      case FLOAT:
        return (PageIterator<T>) new PageIterator<Float>(desc, writerVersion) {
          @Override
          public Float next() {
            return nextFloat();
          }
        };
      case DOUBLE:
        return (PageIterator<T>) new PageIterator<Double>(desc, writerVersion) {
          @Override
          public Double next() {
            return nextDouble();
          }
        };
      case FIXED_LEN_BYTE_ARRAY:
      case BINARY:
        return (PageIterator<T>) new PageIterator<Binary>(desc, writerVersion) {
          @Override
          public Binary next() {
            return nextBinary();
          }
        };
      default:
        throw new UnsupportedOperationException("Unsupported primitive type: "
                + desc.getPrimitiveType().getPrimitiveTypeName());
    }
  }

  private final ColumnDescriptor desc;
  private final String writerVersion;

  // iterator state
  private boolean hasNext = false;
  private int triplesRead = 0;
  private int currentDL = 0;
  private int currentRL = 0;

  // page bookkeeping
  private Dictionary dict = null;
  private DataPage page = null;
  private int triplesCount = 0;
  private Encoding valueEncoding = null;
  private IntIterator definitionLevels = null;
  private IntIterator repetitionLevels = null;
  private ValuesReader values = null;

  private PageIterator(ColumnDescriptor desc, String writerVersion) {
    this.desc = desc;
    this.writerVersion = writerVersion;
  }

  public void setPage(DataPage page) {
    Preconditions.checkNotNull(page, "Cannot read from null page");
    this.page = page;
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

  public void setDictionary(Dictionary dict) {
    this.dict = dict;
  }

  public void reset() {
    this.page = null;
    this.triplesCount = 0;
    this.triplesRead = 0;
    this.definitionLevels = null;
    this.repetitionLevels = null;
    this.values = null;
    this.hasNext = false;
  }

  public int currentPageCount() {
    return triplesCount;
  }

  @Override
  public boolean hasNext() {
    return hasNext;
  }

  @Override
  public int currentDefinitionLevel() {
    Preconditions.checkArgument(currentDL >= 0, "Should not read definition, past page end");
    return currentDL;
  }

  @Override
  public int currentRepetitionLevel() {
//    Preconditions.checkArgument(currentDL >= 0, "Should not read repetition, past page end");
    return currentRL;
  }

  @Override
  public boolean nextBoolean() {
    advance();
    try {
      return values.readBoolean();
    } catch (RuntimeException e) {
      throw handleRuntimeException(e);
    }
  }

  @Override
  public int nextInteger() {
    advance();
    try {
      return values.readInteger();
    } catch (RuntimeException e) {
      throw handleRuntimeException(e);
    }
  }

  @Override
  public long nextLong() {
    advance();
    try {
      return values.readLong();
    } catch (RuntimeException e) {
      throw handleRuntimeException(e);
    }
  }

  @Override
  public float nextFloat() {
    advance();
    try {
      return values.readFloat();
    } catch (RuntimeException e) {
      throw handleRuntimeException(e);
    }
  }

  @Override
  public double nextDouble() {
    advance();
    try {
      return values.readDouble();
    } catch (RuntimeException e) {
      throw handleRuntimeException(e);
    }
  }

  @Override
  public Binary nextBinary() {
    advance();
    try {
      return values.readBytes();
    } catch (RuntimeException e) {
      throw handleRuntimeException(e);
    }
  }

  @Override
  public <V> V nextNull() {
    advance();
    // values do not contain nulls
    return null;
  }

  private void advance() {
    if (triplesRead < triplesCount) {
      this.currentDL = definitionLevels.nextInt();
      this.currentRL = repetitionLevels.nextInt();
      this.triplesRead += 1;
      this.hasNext = true;
    } else {
      this.currentDL = -1;
      this.currentRL = -1;
      this.hasNext = false;
    }
  }

  RuntimeException handleRuntimeException(RuntimeException e) {
    if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, valueEncoding) &&
        e instanceof ArrayIndexOutOfBoundsException) {
      // this is probably PARQUET-246, which may happen if reading data with
      // MR because this can't be detected without reading all footers
      throw new ParquetDecodingException("Read failure possibly due to " +
          "PARQUET-246: try setting parquet.split.files to false",
          new ParquetDecodingException(
              format("Can't read value in column %s at value %d out of %d in current page. " +
                     "repetition level: %d, definition level: %d",
                  desc, triplesRead, triplesCount, currentRL, currentDL),
              e));
    }
    throw new ParquetDecodingException(
        format("Can't read value in column %s at value %d out of %d in current page. " +
               "repetition level: %d, definition level: %d",
            desc, triplesRead, triplesCount, currentRL, currentDL),
        e);
  }

  private void initDataReader(Encoding dataEncoding, ByteBufferInputStream in, int valueCount) {
    ValuesReader previousReader = values;

    this.valueEncoding = dataEncoding;

    // TODO: May want to change this so that this class is not dictionary-aware.
    // For dictionary columns, this class could rely on wrappers to correctly handle dictionaries
    // This isn't currently possible because RLE must be read by getDictionaryBasedValuesReader
    if (dataEncoding.usesDictionary()) {
      if (dict == null) {
        throw new ParquetDecodingException(
            "could not read page in col " + desc + " as the dictionary was missing for encoding " + dataEncoding);
      }
      this.values = dataEncoding.getDictionaryBasedValuesReader(desc, VALUES, dict);
    } else {
      this.values = dataEncoding.getValuesReader(desc, VALUES);
    }

//    if (dataEncoding.usesDictionary() && converter.hasDictionarySupport()) {
//      bindToDictionary(dictionary);
//    } else {
//      bind(path.getType());
//    }

    try {
      values.initFromPage(valueCount, in);
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page in col " + desc, e);
    }

    if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, dataEncoding) &&
        previousReader != null && previousReader instanceof RequiresPreviousReader) {
      // previous reader can only be set if reading sequentially
      ((RequiresPreviousReader) values).setPreviousReader(previousReader);
    }
  }


  private void initFromPage(DataPageV1 page) {
    this.triplesCount = page.getValueCount();
    ValuesReader rlReader = page.getRlEncoding().getValuesReader(desc, REPETITION_LEVEL);
    ValuesReader dlReader = page.getDlEncoding().getValuesReader(desc, DEFINITION_LEVEL);
    this.repetitionLevels = new ValuesReaderIntIterator(rlReader);
    this.definitionLevels = new ValuesReaderIntIterator(dlReader);
    try {
      BytesInput bytes = page.getBytes();
      LOG.debug("page size {} bytes and {} records", bytes.size(), triplesCount);
      LOG.debug("reading repetition levels at 0");
      ByteBufferInputStream in = bytes.toInputStream();
      rlReader.initFromPage(triplesCount, in);
      LOG.debug("reading definition levels at {}", in.position());
      dlReader.initFromPage(triplesCount, in);
      LOG.debug("reading data at {}", in.position());
      initDataReader(page.getValueEncoding(), in, page.getValueCount());
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page " + page + " in col " + desc, e);
    }
  }

  private void initFromPage(DataPageV2 page) {
    this.triplesCount = page.getValueCount();
    this.repetitionLevels = newRLEIterator(desc.getMaxRepetitionLevel(), page.getRepetitionLevels());
    this.definitionLevels = newRLEIterator(desc.getMaxDefinitionLevel(), page.getDefinitionLevels());
    LOG.debug("page data size {} bytes and {} records", page.getData().size(), triplesCount);
    try {
      initDataReader(page.getDataEncoding(), page.getData().toInputStream(), triplesCount);
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page " + page + " in col " + desc, e);
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

  static abstract class IntIterator {
    abstract int nextInt();
  }

  static class ValuesReaderIntIterator extends IntIterator {
    ValuesReader delegate;

    ValuesReaderIntIterator(ValuesReader delegate) {
      super();
      this.delegate = delegate;
    }

    @Override
    int nextInt() {
      return delegate.readInteger();
    }
  }

  static class RLEIntIterator extends IntIterator {
    RunLengthBitPackingHybridDecoder delegate;

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
