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

import java.io.IOException;
import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.parquet.CorruptDeltaByteArrays;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.values.RequiresPreviousReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;

abstract class PageIterator<T> extends BasePageIterator implements TripleIterator<T> {
  @SuppressWarnings("unchecked")
  static <T> PageIterator<T> newIterator(ColumnDescriptor desc, String writerVersion) {
    switch (desc.getPrimitiveType().getPrimitiveTypeName()) {
      case BOOLEAN:
        return (PageIterator<T>)
            new PageIterator<Boolean>(desc, writerVersion) {
              @Override
              public Boolean next() {
                return nextBoolean();
              }
            };
      case INT32:
        return (PageIterator<T>)
            new PageIterator<Integer>(desc, writerVersion) {
              @Override
              public Integer next() {
                return nextInteger();
              }
            };
      case INT64:
        return (PageIterator<T>)
            new PageIterator<Long>(desc, writerVersion) {
              @Override
              public Long next() {
                return nextLong();
              }
            };
      case INT96:
        return (PageIterator<T>)
            new PageIterator<Binary>(desc, writerVersion) {
              @Override
              public Binary next() {
                return nextBinary();
              }
            };
      case FLOAT:
        return (PageIterator<T>)
            new PageIterator<Float>(desc, writerVersion) {
              @Override
              public Float next() {
                return nextFloat();
              }
            };
      case DOUBLE:
        return (PageIterator<T>)
            new PageIterator<Double>(desc, writerVersion) {
              @Override
              public Double next() {
                return nextDouble();
              }
            };
      case FIXED_LEN_BYTE_ARRAY:
      case BINARY:
        return (PageIterator<T>)
            new PageIterator<Binary>(desc, writerVersion) {
              @Override
              public Binary next() {
                return nextBinary();
              }
            };
      default:
        throw new UnsupportedOperationException(
            "Unsupported primitive type: " + desc.getPrimitiveType().getPrimitiveTypeName());
    }
  }

  private PageIterator(ColumnDescriptor desc, String writerVersion) {
    super(desc, writerVersion);
  }

  @Override
  public void setPage(DataPage page) {
    super.setPage(page);
    advance();
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

  RuntimeException handleRuntimeException(RuntimeException exception) {
    if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, valueEncoding)
        && exception instanceof ArrayIndexOutOfBoundsException) {
      // this is probably PARQUET-246, which may happen if reading data with
      // MR because this can't be detected without reading all footers
      throw new ParquetDecodingException(
          "Read failure possibly due to " + "PARQUET-246: try setting parquet.split.files to false",
          new ParquetDecodingException(
              String.format(
                  Locale.ROOT,
                  "Can't read value in column %s at value %d out of %d in current page. "
                      + "repetition level: %d, definition level: %d",
                  desc,
                  triplesRead,
                  triplesCount,
                  currentRL,
                  currentDL),
              exception));
    }
    throw new ParquetDecodingException(
        String.format(
            Locale.ROOT,
            "Can't read value in column %s at value %d out of %d in current page. "
                + "repetition level: %d, definition level: %d",
            desc,
            triplesRead,
            triplesCount,
            currentRL,
            currentDL),
        exception);
  }

  @Override
  protected void initDataReader(Encoding dataEncoding, ByteBufferInputStream in, int valueCount) {
    ValuesReader previousReader = values;

    this.valueEncoding = dataEncoding;

    // TODO: May want to change this so that this class is not dictionary-aware.
    // For dictionary columns, this class could rely on wrappers to correctly handle dictionaries
    // This isn't currently possible because RLE must be read by getDictionaryBasedValuesReader
    if (dataEncoding.usesDictionary()) {
      if (dictionary == null) {
        throw new ParquetDecodingException(
            "could not read page in col "
                + desc
                + " as the dictionary was missing for encoding "
                + dataEncoding);
      }
      this.values =
          dataEncoding.getDictionaryBasedValuesReader(desc, ValuesType.VALUES, dictionary);
    } else {
      this.values = dataEncoding.getValuesReader(desc, ValuesType.VALUES);
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

    if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, dataEncoding)
        && previousReader instanceof RequiresPreviousReader) {
      // previous reader can only be set if reading sequentially
      ((RequiresPreviousReader) values).setPreviousReader(previousReader);
    }
  }

  @Override
  protected void initDefinitionLevelsReader(
      DataPageV1 dataPageV1, ColumnDescriptor desc, ByteBufferInputStream in, int triplesCount)
      throws IOException {
    ValuesReader dlReader =
        dataPageV1.getDlEncoding().getValuesReader(desc, ValuesType.DEFINITION_LEVEL);
    this.definitionLevels = new ValuesReaderIntIterator(dlReader);
    dlReader.initFromPage(triplesCount, in);
  }

  @Override
  protected void initDefinitionLevelsReader(DataPageV2 dataPageV2, ColumnDescriptor desc) {
    this.definitionLevels =
        newRLEIterator(desc.getMaxDefinitionLevel(), dataPageV2.getDefinitionLevels());
  }
}
