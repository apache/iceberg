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
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class BasePageIterator {
  private static final Logger LOG = LoggerFactory.getLogger(BasePageIterator.class);

  protected final ColumnDescriptor desc;
  protected final String writerVersion;

  // iterator state
  protected boolean hasNext = false;
  protected int triplesRead = 0;
  protected int currentDL = 0;
  protected int currentRL = 0;

  // page bookkeeping
  protected Dictionary dictionary = null;
  protected DataPage page = null;
  protected int triplesCount = 0;
  protected Encoding valueEncoding = null;
  protected IntIterator definitionLevels = null;
  protected IntIterator repetitionLevels = null;
  protected ValuesReader vectorizedDefinitionLevelReader = null;
  protected ValuesReader values = null;

  protected BasePageIterator(ColumnDescriptor descriptor, String writerVersion) {
    this.desc = descriptor;
    this.writerVersion = writerVersion;
  }

  protected abstract void reset();

  protected abstract boolean supportsVectorizedReads();

  protected abstract IntIterator newNonVectorizedDefinitionLevelReader(ValuesReader dlReader);

  protected abstract ValuesReader newVectorizedDefinitionLevelReader(ColumnDescriptor descriptor);

  protected abstract void initDataReader(Encoding dataEncoding, ByteBufferInputStream in, int valueCount);

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
    this.hasNext = triplesRead < triplesCount;
  }

  protected void initFromPage(DataPageV1 initPage) {
    this.triplesCount = initPage.getValueCount();
    ValuesReader dlReader = null;
    if (supportsVectorizedReads()) {
      this.vectorizedDefinitionLevelReader = newVectorizedDefinitionLevelReader(desc);
    } else {
      dlReader = initPage.getDlEncoding().getValuesReader(desc, ValuesType.DEFINITION_LEVEL);
      this.definitionLevels = newNonVectorizedDefinitionLevelReader(dlReader);
    }
    ValuesReader rlReader = initPage.getRlEncoding().getValuesReader(desc, ValuesType.REPETITION_LEVEL);
    this.repetitionLevels = new PageIterator.ValuesReaderIntIterator(rlReader);
    try {
      BytesInput bytes = initPage.getBytes();
      LOG.debug("page size {} bytes and {} records", bytes.size(), triplesCount);
      LOG.debug("reading repetition levels at 0");
      ByteBufferInputStream in = bytes.toInputStream();
      rlReader.initFromPage(triplesCount, in);
      LOG.debug("reading definition levels at {}", in.position());
      if (supportsVectorizedReads()) {
        this.vectorizedDefinitionLevelReader.initFromPage(triplesCount, in);
      } else {
        dlReader.initFromPage(triplesCount, in);
      }
      LOG.debug("reading data at {}", in.position());
      initDataReader(initPage.getValueEncoding(), in, initPage.getValueCount());
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page " + initPage + " in col " + desc, e);
    }
  }

  protected void initFromPage(DataPageV2 initPage) {
    this.triplesCount = initPage.getValueCount();
    this.repetitionLevels = newRLEIterator(desc.getMaxRepetitionLevel(), initPage.getRepetitionLevels());
    if (supportsVectorizedReads()) {
      this.vectorizedDefinitionLevelReader = newVectorizedDefinitionLevelReader(desc);
    } else {
      this.definitionLevels = newRLEIterator(desc.getMaxDefinitionLevel(), initPage.getDefinitionLevels());
    }
    LOG.debug("page data size {} bytes and {} records", initPage.getData().size(), triplesCount);
    try {
      initDataReader(initPage.getDataEncoding(), initPage.getData().toInputStream(), triplesCount);
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page " + initPage + " in col " + desc, e);
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

  public void setDictionary(Dictionary dict) {
    this.dictionary = dict;
  }

  protected abstract static class IntIterator {
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
