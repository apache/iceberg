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

import java.util.Optional;
import java.util.PrimitiveIterator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.Binary;

public abstract class ColumnIterator<T> extends BaseColumnIterator implements TripleIterator<T> {
  @SuppressWarnings("unchecked")
  static <T> ColumnIterator<T> newIterator(ColumnDescriptor desc, String writerVersion) {
    switch (desc.getPrimitiveType().getPrimitiveTypeName()) {
      case BOOLEAN:
        return (ColumnIterator<T>)
            new ColumnIterator<Boolean>(desc, writerVersion) {
              @Override
              public Boolean next() {
                return nextBoolean();
              }
            };
      case INT32:
        return (ColumnIterator<T>)
            new ColumnIterator<Integer>(desc, writerVersion) {
              @Override
              public Integer next() {
                return nextInteger();
              }
            };
      case INT64:
        return (ColumnIterator<T>)
            new ColumnIterator<Long>(desc, writerVersion) {
              @Override
              public Long next() {
                return nextLong();
              }
            };
      case INT96:
        return (ColumnIterator<T>)
            new ColumnIterator<Binary>(desc, writerVersion) {
              @Override
              public Binary next() {
                return nextBinary();
              }
            };
      case FLOAT:
        return (ColumnIterator<T>)
            new ColumnIterator<Float>(desc, writerVersion) {
              @Override
              public Float next() {
                return nextFloat();
              }
            };
      case DOUBLE:
        return (ColumnIterator<T>)
            new ColumnIterator<Double>(desc, writerVersion) {
              @Override
              public Double next() {
                return nextDouble();
              }
            };
      case FIXED_LEN_BYTE_ARRAY:
      case BINARY:
        return (ColumnIterator<T>)
            new ColumnIterator<Binary>(desc, writerVersion) {
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

  private final PageIterator<T> pageIterator;
  private long targetRowIndex = Long.MIN_VALUE;

  private ColumnIterator(ColumnDescriptor desc, String writerVersion) {
    super(desc);
    this.pageIterator = PageIterator.newIterator(desc, writerVersion);
  }

  @Override
  public int currentDefinitionLevel() {
    advance();
    return pageIterator.currentDefinitionLevel();
  }

  @Override
  public int currentRepetitionLevel() {
    advance();
    return pageIterator.currentRepetitionLevel();
  }

  @Override
  public boolean nextBoolean() {
    this.triplesRead += 1;
    advance();
    return pageIterator.nextBoolean();
  }

  @Override
  public int nextInteger() {
    this.triplesRead += 1;
    advance();
    return pageIterator.nextInteger();
  }

  @Override
  public long nextLong() {
    this.triplesRead += 1;
    advance();
    return pageIterator.nextLong();
  }

  @Override
  public float nextFloat() {
    this.triplesRead += 1;
    advance();
    return pageIterator.nextFloat();
  }

  @Override
  public double nextDouble() {
    this.triplesRead += 1;
    advance();
    return pageIterator.nextDouble();
  }

  @Override
  public Binary nextBinary() {
    this.triplesRead += 1;
    advance();
    return pageIterator.nextBinary();
  }

  @Override
  public <N> N nextNull() {
    this.triplesRead += 1;
    advance();
    return pageIterator.nextNull();
  }

  @Override
  protected BasePageIterator pageIterator() {
    return pageIterator;
  }

  @Override
  public void setPageSource(PageReader source) {
    setPageSource(source, Optional.empty());
  }

  @Override
  public void setPageSource(PageReader source, Optional<PrimitiveIterator.OfLong> rowIndexes) {
    super.setPageSource(source, rowIndexes);
    if (rowIndexes.isPresent()) {
      this.targetRowIndex = Long.MIN_VALUE;
    }
  }

  @Override
  public boolean needsSynchronizing() {
    return needsSynchronizing;
  }

  @Override
  public void synchronize() {
    numValuesToSkip = 0;
    while (hasNext()) {
      advance();
      if (pageIterator.currentRepetitionLevel() == 0) {
        currentRowIndex += 1;
        if (currentRowIndex > targetRowIndex) {
          targetRowIndex = rowIndexes.hasNext() ? rowIndexes.nextLong() : Long.MAX_VALUE;
        }
      }

      if (currentRowIndex < targetRowIndex) {
        triplesRead += 1;
        if (pageIterator.currentDefinitionLevel() > definitionLevel) {
          numValuesToSkip += 1;
        }

        pageIterator.advance();
      } else {
        break;
      }
    }

    pageIterator.skip(numValuesToSkip);
  }
}
