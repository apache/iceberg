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

import org.apache.parquet.column.ColumnDescriptor;
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
    boolean value = pageIterator.nextBoolean();
    skip();
    return value;
  }

  @Override
  public int nextInteger() {
    this.triplesRead += 1;
    advance();
    int value = pageIterator.nextInteger();
    skip();
    return value;
  }

  @Override
  public long nextLong() {
    this.triplesRead += 1;
    advance();
    long value = pageIterator.nextLong();
    skip();
    return value;
  }

  @Override
  public float nextFloat() {
    this.triplesRead += 1;
    advance();
    float value = pageIterator.nextFloat();
    skip();
    return value;
  }

  @Override
  public double nextDouble() {
    this.triplesRead += 1;
    advance();
    double value = pageIterator.nextDouble();
    skip();
    return value;
  }

  @Override
  public Binary nextBinary() {
    this.triplesRead += 1;
    advance();
    Binary value = pageIterator.nextBinary();
    skip();
    return value;
  }

  @Override
  public <N> N nextNull() {
    this.triplesRead += 1;
    advance();
    N value = pageIterator.nextNull();
    skip();
    return value;
  }

  @Override
  protected BasePageIterator pageIterator() {
    return pageIterator;
  }

  @Override
  protected void skip() {
    if (!synchronizing) {
      return;
    }

    skipValues = 0;
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
          skipValues += 1;
        }

        pageIterator.advance();
      } else {
        break;
      }
    }

    pageIterator.skip(skipValues);
  }
}
