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

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.function.IntPredicate;
import java.util.function.IntUnaryOperator;

/**
 * Iterator implementation for page indexes.
 */
class IndexIterator implements PrimitiveIterator.OfInt {
  private final int endIndex;
  private final IntPredicate filter;
  private final IntUnaryOperator translator;
  private int index;

  private IndexIterator(int startIndex, int endIndex, IntPredicate filter, IntUnaryOperator translator) {
    this.endIndex = endIndex;
    this.filter = filter;
    this.translator = translator;
    index = nextPageIndex(startIndex);
  }

  static PrimitiveIterator.OfInt all(int pageCount) {
    return new IndexIterator(0, pageCount, i -> true, i -> i);
  }

  static PrimitiveIterator.OfInt filter(int pageCount, IntPredicate filter) {
    return new IndexIterator(0, pageCount, filter, i -> i);
  }

  private int nextPageIndex(int startIndex) {
    for (int i = startIndex; i < endIndex; ++i) {
      if (filter.test(i)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public boolean hasNext() {
    return index >= 0;
  }

  @Override
  public int nextInt() {
    if (hasNext()) {
      int ret = index;
      index = nextPageIndex(index + 1);
      return translator.applyAsInt(ret);
    }
    throw new NoSuchElementException();
  }
}

