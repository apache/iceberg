/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.parquet;

import com.netflix.iceberg.exceptions.RuntimeIOException;
import org.apache.parquet.hadoop.ParquetReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

class ParquetIterable<T> implements Iterable<T> {
  private final ParquetReader.Builder<T> builder;

  ParquetIterable(ParquetReader.Builder<T> builder) {
    this.builder = builder;
  }

  @Override
  public Iterator<T> iterator() {
    try {
      return new ParquetIterator<>(builder.build());
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to create Parquet reader");
    }
  }

  private static class ParquetIterator<T> implements Iterator<T> {
    private final ParquetReader<T> parquet;
    private boolean hasNext = false;
    private T next = null;

    ParquetIterator(ParquetReader<T> parquet) {
      this.parquet = parquet;
      this.next = advance();
    }

    @Override
    public boolean hasNext() {
      return hasNext;
    }

    @Override
    public T next() {
      if (!hasNext) {
        throw new NoSuchElementException();
      }
      T toReturn = next;
      this.next = advance();
      return toReturn;
    }

    private T advance() {
      try {
        T next = parquet.read();
        this.hasNext = (next != null);
        return next;
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported");
    }
  }
}
