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
import java.util.NoSuchElementException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.parquet.hadoop.ParquetReader;

public class ParquetIterable<T> extends CloseableGroup implements CloseableIterable<T> {
  private final ParquetReader.Builder<T> builder;

  ParquetIterable(ParquetReader.Builder<T> builder) {
    this.builder = builder;
  }

  @Override
  public CloseableIterator<T> iterator() {
    try {
      ParquetReader<T> reader = builder.build();
      addCloseable(reader);
      return new ParquetIterator<>(reader);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to create Parquet reader");
    }
  }

  private static class ParquetIterator<T> implements CloseableIterator<T> {
    private final ParquetReader<T> parquet;
    private boolean needsAdvance = false;
    private boolean hasNext = false;
    private T next;

    ParquetIterator(ParquetReader<T> parquet) {
      this.parquet = parquet;
      this.next = advance();
    }

    @Override
    public boolean hasNext() {
      if (needsAdvance) {
        this.next = advance();
      }
      return hasNext;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      this.needsAdvance = true;

      return next;
    }

    private T advance() {
      // this must be called in hasNext because it reuses an UnsafeRow
      try {
        T nextRecord = parquet.read();
        this.needsAdvance = false;
        this.hasNext = nextRecord != null;
        return nextRecord;
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported");
    }

    @Override
    public void close() throws IOException {
      parquet.close();
    }
  }
}
