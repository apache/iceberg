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
package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.SortedMerge;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;

/**
 * A {@link PartitionReader} that performs a k-way merge of multiple sorted readers.
 *
 * <p>This reader takes multiple {@link PartitionReader}s (one per file), each producing sorted data
 * according to the same {@link SortOrder}, and merges them into a single sorted stream using
 * Iceberg's {@link SortedMerge} utility.
 *
 * <p>The merge is performed using a priority queue (heap) to efficiently select the next row from
 * among all readers, maintaining the sort order with O(log k) comparisons per row, where k is the
 * number of files being merged.
 *
 * @param <T> the type of InternalRow being read
 */
class MergingPartitionReader<T extends InternalRow> implements PartitionReader<T> {
  private final List<PartitionReader<T>> readers;
  private final CloseableIterator<T> mergedIterator;
  private T current = null;
  private boolean closed = false;

  MergingPartitionReader(
      List<PartitionReader<T>> readers,
      SortOrder sortOrder,
      StructType sparkSchema,
      Schema icebergSchema) {
    Preconditions.checkNotNull(readers, "Readers cannot be null");
    Preconditions.checkArgument(!readers.isEmpty(), "Readers cannot be empty");
    Preconditions.checkNotNull(sortOrder, "Sort order cannot be null");
    Preconditions.checkArgument(sortOrder.isSorted(), "Sort order must be sorted");

    this.readers = readers;

    Comparator<T> comparator =
        (Comparator<T>) new InternalRowComparator(sortOrder, sparkSchema, icebergSchema);

    List<CloseableIterable<T>> iterables =
        readers.stream().map(this::readerToIterable).collect(Collectors.toList());

    SortedMerge<T> sortedMerge = new SortedMerge<>(comparator, iterables);
    this.mergedIterator = sortedMerge.iterator();
  }

  /** Converts a PartitionReader to a CloseableIterable for use with SortedMerge. */
  private CloseableIterable<T> readerToIterable(PartitionReader<T> reader) {
    return new CloseableIterable<T>() {
      @Override
      public CloseableIterator<T> iterator() {
        return new CloseableIterator<T>() {
          private boolean advanced = false;
          private boolean hasNext = false;

          @Override
          public boolean hasNext() {
            if (!advanced) {
              try {
                hasNext = reader.next();
                advanced = true;
              } catch (IOException e) {
                throw new RuntimeException("Failed to advance reader", e);
              }
            }
            return hasNext;
          }

          @Override
          public T next() {
            if (!advanced) {
              hasNext();
            }
            advanced = false;
            // Spark readers reuse InternalRow objects for performance (see
            // SparkParquetReaders.java:547)
            // Return a copy of the row to avoid corruption.
            return (T) reader.get().copy();
          }

          @Override
          public void close() throws IOException {
            reader.close();
          }
        };
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }
    };
  }

  @Override
  public boolean next() throws IOException {
    if (mergedIterator.hasNext()) {
      this.current = mergedIterator.next();
      return true;
    }
    return false;
  }

  @Override
  public T get() {
    return current;
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    try {
      mergedIterator.close();
    } finally {
      closed = true;
    }
  }
}
