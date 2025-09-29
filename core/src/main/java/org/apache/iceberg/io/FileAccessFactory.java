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
package org.apache.iceberg.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.hadoop.util.Lists;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;

/**
 * Interface that provides a unified abstraction for converting between data file formats and
 * input/output data representations.
 *
 * <p>FileAccessFactory serves as a bridge between storage formats ({@link FileFormat}) and expected
 * input/output data structures, optimizing performance through direct conversion without
 * intermediate representations. File format implementations handle the low-level parsing details
 * while the object model determines the in-memory representation used for the parsed data.
 * Together, these provide a consistent API for consuming data files while optimizing for specific
 * processing engines.
 *
 * <p>Iceberg provides these built-in object models:
 *
 * <ul>
 *   <li><strong>generic</strong> - for {@link Record} objects (engine-agnostic)
 *   <li><strong>spark</strong> - for Apache Spark InternalRow objects
 *   <li><strong>spark-vectorized</strong> - for columnar batch processing in Spark (not supported
 *       with {@link FileFormat#AVRO})
 *   <li><strong>flink</strong> - for Apache Flink RowData objects
 *   <li><strong>arrow</strong> - for Apache Arrow columnar format (only supported with {@link
 *       FileFormat#PARQUET})
 * </ul>
 *
 * <p>Processing engines can implement custom object models to integrate with Iceberg's file reading
 * and writing capabilities.
 *
 * @param <E> input schema type used when converting input data to the file format
 * @param <D> output type used for reading data, and input type for writing data and deletes
 */
public interface FileAccessFactory<E, D> {
  /** The file format which is read/written by the object model. */
  FileFormat format();

  /**
   * Returns the unique identifier for the object model implementation processed by this factory.
   *
   * <p>The object model names (such as "generic", "spark", "spark-vectorized", "flink", "arrow")
   * act as a contract specifying the expected data structures for both reading (converting file
   * formats into output objects) and writing (converting input objects into file formats). This
   * ensures proper integration between Iceberg's storage layer and processing engines.
   *
   * @return string identifier for this object model implementation
   */
  String objectModelName();

  /**
   * Creates a writer builder for standard data files.
   *
   * <p>The returned {@link WriteBuilder} configures and creates a writer that converts input
   * objects into the file format supported by this factory for regular data content.
   *
   * <p>The builder follows the fluent pattern for configuring writer properties like compression,
   * encryption, row group size, and other format-specific options.
   *
   * @param outputFile destination for the written data
   * @return configured writer builder for standard data files
   * @param <B> the concrete builder type for method chaining
   */
  <B extends WriteBuilder<B, E, D>> B dataWriteBuilder(OutputFile outputFile);

  /**
   * Creates a writer builder for equality delete files.
   *
   * <p>The returned {@link WriteBuilder} configures and creates a writer that converts input
   * objects into the file format supported by this factory for equality delete content.
   *
   * <p>Equality delete files contain records that identify rows to be deleted based on equality
   * conditions.
   *
   * <p>The builder follows the fluent pattern for configuring writer properties like compression,
   * encryption, row group size, and other format-specific options.
   *
   * @param outputFile destination for the written equality delete data
   * @return configured writer builder for equality delete files
   * @param <B> the concrete builder type for method chaining
   */
  <B extends WriteBuilder<B, E, D>> B equalityDeleteWriteBuilder(OutputFile outputFile);

  /**
   * Creates a writer builder for position delete files.
   *
   * <p>The returned {@link WriteBuilder} configures and creates a writer that converts {@link
   * PositionDelete} objects into the file format supported by this factory for position delete
   * content.
   *
   * <p>Position delete files contain records that identify rows to be deleted by file path and
   * position. Each PositionDelete object could contain the writer's output type in its row field.
   *
   * <p>The builder follows the fluent pattern for configuring writer properties like compression,
   * encryption, row group size, and other format-specific options.
   *
   * @param outputFile destination for the written position delete data
   * @return configured writer builder for position delete files
   * @param <B> the concrete builder type for method chaining
   */
  <B extends WriteBuilder<B, E, PositionDelete<D>>> B positionDeleteWriteBuilder(
      OutputFile outputFile);

  /**
   * Creates a file reader builder for the specified input file.
   *
   * <p>The returned {@link ReadBuilder} configures and creates a reader that converts data from the
   * file format into output objects supported by this factory. The builder allows for configuration
   * of various reading aspects like schema projection, predicate pushdown, row/batch size,
   * container reuse, encryption settings, and other format-specific options.
   *
   * <p>The builder follows the fluent pattern for configuring reader properties and ultimately
   * creates a {@link CloseableIterable} for consuming the file data.
   *
   * @param inputFile source file to read from
   * @return configured reader builder for the specified input
   * @param <B> the concrete builder type for method chaining
   */
  <B extends ReadBuilder<B, D>> B readBuilder(InputFile inputFile);

  default BiFunction<Schema, Integer[][], Combiner<D>> combiner() {
    throw new UnsupportedOperationException("Not implemented");
  }

  default BiFunction<Schema, Integer[], Narrower<D>> narrower() {
    throw new UnsupportedOperationException("Not implemented");
  }

  interface Combiner<E> {
    E combine(List<E> elements);
  }

  interface Narrower<E> {
    E narrow(E elements);
  }

  static <E> CloseableIterable<E> combiner(
      Collection<CloseableIterable<E>> iterable, Combiner<E> combiner, boolean multiThreaded) {
    return CloseableIterable.combine(
        () ->
            multiThreaded
                ? new MultiThreadedCombiningReadIterator<>(
                    iterable.stream().map(Iterable::iterator).collect(Collectors.toList()),
                    combiner)
                : new SingleThreadedCombiningReadIterator<>(
                    iterable.stream().map(Iterable::iterator).collect(Collectors.toList()),
                    combiner),
        () -> {
          for (CloseableIterable<E> inner : iterable) {
            inner.close();
          }
        });
  }

  class SingleThreadedCombiningReadIterator<E> implements Iterator<E>, Closeable {
    private final Collection<Iterator<E>> iterators;
    private final Combiner<E> combiner;
    private boolean closed = false;

    private SingleThreadedCombiningReadIterator(
        Collection<Iterator<E>> iterators, Combiner<E> combiner) {
      this.iterators = iterators;
      this.combiner = combiner;
    }

    @Override
    public boolean hasNext() {
      if (closed) {
        return false;
      }

      // If any iterator doesn't have next, return false
      for (Iterator<E> iterator : iterators) {
        if (!iterator.hasNext()) {
          return false;
        }
      }

      return true;
    }

    @Override
    public E next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      return combiner.combine(iterators.stream().map(Iterator::next).collect(Collectors.toList()));
    }

    @Override
    public void close() throws IOException {
      closed = true;
    }
  }

  class MultiThreadedCombiningReadIterator<E> implements Iterator<E>, Closeable {
    private final List<Iterator<E>> iterators;
    private final Combiner<E> combiner;
    private final ExecutorService executorService;
    private final List<BlockingQueue<E>> queues;
    private boolean closed = false;
    private boolean fetching = false;

    private MultiThreadedCombiningReadIterator(
        Collection<Iterator<E>> iterators, Combiner<E> combiner) {
      this.iterators = Lists.newArrayList(iterators);
      this.combiner = combiner;
      this.executorService = Executors.newFixedThreadPool(iterators.size());
      this.queues =
          iterators.stream()
              .map(i -> Queues.<E>newLinkedBlockingDeque(100))
              .collect(Collectors.toList());
    }

    @Override
    public boolean hasNext() {
      if (closed) {
        return false;
      }

      if (!fetching) {
        fetching = true;

        // Start fetching elements from each iterator in parallel
        for (int i = 0; i < iterators.size(); i++) {
          final Iterator<E> iterator = iterators.get(i);
          final BlockingQueue<E> queue = queues.get(i);
          executorService.execute(
              () -> {
                try {
                  while (iterator.hasNext()) {
                    synchronized (iterator) {
                      queue.put(iterator.next());
                    }
                  }
                } catch (Exception e) {
                  throw new RuntimeIOException("Alma %s", e.getMessage());
                }
              });
        }
      }

      // If any iterator doesn't have next, return false
      for (int i = 0; i < iterators.size(); i++) {
        final Iterator<E> iterator = iterators.get(i);
        final BlockingQueue<E> queue = queues.get(i);
        if (!iterator.hasNext() && queue.isEmpty()) {
          synchronized (iterator) {
            if (!iterator.hasNext() && queue.isEmpty()) {
              return false;
            }
          }
        }
      }

      return true;
    }

    @Override
    public E next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      return combiner.combine(
          queues.stream()
              .map(
                  q -> {
                    try {
                      return q.take();
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new RuntimeIOException("Alma %s", e.getMessage());
                    }
                  })
              .collect(Collectors.toList()));
    }

    @Override
    public void close() throws IOException {
      closed = true;
      executorService.shutdown();
    }
  }
}
