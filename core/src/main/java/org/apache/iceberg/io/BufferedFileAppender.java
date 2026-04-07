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

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * A FileAppender that buffers the first N rows, then creates a delegate appender via a factory.
 *
 * <p>The factory receives the buffered rows and is responsible for creating the real appender. Row
 * replay is handled internally. All subsequent {@link #add} calls delegate directly to the real
 * appender.
 *
 * <p>If fewer than {@code bufferSize} rows are written before close, the factory is called with
 * whatever rows were buffered. If no rows were written, the factory is not called and no file is
 * created on disk. In this case, {@link #metrics()} returns {@code new Metrics(0L)} and {@link
 * #length()} returns {@code 0L}.
 *
 * @param <D> the row type
 */
public class BufferedFileAppender<D> implements FileAppender<D> {
  private final int bufferRowCount;
  private final Function<List<D>, FileAppender<D>> appenderFactory;
  private final UnaryOperator<D> copyFunc;
  private List<D> buffer;
  private FileAppender<D> delegate;
  private boolean closed = false;

  /**
   * @param bufferRowCount number of rows to buffer before creating the delegate appender
   * @param appenderFactory given the buffered rows, creates the delegate appender
   */
  public BufferedFileAppender(
      int bufferRowCount, Function<List<D>, FileAppender<D>> appenderFactory) {
    this(bufferRowCount, appenderFactory, UnaryOperator.identity());
  }

  /**
   * @param bufferRowCount number of rows to buffer before creating the delegate appender
   * @param appenderFactory given the buffered rows, creates the delegate appender
   * @param copyFunc copies a row before buffering (needed when row objects are reused, e.g. Spark
   *     InternalRow)
   */
  public BufferedFileAppender(
      int bufferRowCount,
      Function<List<D>, FileAppender<D>> appenderFactory,
      UnaryOperator<D> copyFunc) {
    Preconditions.checkArgument(
        bufferRowCount > 0, "bufferRowCount must be > 0, got %s", bufferRowCount);
    Preconditions.checkNotNull(appenderFactory, "appenderFactory must not be null");
    Preconditions.checkNotNull(copyFunc, "copyFunc must not be null");
    this.bufferRowCount = bufferRowCount;
    this.appenderFactory = appenderFactory;
    this.copyFunc = copyFunc;
    this.buffer = Lists.newArrayListWithCapacity(bufferRowCount);
  }

  @Override
  public void add(D datum) {
    Preconditions.checkState(!closed, "Cannot add to a closed appender");
    if (delegate != null) {
      delegate.add(datum);
    } else {
      buffer.add(copyFunc.apply(datum));
      if (buffer.size() >= bufferRowCount) {
        initialize();
      }
    }
  }

  @Override
  public Metrics metrics() {
    Preconditions.checkState(closed, "Cannot return metrics for unclosed appender");
    if (delegate == null) {
      return new Metrics(0L);
    }

    return delegate.metrics();
  }

  @Override
  public long length() {
    if (delegate != null) {
      return delegate.length();
    }

    // No bytes written to disk yet; data is buffered in memory
    return 0L;
  }

  @Override
  public List<Long> splitOffsets() {
    if (delegate != null) {
      return delegate.splitOffsets();
    }

    return null;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      if (delegate == null && buffer != null && !buffer.isEmpty()) {
        initialize();
      }

      if (delegate != null) {
        delegate.close();
      }

      this.closed = true;
      this.buffer = null;
    }
  }

  private void initialize() {
    delegate = appenderFactory.apply(buffer);
    Preconditions.checkState(delegate != null, "appenderFactory must not return null");
    try {
      buffer.forEach(delegate::add);
    } finally {
      buffer = null;
    }
  }
}
