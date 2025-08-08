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
package org.apache.iceberg.data;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileWriter;

/**
 * Utility class for creating writers that apply a transformation to each row before writing it.
 * {@link FileWriter}s and {@link FileAppender} only support types directly converted from Iceberg
 * types (like: int). When an engine needs to convert its internal types that aren't directly
 * supported by Iceberg (like: byte, short) to Iceberg-compatible types, it can use these
 * transforming writers to handle the conversion automatically before writing.
 */
public class TransformingWriters {
  private TransformingWriters() {}

  public static <T> DataWriter<T> of(DataWriter<T> delegate, RowTransformer<T> transformer) {
    return new TransformingDataWriter<>(delegate, transformer);
  }

  public static <T> EqualityDeleteWriter<T> of(
      EqualityDeleteWriter<T> delegate, RowTransformer<T> transformer) {
    return new TransformingEqualityDeleteWriter<>(delegate, transformer);
  }

  public static <T> PositionDeleteWriter<T> of(
      PositionDeleteWriter<T> delegate, RowTransformer<T> transformer) {
    return new TransformingPositionDeleteWriter<>(delegate, transformer);
  }

  public static <T> FileAppender<T> of(FileAppender<T> delegate, RowTransformer<T> transformer) {
    return new TransformingAppender<>(delegate, transformer);
  }

  private static class TransformingDataWriter<T> extends DataWriter<T> {
    private final RowTransformer<T> transformer;

    private TransformingDataWriter(DataWriter<T> delegate, RowTransformer<T> transformer) {
      super(delegate);
      this.transformer = transformer;
    }

    @Override
    public void write(T row) {
      super.write(transformer.transform(row));
    }
  }

  private static class TransformingEqualityDeleteWriter<T> extends EqualityDeleteWriter<T> {
    private final RowTransformer<T> transformer;

    private TransformingEqualityDeleteWriter(
        EqualityDeleteWriter<T> delegate, RowTransformer<T> transformer) {
      super(delegate);
      this.transformer = transformer;
    }

    @Override
    public void write(T row) {
      super.write(transformer.transform(row));
    }
  }

  private static class TransformingPositionDeleteWriter<T> extends PositionDeleteWriter<T> {
    private final RowTransformer<T> transformer;
    private final PositionDelete<T> positionDelete = PositionDelete.create();

    private TransformingPositionDeleteWriter(
        PositionDeleteWriter<T> delegate, RowTransformer<T> transformer) {
      super(delegate);
      this.transformer = transformer;
    }

    @Override
    public void write(PositionDelete<T> row) {
      super.write(positionDelete.set(row.path(), row.pos(), transformer.transform(row.row())));
    }
  }

  private static class TransformingAppender<T> implements FileAppender<T> {
    private final FileAppender<T> delegate;
    private final RowTransformer<T> transformer;

    private TransformingAppender(FileAppender<T> delegate, RowTransformer<T> transformer) {
      this.delegate = delegate;
      this.transformer = transformer;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public void add(T datum) {
      delegate.add(transformer.transform(datum));
    }

    @Override
    public Metrics metrics() {
      return delegate.metrics();
    }

    @Override
    public long length() {
      return delegate.length();
    }

    @Override
    public List<Long> splitOffsets() {
      return delegate.splitOffsets();
    }
  }
}
