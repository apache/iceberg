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

package org.apache.iceberg.orc;

import java.io.IOException;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.storage.ql.io.sarg.SearchArgument;

/**
 * Iterable used to read rows from ORC.
 */
class OrcIterable<T> extends CloseableGroup implements CloseableIterable<T> {
  private final Configuration config;
  private final Schema schema;
  private final InputFile file;
  private final Long start;
  private final Long length;
  private final Function<TypeDescription, OrcRowReader<?>> readerFunction;
  private final Expression filter;
  private final boolean caseSensitive;
  private final Function<TypeDescription, OrcBatchReader<?>> batchReaderFunction;
  private final int recordsPerBatch;

  OrcIterable(InputFile file, Configuration config, Schema schema,
              Long start, Long length,
              Function<TypeDescription, OrcRowReader<?>> readerFunction, boolean caseSensitive, Expression filter,
              Function<TypeDescription, OrcBatchReader<?>> batchReaderFunction, int recordsPerBatch) {
    this.schema = schema;
    this.readerFunction = readerFunction;
    this.file = file;
    this.start = start;
    this.length = length;
    this.config = config;
    this.caseSensitive = caseSensitive;
    this.filter = (filter == Expressions.alwaysTrue()) ? null : filter;
    this.batchReaderFunction = batchReaderFunction;
    this.recordsPerBatch = recordsPerBatch;
  }

  @SuppressWarnings("unchecked")
  @Override
  public CloseableIterator<T> iterator() {
    Reader orcFileReader = ORC.newFileReader(file, config);
    addCloseable(orcFileReader);
    TypeDescription readOrcSchema = ORCSchemaUtil.buildOrcProjection(schema, orcFileReader.getSchema());

    SearchArgument sarg = null;
    if (filter != null) {
      Expression boundFilter = Binder.bind(schema.asStruct(), filter, caseSensitive);
      sarg = ExpressionToSearchArgument.convert(boundFilter, readOrcSchema);
    }

    VectorizedRowBatchIterator rowBatchIterator = newOrcIterator(file, readOrcSchema, start, length, orcFileReader,
        sarg, recordsPerBatch);
    if (batchReaderFunction != null) {
      OrcBatchReader<T> batchReader = (OrcBatchReader<T>) batchReaderFunction.apply(readOrcSchema);
      return CloseableIterator.transform(rowBatchIterator, batchReader::read);
    } else {
      return new OrcRowIterator<>(rowBatchIterator, (OrcRowReader<T>) readerFunction.apply(readOrcSchema));
    }
  }

  private static VectorizedRowBatchIterator newOrcIterator(InputFile file,
                                                           TypeDescription readerSchema,
                                                           Long start, Long length,
                                                           Reader orcFileReader, SearchArgument sarg,
                                                           int recordsPerBatch) {
    final Reader.Options options = orcFileReader.options();
    if (start != null) {
      options.range(start, length);
    }
    options.schema(readerSchema);
    options.searchArgument(sarg, new String[]{});

    try {
      return new VectorizedRowBatchIterator(file.location(), readerSchema, orcFileReader.rows(options),
          recordsPerBatch);
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to get ORC rows for file: %s", file);
    }
  }

  private static class OrcRowIterator<T> implements CloseableIterator<T> {

    private int nextRow;
    private VectorizedRowBatch current;

    private final VectorizedRowBatchIterator batchIter;
    private final OrcRowReader<T> reader;

    OrcRowIterator(VectorizedRowBatchIterator batchIter, OrcRowReader<T> reader) {
      this.batchIter = batchIter;
      this.reader = reader;
      current = null;
      nextRow = 0;
    }

    @Override
    public boolean hasNext() {
      return (current != null && nextRow < current.size) || batchIter.hasNext();
    }

    @Override
    public T next() {
      if (current == null || nextRow >= current.size) {
        current = batchIter.next();
        nextRow = 0;
      }

      return this.reader.read(current, nextRow++);
    }

    @Override
    public void close() throws IOException {
      batchIter.close();
    }
  }
}
