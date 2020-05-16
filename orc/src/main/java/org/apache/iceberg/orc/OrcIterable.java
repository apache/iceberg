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
import java.util.Iterator;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

/**
 * Iterable used to read rows from ORC.
 */
class OrcIterable<T> extends CloseableGroup implements CloseableIterable<T> {
  private final Configuration config;
  private final Schema schema;
  private final InputFile file;
  private final Long start;
  private final Long length;
  private final Function<TypeDescription, OrcValueReader<?>> readerFunction;

  OrcIterable(InputFile file, Configuration config, Schema schema,
              Long start, Long length,
              Function<TypeDescription, OrcValueReader<?>> readerFunction) {
    this.schema = schema;
    this.readerFunction = readerFunction;
    this.file = file;
    this.start = start;
    this.length = length;
    this.config = config;
  }

  @SuppressWarnings("unchecked")
  @Override
  public CloseableIterator<T> iterator() {
    Reader orcFileReader = ORC.newFileReader(file, config);
    addCloseable(orcFileReader);
    TypeDescription readOrcSchema = ORCSchemaUtil.buildOrcProjection(schema, orcFileReader.getSchema());

    Iterator<T> iterator = new OrcIterator(
        newOrcIterator(file, readOrcSchema, start, length, orcFileReader),
        readerFunction.apply(readOrcSchema));
    return CloseableIterator.withClose(iterator);
  }

  private static VectorizedRowBatchIterator newOrcIterator(InputFile file,
                                                           TypeDescription readerSchema,
                                                           Long start, Long length,
                                                           Reader orcFileReader) {
    final Reader.Options options = orcFileReader.options();
    if (start != null) {
      options.range(start, length);
    }
    options.schema(readerSchema);

    try {
      return new VectorizedRowBatchIterator(file.location(), readerSchema, orcFileReader.rows(options));
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to get ORC rows for file: %s", file);
    }
  }

  private static class OrcIterator<T> implements Iterator<T> {

    private int nextRow;
    private VectorizedRowBatch current;

    private final VectorizedRowBatchIterator batchIter;
    private final OrcValueReader<T> reader;

    OrcIterator(VectorizedRowBatchIterator batchIter, OrcValueReader<T> reader) {
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
  }

}
