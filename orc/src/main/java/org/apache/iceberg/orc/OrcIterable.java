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
import java.util.Set;
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
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.Pair;
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
  private NameMapping nameMapping;
  private final OrcRowFilter rowFilter;

  OrcIterable(InputFile file, Configuration config, Schema schema,
              NameMapping nameMapping, Long start, Long length,
              Function<TypeDescription, OrcRowReader<?>> readerFunction, boolean caseSensitive, Expression filter,
              Function<TypeDescription, OrcBatchReader<?>> batchReaderFunction, int recordsPerBatch,
              OrcRowFilter rowFilter) {
    this.schema = schema;
    this.readerFunction = readerFunction;
    this.file = file;
    this.nameMapping = nameMapping;
    this.start = start;
    this.length = length;
    this.config = config;
    this.caseSensitive = caseSensitive;
    this.filter = (filter == Expressions.alwaysTrue()) ? null : filter;
    this.batchReaderFunction = batchReaderFunction;
    this.recordsPerBatch = recordsPerBatch;
    this.rowFilter = rowFilter;
  }

  @SuppressWarnings("unchecked")
  @Override
  public CloseableIterator<T> iterator() {
    Reader orcFileReader = ORC.newFileReader(file, config);
    addCloseable(orcFileReader);

    TypeDescription fileSchema = orcFileReader.getSchema();
    final TypeDescription readOrcSchema;
    if (ORCSchemaUtil.hasIds(fileSchema)) {
      readOrcSchema = ORCSchemaUtil.buildOrcProjection(schema, fileSchema);
    } else {
      if (nameMapping == null) {
        nameMapping = MappingUtil.create(schema);
      }
      TypeDescription typeWithIds = ORCSchemaUtil.applyNameMapping(fileSchema, nameMapping);
      readOrcSchema = ORCSchemaUtil.buildOrcProjection(schema, typeWithIds);
    }

    SearchArgument sarg = null;
    if (filter != null) {
      Expression boundFilter = Binder.bind(schema.asStruct(), filter, caseSensitive);
      sarg = ExpressionToSearchArgument.convert(boundFilter, readOrcSchema);
    }

    if (rowFilter == null) {
      VectorizedRowBatchIterator rowBatchIterator = newOrcIterator(file, readOrcSchema, start, length, orcFileReader,
          sarg, recordsPerBatch);
      if (batchReaderFunction != null) {
        OrcBatchReader<T> batchReader = (OrcBatchReader<T>) batchReaderFunction.apply(readOrcSchema);
        return CloseableIterator.transform(rowBatchIterator, pair -> {
          batchReader.setBatchContext(pair.second());
          return batchReader.read(pair.first());
        });
      } else {
        return new OrcRowIterator<>(rowBatchIterator, (OrcRowReader<T>) readerFunction.apply(readOrcSchema),
            null, null);
      }
    } else {
      Preconditions.checkArgument(batchReaderFunction == null,
          "Row-level filtering not supported by vectorized reader");
      Set<Integer> filterColumnIds = TypeUtil.getProjectedIds(rowFilter.requiredSchema());
      Set<Integer> filterColumnIdsNotInReadSchema = Sets.difference(filterColumnIds,
          TypeUtil.getProjectedIds(schema));
      Schema extraFilterColumns = TypeUtil.select(rowFilter.requiredSchema(), filterColumnIdsNotInReadSchema);
      Schema finalReadSchema = TypeUtil.join(schema, extraFilterColumns);

      TypeDescription finalReadOrcSchema = ORCSchemaUtil.buildOrcProjection(finalReadSchema,
          orcFileReader.getSchema());
      TypeDescription rowFilterOrcSchema = ORCSchemaUtil.buildOrcProjection(rowFilter.requiredSchema(),
          orcFileReader.getSchema());
      RowFilterValueReader filterReader = new RowFilterValueReader(finalReadOrcSchema, rowFilterOrcSchema);

      return new OrcRowIterator<>(
          newOrcIterator(file, finalReadOrcSchema, start, length, orcFileReader, sarg, recordsPerBatch),
          (OrcRowReader<T>) readerFunction.apply(readOrcSchema), rowFilter, filterReader);
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

    private int currentRow;
    private VectorizedRowBatch currentBatch;
    private boolean advanced = false;

    private final VectorizedRowBatchIterator batchIter;
    private final OrcRowReader<T> reader;
    private final OrcRowFilter filter;
    private final RowFilterValueReader filterReader;

    OrcRowIterator(VectorizedRowBatchIterator batchIter, OrcRowReader<T> reader, OrcRowFilter filter,
        RowFilterValueReader filterReader) {
      this.batchIter = batchIter;
      this.reader = reader;
      this.filter = filter;
      this.filterReader = filterReader;
      currentBatch = null;
      currentRow = 0;
    }

    private void advance() {
      if (!advanced) {
        while (true) {
          currentRow++;
          // if batch has been consumed, move to next batch
          if (currentBatch == null || currentRow >= currentBatch.size) {
            if (batchIter.hasNext()) {
              Pair<VectorizedRowBatch, Long> nextBatch = batchIter.next();
              currentBatch = nextBatch.first();
              currentRow = 0;
              reader.setBatchContext(nextBatch.second());
              if (filterReader != null) {
                filterReader.setBatchContext(nextBatch.second());
              }
            } else {
              // no more batches left to process
              currentBatch = null;
              currentRow = -1;
              break;
            }
          }
          if (filter == null || filter.shouldKeep(filterReader.read(currentBatch, currentRow))) {
            // we have found our row
            break;
          }
        }
        advanced = true;
      }
    }

    @Override
    public boolean hasNext() {
      advance();
      return currentBatch != null;
    }

    @Override
    public T next() {
      advance();
      // mark current row as used
      advanced = false;
      return this.reader.read(currentBatch, currentRow);
    }

    @Override
    public void close() throws IOException {
      batchIter.close();
    }
  }
}
