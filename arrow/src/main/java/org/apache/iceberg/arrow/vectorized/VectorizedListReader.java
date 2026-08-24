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
package org.apache.iceberg.arrow.vectorized;

import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.iceberg.parquet.TripleIterator;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

public class VectorizedListReader extends VectorizedArrowReader {

  private final VectorizedReader<VectorHolder> elementReader;
  private final ElementIterator elements = new ElementIterator();
  private final int repetitionLevel;
  private final int definitionLevel;
  private final boolean isElementRequired;
  private final ListVectorBuilder listBuilder;
  private long estimatedSize;
  private int batchSize = DEFAULT_BATCH_SIZE;

  public VectorizedListReader(
      ColumnDescriptor desc,
      Types.NestedField icebergField,
      boolean isListRequired,
      boolean isElementRequired,
      BufferAllocator rootAllocator,
      boolean setArrowValidityVector,
      VectorizedReader<VectorHolder> element) {
    super(desc, icebergField, rootAllocator, setArrowValidityVector);
    this.elementReader = element;
    this.repetitionLevel = desc.getMaxRepetitionLevel();
    this.definitionLevel = desc.getMaxDefinitionLevel();
    this.isElementRequired = isElementRequired;
    this.listBuilder =
        new ListVectorBuilder(
            icebergField, rootAllocator, definitionLevel, isListRequired, isElementRequired);
  }

  public VectorizedListReader(
      int repetitionLevel,
      int definitionLevel,
      boolean isListRequired,
      boolean isElementRequired,
      Types.NestedField icebergField,
      BufferAllocator rootAllocator,
      boolean setArrowValidityVector,
      VectorizedReader<VectorHolder> element) {
    super(icebergField);
    this.elementReader = element;
    this.repetitionLevel = repetitionLevel;
    this.definitionLevel = definitionLevel;
    this.isElementRequired = isElementRequired;
    this.listBuilder =
        new ListVectorBuilder(
            icebergField, rootAllocator, definitionLevel, isListRequired, isElementRequired);
  }

  @Override
  public void setBatchSize(int batchSize) {
    super.setBatchSize(batchSize);
    this.batchSize = (batchSize == 0) ? DEFAULT_BATCH_SIZE : batchSize;
    elementReader.setBatchSize(batchSize);
  }

  @Override
  public VectorHolder read(VectorHolder reuse, int numRowsToRead) {
    listBuilder.prepareBatch(numRowsToRead, (int) estimatedSize);
    int rowsRemaining = numRowsToRead;

    while (rowsRemaining > 0 && elements.hasNext()) {
      int elementRepetitionLevel = elements.currentRepetitionLevel();
      int elementDefinitionLevel = elements.currentDefinitionLevel();

      if (elementRepetitionLevel < repetitionLevel) { // new list
        if (listBuilder.endCurrentList()) { // no-op on the batch's first list
          if (elementRepetitionLevel == 0) {
            rowsRemaining--;
            if (rowsRemaining == 0) {
              break;
            }
          }
        }

        listBuilder.openNewList(elementRepetitionLevel, elementDefinitionLevel);
      }

      if (elementDefinitionLevel < definitionLevel) { // null value or empty list
        elements.nextNull();
        if (!isElementRequired && elementDefinitionLevel == definitionLevel - 1) { // null element
          listBuilder.writeNull();
        }
        // nothing to do if emptyList
      } else { // non-null element
        elements.writeNextElement(listBuilder);
      }
    }

    if (rowsRemaining > 0) {
      // EOF exit: close the last opened list using the def level recorded when it was started.
      listBuilder.endCurrentList();
    }

    return listBuilder.build();
  }

  @Override
  public void setRowGroupInfo(PageReadStore source, Map<ColumnPath, ColumnChunkMetaData> metadata) {
    // The element reader will read the page so it should be initialized
    elementReader.setRowGroupInfo(source, metadata);
    // Reset element iterator state for the new row group.
    elements.reset();
    ColumnDescriptor descriptor = columnDescriptor();
    if (descriptor != null) {
      ColumnChunkMetaData chunkMetaData = metadata.get(ColumnPath.get(descriptor.getPath()));
      List<Long> repetitionLevelHistogram =
          chunkMetaData.getSizeStatistics().getRepetitionLevelHistogram();
      if (!repetitionLevelHistogram.isEmpty()) {
        this.estimatedSize = 0;
        // Values less than the repetition level signals start of a new list
        for (int i = 0; i < repetitionLevel; i++) {
          this.estimatedSize += repetitionLevelHistogram.get(i);
        }

        return;
      }
    }

    this.estimatedSize = batchSize;
  }

  @Override
  public void close() {
    super.close();
    listBuilder.close();
    elementReader.close();
  }

  /**
   * Iterator over the Parquet element triples produced by the child element reader.
   *
   * <p>Hides the batching semantics of {@link VectorizedReader#read}: the underlying reader hands
   * back batches of triples, but callers want a per-triple view. The iterator fetches a fresh batch
   * on demand when the current one is exhausted, so callers only see a flat stream of triples.
   */
  private class ElementIterator<T> implements TripleIterator<T> {
    private VectorHolder currentBatch;
    private int currentOffset;
    private int currentBatchSize;

    @Override
    public boolean hasNext() {
      advance();
      return currentOffset < currentBatchSize;
    }

    @Override
    public int currentRepetitionLevel() {
      advance();
      return currentBatch.repetitionLevels().get(currentOffset);
    }

    @Override
    public int currentDefinitionLevel() {
      advance();
      return currentBatch.nullabilityHolder().definitionLevelAt(currentOffset);
    }

    void writeNextElement(ListVectorBuilder builder) {
      advance();
      builder.writeNonNullElement(currentBatch, currentOffset);
      this.currentOffset++;
    }

    @Override
    public T next() {
      throw new UnsupportedOperationException("Next is not supported, use setNext");
    }

    @Override
    public <N> N nextNull() {
      advance();
      this.currentOffset++;
      return null;
    }

    void reset() {
      this.currentBatch = null;
      this.currentOffset = 0;
      this.currentBatchSize = 0;
    }

    private void advance() {
      if (currentOffset < currentBatchSize) {
        return;
      }

      this.currentBatch = elementReader.read(currentBatch, batchSize);
      this.currentBatchSize = currentBatch.numValues();
      this.currentOffset = 0;
    }
  }
}
