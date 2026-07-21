/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.arrow.vectorized;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.iceberg.arrow.ArrowSchemaUtil;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

public class VectorizedListReader extends VectorizedArrowReader {

  private final VectorizedReader<VectorHolder> elementReader;
  private final ElementIterator elements = new ElementIterator();
  private NullabilityHolder nullabilityHolder;
  private final int repetitionLevel;
  private final int definitionLevel;
  private final boolean isListRequired;
  private final boolean isElementRequired;
  private ListVector listVector;
  private IntVector listRepetitionLevels;
  private long estimatedSize;

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
    this.isListRequired = isListRequired;
    this.isElementRequired = isElementRequired;
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
    this.isListRequired = isListRequired;
    this.rootAlloc = rootAllocator;
    this.isElementRequired = isElementRequired;
  }

  @Override
  public void setBatchSize(int batchSize) {
    this.batchSize = (batchSize == 0) ? DEFAULT_BATCH_SIZE : batchSize;
    elementReader.setBatchSize(batchSize);
  }

  @Override
  public VectorHolder read(VectorHolder reuse, int numRowsToRead) {
    if (nullabilityHolder == null || nullabilityHolder.size() < estimatedSize) {
      nullabilityHolder = new NullabilityHolder((int) estimatedSize);
    } else {
      nullabilityHolder.reset();
    }

    if (listRepetitionLevels != null) {
      listRepetitionLevels.close();
    }
    listRepetitionLevels = new IntVector("repetition_levels", rootAlloc);
    listRepetitionLevels.allocateNew((int) estimatedSize);

    if (listVector != null) {
      listVector.setValueCount(0);
      listVector.getDataVector().setValueCount(0);
    } else {
      listVector = ListVector.empty(icebergField.name(), rootAlloc);
      listVector.initializeChildrenFromFields(ArrowSchemaUtil.convert(icebergField).getChildren());
      listVector.setInitialCapacity(numRowsToRead);
      listVector.allocateNew();
    }

    int elementIndex = 0;
    FieldVector childVector = listVector.getDataVector();
    int listSize = 0;
    int listIndex = -1;
    int listRepetitionLevel = 0;
    int rowsRemaining = numRowsToRead;
    int listDefinitionLevel = -1;

    while (rowsRemaining > 0 && elements.hasNext()) {
      Element elem = elements.peek();
      int elementRepetitionLevel = elem.repetitionLevel();
      int elementDefinitionLevel = elem.definitionLevel();

      if (elementRepetitionLevel < repetitionLevel) { // new list
        if (listIndex >= 0) { // close the previous list
          listRepetitionLevels.setSafe(listIndex, listRepetitionLevel);
          if (!isListRequired
              && listDefinitionLevel < definitionLevel - (isElementRequired ? 1 : 2)) {
            listVector.setNull(listIndex);
            nullabilityHolder.setNull(listIndex, listDefinitionLevel);
          } else {
            listVector.endValue(listIndex, listSize);
            nullabilityHolder.setNotNull(listIndex, definitionLevel - (isElementRequired ? 1 : 2));
          }
          if (elementRepetitionLevel == 0) {
            rowsRemaining--;
          }
          if (rowsRemaining == 0) {
            // Do NOT consume `elem`: it starts the next list and must be re-processed
            // by the following read() call. peek() is idempotent, so leaving it here
            // preserves state.
            break;
          }
        }
        listDefinitionLevel = elementDefinitionLevel;
        listRepetitionLevel = elementRepetitionLevel;
        listIndex++;
        listSize = 0;
        listVector.startNewValue(listIndex);
      }

      // Consume the peeked element.
      elements.next();

      if (elem.isNull()) { // null value or empty list
        if (!isElementRequired && elementDefinitionLevel == definitionLevel - 1) {
          childVector.setNull(elementIndex++);
          listSize++;
        }
      } else { // non-null element
        setValue(childVector, elementIndex++, elem.vector(), elem.index());
        listSize++;
      }
    }

    if (listIndex >= 0) {
      listRepetitionLevels.setSafe(listIndex, listRepetitionLevel);
      if (rowsRemaining > 0) {
        // EOF exit: close the last opened list using the def level recorded when it was started.
        int nullThreshold = definitionLevel - (isElementRequired ? 1 : 2);
        if (!isListRequired && listDefinitionLevel < nullThreshold) {
          listVector.setNull(listIndex);
          nullabilityHolder.setNull(listIndex, listDefinitionLevel);
        } else {
          listVector.endValue(listIndex, listSize);
          nullabilityHolder.setNotNull(listIndex, nullThreshold);
        }
      }
    }
    listRepetitionLevels.setValueCount(listIndex + 1);
    listVector.setValueCount(listIndex + 1);

    return VectorHolder.vectorHolder(
        listVector, icebergField, nullabilityHolder, listRepetitionLevels);
  }

  @Override
  public void setRowGroupInfo(PageReadStore source, Map<ColumnPath, ColumnChunkMetaData> metadata) {
    // The element reader will read the page so it should be initialized
    elementReader.setRowGroupInfo(source, metadata);
    // Reset element iterator state for the new row group.
    elements.reset();
    if (columnDescriptor != null) {
      ColumnChunkMetaData chunkMetaData = metadata.get(ColumnPath.get(columnDescriptor.getPath()));
      List<Long> repetitionLevelHistogram =
          chunkMetaData.getSizeStatistics().getRepetitionLevelHistogram();
      if (!repetitionLevelHistogram.isEmpty()) {
        estimatedSize = 0;
        // Values less than the repetition level signals start of a new list
        for (int i = 0; i < repetitionLevel; i++) {
          estimatedSize += repetitionLevelHistogram.get(i);
        }
        return;
      }
    }
    estimatedSize = batchSize;
  }

  private static void setValue(
      FieldVector childVector, int elementIndex, FieldVector elementVector, int batchIndex) {
    childVector.copyFromSafe(batchIndex, elementIndex, elementVector);
  }

  @Override
  public void close() {
    super.close();
    if (listVector != null) {
      listVector.close();
      listVector = null;
    }
    if (listRepetitionLevels != null) {
      listRepetitionLevels.close();
      listRepetitionLevels = null;
    }
    elementReader.close();
  }

  /**
   * Iterator over the Parquet element triples produced by the child element reader.
   *
   * <p>Hides the batching semantics of {@link VectorizedReader#read}: the underlying reader hands
   * back batches of triples, but callers want a per-triple view. The iterator fetches a fresh batch
   * on demand when the current one is exhausted, so callers only see a flat stream of {@link
   * Element}s.
   *
   * <p>The iterator is stateful and lives across {@link VectorizedListReader#read} calls. When a
   * {@code read()} exits mid-batch because it filled the requested row budget, the next call
   * resumes from the same triple. {@link #peek()} is idempotent — repeated {@code peek()} calls
   * return the same element until {@link #next()} advances past it. This makes it easy to inspect a
   * triple (to decide whether it starts a new list) and then leave it unconsumed for the next
   * {@code read()} to pick up.
   */
  private class ElementIterator implements Iterator<Element> {
    private VectorHolder currentBatch;
    private int currentOffset;
    private Element peeked;

    @Override
    public boolean hasNext() {
      return peek() != null;
    }

    /**
     * Returns the current element without advancing the iterator. Idempotent: repeated calls
     * return the same element until {@link #next()} consumes it.
     */
    Element peek() {
      if (peeked != null) {
        return peeked;
      }
      if (!ensureBatch()) {
        return null;
      }
      peeked = new Element(currentBatch, currentOffset);
      return peeked;
    }

    @Override
    public Element next() {
      Element elem = peek();
      if (elem == null) {
        throw new NoSuchElementException();
      }
      peeked = null;
      currentOffset++;
      return elem;
    }

    void reset() {
      currentBatch = null;
      currentOffset = 0;
      peeked = null;
    }

    private boolean ensureBatch() {
      if (currentBatch != null && currentOffset < currentBatch.numValues()) {
        return true;
      }
      currentBatch = elementReader.read(currentBatch, batchSize);
      currentOffset = 0;
      return currentBatch != null && currentBatch.numValues() > 0;
    }
  }

  /**
   * A single Parquet triple (repetition level, definition level, value) surfaced by {@link
   * ElementIterator}. Lightweight view over a slot in the underlying element batch — reads are
   * looked up lazily so this stays cheap to create.
   */
  private static final class Element {
    private final VectorHolder batch;
    private final int index;

    Element(VectorHolder batch, int index) {
      this.batch = batch;
      this.index = index;
    }

    int repetitionLevel() {
      return batch.repetitionLevels().get(index);
    }

    int definitionLevel() {
      return batch.nullabilityHolder().definitionLevelAt(index);
    }

    boolean isNull() {
      return batch.nullabilityHolder().isNullAt(index) == 1;
    }

    FieldVector vector() {
      return batch.vector();
    }

    int index() {
      return index;
    }
  }
}