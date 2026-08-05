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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.iceberg.arrow.ArrowSchemaUtil;
import org.apache.iceberg.types.Types;

/**
 * Owns the Arrow {@link ListVector} construction state for {@link VectorizedListReader}.
 *
 * <p>Encapsulates allocation and per-list open/close semantics: {@link
 * ListVector#startNewValue(int)}, {@link ListVector#endValue(int, int)}, {@link
 * ListVector#setNull(int)}, plus the companion {@link IntVector} of list repetition levels and the
 * {@link NullabilityHolder} that tracks list-level nullability for the batch.
 *
 * <p>Isolating this state gives us a clean seam to swap the per-row {@code startNewValue} / {@code
 * endValue} protocol for a bulk-fill implementation that writes the offsets buffer directly from
 * Parquet rep/def-level arrays.
 *
 * <p>Not thread-safe.
 */
class VectorizedListBuilder implements AutoCloseable {

  private final Types.NestedField icebergField;
  private final BufferAllocator allocator;
  private final int definitionLevel;
  private final boolean isListRequired;
  private final boolean isElementRequired;

  private ListVector listVector;
  private IntVector listRepetitionLevels;
  private NullabilityHolder nullabilityHolder;

  // Per-batch state; reset by prepareBatch().
  private int listIndex;
  private int listSize;
  private int listDefinitionLevel;
  private int elementIndex;

  VectorizedListBuilder(
      Types.NestedField icebergField,
      BufferAllocator allocator,
      int definitionLevel,
      boolean isListRequired,
      boolean isElementRequired) {
    this.icebergField = icebergField;
    this.allocator = allocator;
    this.definitionLevel = definitionLevel;
    this.isListRequired = isListRequired;
    this.isElementRequired = isElementRequired;
  }

  /**
   * Prepares the builder for a new batch. Resets or reallocates the underlying Arrow vectors and
   * the {@link NullabilityHolder}, and resets all per-batch counters.
   *
   * @param numRowsToRead upper bound on top-level list rows this batch may produce
   * @param estimatedSize estimated total number of lists in this batch (from the Parquet
   *     repetition-level histogram); used to size the nullability holder and repetition-level
   *     vector
   */
  void prepareBatch(int numRowsToRead, int estimatedSize) {
    if (nullabilityHolder == null || nullabilityHolder.size() < estimatedSize) {
      nullabilityHolder = new NullabilityHolder(estimatedSize);
    } else {
      nullabilityHolder.reset();
    }

    if (listRepetitionLevels != null) {
      listRepetitionLevels.close();
    }
    listRepetitionLevels = new IntVector("repetition_levels", allocator);
    listRepetitionLevels.allocateNew(estimatedSize);

    if (listVector != null) {
      listVector.setValueCount(0);
      listVector.getDataVector().setValueCount(0);
    } else {
      listVector = ListVector.empty(icebergField.name(), allocator);
      listVector.initializeChildrenFromFields(ArrowSchemaUtil.convert(icebergField).getChildren());
      listVector.setInitialCapacity(numRowsToRead);
      listVector.allocateNew();
    }

    listIndex = -1;
    listSize = 0;
    listDefinitionLevel = -1;
    elementIndex = 0;
  }

  /** Child (data) vector of the underlying {@link ListVector}. */
  FieldVector childVector() {
    return listVector.getDataVector();
  }

  /** Current write position in the child vector; caller uses this as the destination index. */
  int nextElementIndex() {
    return elementIndex;
  }

  /**
   * Closes the currently open list, if any, using the definition level that was recorded when it
   * was opened. Idempotent: returns {@code false} without side effects when no list is currently
   * open (e.g. before the batch's first list has been opened).
   *
   * @return {@code true} if a list was actually closed, {@code false} otherwise
   */
  boolean closeCurrentList() {
    if (listIndex < 0) {
      return false;
    }
    closeList(listDefinitionLevel);
    return true;
  }

  /**
   * Opens a new list at the given repetition/definition level. The caller must ensure any
   * previously-open list was closed via {@link #closeCurrentList()} first.
   */
  void openNewList(int elementRepetitionLevel, int elementDefinitionLevel) {
    listDefinitionLevel = elementDefinitionLevel;
    listIndex++;
    listSize = 0;
    listVector.startNewValue(listIndex);
    listRepetitionLevels.setSafe(listIndex, elementRepetitionLevel);
  }

  /** Records that one non-null element was written at {@link #nextElementIndex()}. */
  void elementAppended() {
    listSize++;
    elementIndex++;
  }

  /**
   * Finalises the batch and returns the {@link VectorHolder} that wraps the built {@link
   * ListVector}, its list-level {@link NullabilityHolder}, and the repetition-level {@link
   * IntVector}.
   */
  VectorHolder buildResult() {
    listRepetitionLevels.setValueCount(listIndex + 1);
    listVector.setValueCount(listIndex + 1);
    return VectorHolder.vectorHolder(
        listVector, icebergField, nullabilityHolder, listRepetitionLevels);
  }

  @Override
  public void close() {
    if (listVector != null) {
      listVector.close();
      listVector = null;
    }
    if (listRepetitionLevels != null) {
      listRepetitionLevels.close();
      listRepetitionLevels = null;
    }
  }

  private void closeList(int closingDefinitionLevel) {
    int nullThreshold = definitionLevel - (isElementRequired ? 1 : 2);
    if (!isListRequired && closingDefinitionLevel < nullThreshold) {
      listVector.setNull(listIndex);
      nullabilityHolder.setNull(listIndex, closingDefinitionLevel);
    } else {
      listVector.endValue(listIndex, listSize);
      nullabilityHolder.setNotNull(listIndex, nullThreshold);
    }
  }
}
