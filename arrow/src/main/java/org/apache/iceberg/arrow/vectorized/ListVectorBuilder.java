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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.iceberg.arrow.ArrowSchemaUtil;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

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
class ListVectorBuilder implements AutoCloseable {

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

  ListVectorBuilder(
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
      this.nullabilityHolder = new NullabilityHolder(estimatedSize);
    } else {
      nullabilityHolder.reset();
    }

    if (listRepetitionLevels != null) {
      listRepetitionLevels.close();
    }

    this.listRepetitionLevels = new IntVector("repetition_levels", allocator);
    listRepetitionLevels.allocateNew(estimatedSize);

    if (listVector != null) {
      listVector.setValueCount(0);
      listVector.getDataVector().setValueCount(0);
    } else {
      this.listVector = ListVector.empty(icebergField.name(), allocator);
      listVector.initializeChildrenFromFields(ArrowSchemaUtil.convert(icebergField).getChildren());
      listVector.setInitialCapacity(numRowsToRead);
      listVector.allocateNew();
    }

    this.listIndex = -1;
    this.listSize = 0;
    this.listDefinitionLevel = -1;
    this.elementIndex = 0;
  }

  /**
   * Opens a new list at the given repetition/definition level. The caller must ensure any
   * previously-open list was closed via {@link #endCurrentList()} first.
   */
  void openNewList(int elementRepetitionLevel, int elementDefinitionLevel) {
    this.listDefinitionLevel = elementDefinitionLevel;
    this.listIndex++;
    this.listSize = 0;
    listVector.startNewValue(listIndex);
    listRepetitionLevels.set(listIndex, elementRepetitionLevel);
  }

  public void writeNull() {
    listVector.getDataVector().setNull(elementIndex);
    this.listSize++;
    this.elementIndex++;
  }

  /**
   * Writes one element from an element-reader batch into the child vector at elementIndex. Handles
   * both plain and Parquet dictionary-encoded source batches: when {@code
   * sourceBatch.isDictionaryEncoded()} is {@code true}, the {@link IntVector} of dictionary IDs is
   * decoded through {@code sourceBatch.dictionary()} into the plain-typed child vector; otherwise
   * the value is copied directly via {@link FieldVector#copyFromSafe}.
   *
   * <p>The dispatch mirrors the {@code nextVal} implementations in {@code
   * VectorizedDictionaryEncodedParquetValuesReader} and the vector allocation done by {@code
   * VectorizedArrowReader#allocateFieldVector}, so that a list<T> read produces the same
   * materialized child regardless of the element column's encoding on disk.
   */
  void writeNonNullElement(VectorHolder sourceBatch, int sourceOffset) {
    FieldVector targetVector = listVector.getDataVector();
    if (!sourceBatch.isDictionaryEncoded()) {
      targetVector.copyFromSafe(sourceOffset, elementIndex, sourceBatch.vector());
      this.listSize++;
      this.elementIndex++;
      return;
    }

    IntVector dictIds = (IntVector) sourceBatch.vector();
    int dictId = dictIds.get(sourceOffset);
    Dictionary dictionary = sourceBatch.dictionary();
    PrimitiveType primitive = sourceBatch.descriptor().getPrimitiveType();
    LogicalTypeAnnotation logicalType = primitive.getLogicalTypeAnnotation();
    if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
      long micros = dictionary.decodeToLong(dictId);
      if (ts.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS) {
        micros *= 1000;
      }

      // The child vector is always allocated as BigIntVector for Iceberg timestamp types.
      ((BigIntVector) targetVector).setSafe(elementIndex, micros);
      return;
    }

    switch (primitive.getPrimitiveTypeName()) {
      case INT32 ->
          ((IntVector) targetVector).setSafe(elementIndex, dictionary.decodeToInt(dictId));
      case INT64 ->
          ((BigIntVector) targetVector).setSafe(elementIndex, dictionary.decodeToLong(dictId));
      case FLOAT ->
          ((Float4Vector) targetVector).setSafe(elementIndex, dictionary.decodeToFloat(dictId));
      case DOUBLE ->
          ((Float8Vector) targetVector).setSafe(elementIndex, dictionary.decodeToDouble(dictId));
      case BINARY -> {
        ByteBuffer buffer = dictionary.decodeToBinary(dictId).toByteBuffer();
        ((BaseVariableWidthVector) targetVector)
            .setSafe(elementIndex, buffer, buffer.position(), buffer.remaining());
      }
      case FIXED_LEN_BYTE_ARRAY -> {
        FixedSizeBinaryVector fixed = (FixedSizeBinaryVector) targetVector;
        byte[] bytes = dictionary.decodeToBinary(dictId).getBytesUnsafe();
        byte[] slot = new byte[fixed.getByteWidth()];
        System.arraycopy(bytes, 0, slot, 0, fixed.getByteWidth());
        fixed.setSafe(elementIndex, slot);
      }
      case INT96 -> {
        ByteBuffer int96 =
            dictionary.decodeToBinary(dictId).toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
        ((BigIntVector) targetVector)
            .setSafe(elementIndex, ParquetUtil.extractTimestampInt96(int96));
      }
      default ->
          throw new UnsupportedOperationException(
              "Unsupported dictionary-encoded element type: " + primitive);
    }

    this.listSize++;
    this.elementIndex++;
  }

  /**
   * Ends the currently open list, if any, using the definition level that was recorded when it was
   * opened.
   *
   * @return {@code true} if a list was actually closed, {@code false} otherwise it happens the
   *     first list of the batch
   */
  boolean endCurrentList() {
    if (listIndex < 0) {
      return false;
    }

    int nullThreshold = definitionLevel - (isElementRequired ? 1 : 2);
    if (!isListRequired && listDefinitionLevel < nullThreshold) {
      listVector.setNull(listIndex);
      nullabilityHolder.setNull(listIndex, listDefinitionLevel);
    } else {
      listVector.endValue(listIndex, listSize);
      nullabilityHolder.setNotNull(listIndex, nullThreshold);
    }

    return true;
  }

  /**
   * Finalises the batch and returns the {@link VectorHolder} that wraps the built {@link
   * ListVector}, its list-level {@link NullabilityHolder}, and the repetition-level {@link
   * IntVector}.
   */
  VectorHolder build() {
    listRepetitionLevels.setValueCount(listIndex + 1);
    listVector.setValueCount(listIndex + 1);
    return VectorHolder.vectorHolder(
        listVector, icebergField, nullabilityHolder, listRepetitionLevels);
  }

  @Override
  public void close() {
    if (listVector != null) {
      listVector.close();
      this.listVector = null;
    }

    if (listRepetitionLevels != null) {
      listRepetitionLevels.close();
      this.listRepetitionLevels = null;
    }
  }
}
