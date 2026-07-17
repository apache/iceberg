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

import java.util.List;
import java.util.Map;
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
  private NullabilityHolder nullabilityHolder;
  private final int repetitionLevel;
  private final int definitionLevel;
  private final boolean isListRequired;
  private final boolean isElementRequired;
  private ListVector listVector;
  private IntVector listRepetitionLevels;
  private long estimatedSize;

  //  private int batchSize = VectorizedArrowReader.DEFAULT_BATCH_SIZE;

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
    // Read all triples for this batch from the element reader.
    // numRowsToRead is the number of top-level rows (lists), but the element reader
    // will read more triples. We read one batch worth of triples from
    // the element reader; the repetition and definition levels tell us how many list rows that maps
    // to.

    if (nullabilityHolder == null || nullabilityHolder.size() < estimatedSize) {
      nullabilityHolder = new NullabilityHolder((int) estimatedSize);
    }
    if (listRepetitionLevels != null) {
      listRepetitionLevels.close();
    }
    listRepetitionLevels = new IntVector("repetition_levels", rootAlloc);
    listRepetitionLevels.allocateNew((int) estimatedSize);
    nullabilityHolder.reset();
    if (listVector == null) {
      listVector = ListVector.empty(icebergField.name(), rootAlloc);
      listVector.initializeChildrenFromFields(ArrowSchemaUtil.convert(icebergField).getChildren());
      listVector.setInitialCapacity((int) estimatedSize);
      listVector.allocateNew();
    } else {
      listVector.setValueCount(0);
      listVector.getDataVector().setValueCount(0);
    }
    int elementIndex = 0;
    FieldVector childVector = listVector.getDataVector();
    int listSize = 0;
    int listIndex = -1;
    int listRepetitionLevel = 0;
    int rowsRemaining = numRowsToRead;
    int lastDefinitionLevel = -1;
    do {
      VectorHolder elementHolder = elementReader.read(null, numRowsToRead);
      FieldVector elementVector = elementHolder.vector();
      IntVector elementRepetitionLevels = elementHolder.repetitionLevels();
      NullabilityHolder elementNullabilityHolder = elementHolder.nullabilityHolder();
      int elementCount = elementVector.getValueCount();
      if (elementCount == 0) {
        break;
      }
      for (int i = 0; i < elementCount; i++) {
        int currentRepetitionLevel = elementRepetitionLevels.get(i);
        int currentDefinitionLevel = elementNullabilityHolder.definitionLevelAt(i);
        if (currentRepetitionLevel < repetitionLevel) { // new list
          if (listIndex >= 0) { // The first 0 in repetition levels doesn't close a list
            // TODO can copy the values in batch at this point
            // TODO 2-level lists requires special handling
            listRepetitionLevels.setSafe(listIndex, listRepetitionLevel);
            listRepetitionLevel = currentRepetitionLevel;

            // The start of the list can be on the previous page
            int listDefinitionLevel =
                i == 0 ? lastDefinitionLevel : elementNullabilityHolder.definitionLevelAt(i - 1);
            if (!isListRequired
                && listDefinitionLevel
                    < definitionLevel - (isElementRequired ? 1 : 2)) { // null list
              listVector.setNull(listIndex);
              nullabilityHolder.setNull(listIndex, listDefinitionLevel);
            } else { // non-null list
              listVector.endValue(listIndex, listSize);
              nullabilityHolder.setNotNull(
                  listIndex, definitionLevel - (isElementRequired ? 1 : 2));
            }
            if (currentRepetitionLevel == 0) { // start of a new row
              rowsRemaining--;
            }
            if (rowsRemaining == 0) {
              break;
            }
          }

          // start a new list
          listIndex++;
          listSize = 0;
          listVector.startNewValue(listIndex);
        }

        if (elementNullabilityHolder.isNullAt(i) == 1) { // null value or empty List
          if (!isElementRequired && currentDefinitionLevel == definitionLevel - 1) { // null element
            childVector.setNull(elementIndex);
            elementIndex++;
            listSize++;
          }
        } else { // non-null list element
          setValue(childVector, elementIndex, elementVector, i);
          elementIndex++;
          listSize++;
        }
      }
      lastDefinitionLevel = elementNullabilityHolder.definitionLevelAt(elementCount - 1);
    } while (rowsRemaining > 0);

    if (listIndex >= 0) {
      listRepetitionLevels.setSafe(listIndex, listRepetitionLevel);
      if (rowsRemaining > 0) {
        // EOF exit: the last opened list was never closed inside the loop.
        // Use lastListStartDefinitionLevel to determine null vs non-null.
        int nullThreshold = definitionLevel - (isElementRequired ? 1 : 2);
        if (!isListRequired && lastDefinitionLevel < nullThreshold) {
          listVector.setNull(listIndex);
          nullabilityHolder.setNull(listIndex, lastDefinitionLevel);
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
}
