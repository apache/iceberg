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
package org.apache.iceberg.spark.data.vectorized;

import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestColumnarBatchUtil {

  private ColumnVector[] columnVectors;
  private DeleteFilter deleteFilter;
  PositionDeleteIndex deletedRowPos;
  Predicate<InternalRow> eqDeleteFilter;

  @BeforeEach
  public void setup() {
    columnVectors = new ColumnVector[2];
    columnVectors[0] = mock(ColumnVector.class);
    columnVectors[1] = mock(ColumnVector.class);
    deleteFilter = mock(DeleteFilter.class);
    when(deleteFilter.hasPosDeletes()).thenReturn(true);
    when(deleteFilter.hasEqDeletes()).thenReturn(true);
    deletedRowPos = mock(PositionDeleteIndex.class);
    eqDeleteFilter = mock(Predicate.class);
  }

  @Test
  public void testBuildRowIdMappingNoDeletes() {

    for (long i = 0; i <= 10; i++) {
      when(deletedRowPos.isDeleted(i)).thenReturn(false);
    }

    when(eqDeleteFilter.test(any(InternalRow.class))).thenReturn(true);

    when(deleteFilter.deletedRowPositions()).thenReturn(deletedRowPos);
    when(deleteFilter.eqDeletedRowFilter()).thenReturn(eqDeleteFilter);
    var rowIdMapping = ColumnarBatchUtil.buildRowIdMapping(columnVectors, deleteFilter, 0, 10);
    assertThat(rowIdMapping).isNull();
  }

  @Test
  public void testBuildRowIdMapping() {
    mockEqAndPosDeletes();
    var rowIdMapping = ColumnarBatchUtil.buildRowIdMapping(columnVectors, deleteFilter, 0, 100);
    assertThat(rowIdMapping).isNotNull();

    int[] rowIds = (int[]) rowIdMapping.first();
    int liveRows = (int) rowIdMapping.second();

    for (int id : rowIds) {
      assertThat(id < 90).isTrue();
    }

    assertThat(rowIds.length).isEqualTo(100);
    assertThat(liveRows).isEqualTo(90);
  }

  @Test
  void testBuildIsDeletedWithDeletedRange() {
    mockEqAndPosDeletes();
    var isDeleted = ColumnarBatchUtil.buildIsDeleted(columnVectors, deleteFilter, 0, 100);

    assertThat(isDeleted).isNotNull();
    assertThat(isDeleted.length).isEqualTo(100);

    for (int i = 0; i < 100; i++) {
      if (i > 89) {
        assertThat(isDeleted[i]).isTrue();
      } else {
        assertThat(isDeleted[i]).isFalse();
      }
    }
  }

  @Test
  void testBuildIsDeletedNoDeletes() {
    var result = ColumnarBatchUtil.buildIsDeleted(columnVectors, null, 0, 5);
    assertThat(result).isNotNull();
    for (int i = 0; i < 5; i++) {
      assertThat(result[i]).isFalse();
    }
  }

  @Test
  void testRemoveExtraColumns() {
    ColumnVector[] vectors = new ColumnVector[5];
    for (int i = 0; i < 5; i++) {
      vectors[i] = mock(ColumnVector.class);
    }
    when(deleteFilter.expectedSchema()).thenReturn(mock(Schema.class));
    when(deleteFilter.expectedSchema().columns()).thenReturn(nCopies(3, null));

    ColumnVector[] result = ColumnarBatchUtil.removeExtraColumns(deleteFilter, vectors);
    assertThat(result.length).isEqualTo(3);
  }

  @Test
  void testRemoveExtraColumnsNotNeeded() {
    ColumnVector[] vectors = new ColumnVector[3];
    for (int i = 0; i < 3; i++) {
      vectors[i] = mock(ColumnVector.class);
    }
    when(deleteFilter.expectedSchema()).thenReturn(mock(Schema.class));
    when(deleteFilter.expectedSchema().columns()).thenReturn(nCopies(3, null));

    ColumnVector[] result = ColumnarBatchUtil.removeExtraColumns(deleteFilter, vectors);
    assertThat(result.length).isEqualTo(3);
  }

  private void mockEqAndPosDeletes() {
    // 5 position deletes
    for (long i = 95; i < 100; i++) {
      when(deletedRowPos.isDeleted(i)).thenReturn(true);
    }

    // 5 equality deletes
    AtomicInteger rowIdCounter = new AtomicInteger(-1);
    when(eqDeleteFilter.test(any(InternalRow.class)))
        .thenAnswer(
            invocation -> {
              int rowId = rowIdCounter.incrementAndGet();
              return !(rowId == 90 || rowId == 91 || rowId == 92 || rowId == 93 || rowId == 94);
            });

    when(deleteFilter.deletedRowPositions()).thenReturn(deletedRowPos);
    when(deleteFilter.eqDeletedRowFilter()).thenReturn(eqDeleteFilter);
  }
}
