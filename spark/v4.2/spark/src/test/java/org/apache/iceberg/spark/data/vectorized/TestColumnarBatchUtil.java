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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestColumnarBatchUtil {

  private ColumnVector[] columnVectors;
  private DeleteFilter deleteFilter;

  @BeforeEach
  public void before() {
    columnVectors = mockColumnVector();
    deleteFilter = mock(DeleteFilter.class);
  }

  @Test
  public void testBuildRowIdMappingNoDeletes() {
    when(deleteFilter.hasPosDeletes()).thenReturn(true);
    when(deleteFilter.deletedRowPositions()).thenReturn(positionIndex());
    var rowIdMapping = ColumnarBatchUtil.buildRowIdMapping(columnVectors, deleteFilter, 0, 10);
    assertThat(rowIdMapping).isNull();
  }

  @Test
  public void testBuildRowIdMappingPositionDeletesOnly() {
    when(deleteFilter.hasPosDeletes()).thenReturn(true);
    // 5 position deletes
    when(deleteFilter.deletedRowPositions()).thenReturn(positionIndex(98, 99, 100, 101, 102));

    var rowIdMapping = ColumnarBatchUtil.buildRowIdMapping(columnVectors, deleteFilter, 0, 200);
    assertThat(rowIdMapping).isNotNull();

    int[] rowIds = (int[]) rowIdMapping.first();
    int liveRows = (int) rowIdMapping.second();

    for (int id : rowIds) {
      assertThat(id < 98 || id > 102).isTrue();
    }

    assertThat(rowIds.length).isEqualTo(200);
    assertThat(liveRows).isEqualTo(195);
  }

  @Test
  public void testBuildRowIdMappingEqualityDeletesOnly() {
    // Define raw equality delete predicate — delete rows where value == 42
    Predicate<InternalRow> rawEqDelete = row -> row.getInt(0) == 42;

    // Mimic real eqDeletedRowFilter(): keep row only if it does NOT match delete condition
    Predicate<InternalRow> eqDeletePredicate =
        Stream.of(rawEqDelete).map(Predicate::negate).reduce(Predicate::and).orElse(t -> true);

    // Mock DeleteFilter
    when(deleteFilter.hasPosDeletes()).thenReturn(false);
    when(deleteFilter.deletedRowPositions()).thenReturn(null);
    when(deleteFilter.eqDeletedRowFilter()).thenReturn(eqDeletePredicate);

    var rowIdMapping = ColumnarBatchUtil.buildRowIdMapping(columnVectors, deleteFilter, 0, 5);

    assertThat(rowIdMapping).isNotNull();
    int[] rowIds = (int[]) rowIdMapping.first();
    int liveRows = (Integer) rowIdMapping.second();

    // Expect to keep positions 0, 1, 3, 4 → values 40, 41, 43, 44
    assertThat(liveRows).isEqualTo(4);
    assertThat(Arrays.copyOf(rowIds, liveRows)).containsExactly(0, 1, 3, 4);
  }

  @Test
  public void testBuildRowIdMappingPositionAndEqualityDeletes() {

    // Define raw equality delete predicate — delete rows where value == 42
    Predicate<InternalRow> rawEqDelete = row -> row.getInt(0) == 42;

    // Mimic real eqDeletedRowFilter(): keep row only if it does NOT match delete condition
    Predicate<InternalRow> eqDeletePredicate =
        Stream.of(rawEqDelete).map(Predicate::negate).reduce(Predicate::and).orElse(t -> true);
    when(deleteFilter.eqDeletedRowFilter()).thenReturn(eqDeletePredicate);

    when(deleteFilter.hasPosDeletes()).thenReturn(true);
    when(deleteFilter.hasEqDeletes()).thenReturn(true);
    when(deleteFilter.deletedRowPositions()).thenReturn(positionIndex(1, 4)); // 41 and 44

    var rowIdMapping = ColumnarBatchUtil.buildRowIdMapping(columnVectors, deleteFilter, 0, 5);

    assertThat(rowIdMapping).isNotNull();
    int[] rowIds = (int[]) rowIdMapping.first();
    int liveRows = (Integer) rowIdMapping.second();

    assertThat(liveRows).isEqualTo(2);
    assertThat(Arrays.copyOf(rowIds, liveRows)).containsExactly(0, 3);
  }

  @Test
  void testBuildRowIdMappingEmptyColumVectors() {
    ColumnVector[] columnVectorsZero = new ColumnVector[0];

    when(deleteFilter.hasPosDeletes()).thenReturn(true);
    when(deleteFilter.deletedRowPositions()).thenReturn(positionIndex(1, 4));

    var rowIdMapping = ColumnarBatchUtil.buildRowIdMapping(columnVectorsZero, deleteFilter, 0, 0);

    // Empty batch size, expect no rows deleted.
    assertThat(rowIdMapping).isNull();
  }

  @Test
  void testBuildRowIdMapAllRowsDeleted() {

    // Define raw equality delete predicate — delete rows where value == 42 or 43
    Predicate<InternalRow> rawEqDelete = row -> row.getInt(0) == 42 || row.getInt(0) == 43;

    // Mimic real eqDeletedRowFilter(): keep row only if it does NOT match delete condition
    Predicate<InternalRow> eqDeletePredicate =
        Stream.of(rawEqDelete).map(Predicate::negate).reduce(Predicate::and).orElse(t -> true);
    when(deleteFilter.eqDeletedRowFilter()).thenReturn(eqDeletePredicate);

    when(deleteFilter.hasPosDeletes()).thenReturn(true);
    when(deleteFilter.hasEqDeletes()).thenReturn(true);
    when(deleteFilter.deletedRowPositions()).thenReturn(positionIndex(0, 1, 4)); // 40, 41, 44

    var rowIdMapping = ColumnarBatchUtil.buildRowIdMapping(columnVectors, deleteFilter, 0, 5);

    assertThat(rowIdMapping).isNotNull();
    int[] rowIds = (int[]) rowIdMapping.first();
    int liveRows = (Integer) rowIdMapping.second();

    // Expect all rows to be deleted
    assertThat(liveRows).isEqualTo(0);
    assertThat(rowIds).containsExactly(0, 0, 0, 0, 0);
  }

  @Test
  void testBuildIsDeletedPositionDeletes() {
    when(deleteFilter.deletedRowPositions()).thenReturn(positionIndex(98, 99));

    var isDeleted = ColumnarBatchUtil.buildIsDeleted(columnVectors, deleteFilter, 0, 100);

    assertThat(isDeleted).isNotNull();
    assertThat(isDeleted.length).isEqualTo(100);

    for (int i = 98; i < 100; i++) {
      assertThat(isDeleted[i]).isTrue();
    }

    for (int i = 0; i < 98; i++) {
      assertThat(isDeleted[i]).isFalse();
    }
  }

  @Test
  void testBuildIsDeletedEqualityDeletes() {
    // Define raw equality delete predicate — delete rows where value == 42 or 43
    Predicate<InternalRow> rawEqDelete = row -> row.getInt(0) == 42 || row.getInt(0) == 43;

    // Mimic real eqDeletedRowFilter(): keep row only if it does NOT match delete condition
    Predicate<InternalRow> eqDeletePredicate =
        Stream.of(rawEqDelete).map(Predicate::negate).reduce(Predicate::and).orElse(t -> true);
    when(deleteFilter.eqDeletedRowFilter()).thenReturn(eqDeletePredicate);

    var isDeleted = ColumnarBatchUtil.buildIsDeleted(columnVectors, deleteFilter, 0, 5);

    for (int i = 0; i < isDeleted.length; i++) {
      if (i == 2 || i == 3) { // 42 and 43
        assertThat(isDeleted[i]).isTrue();
      } else {
        assertThat(isDeleted[i]).isFalse();
      }
    }
  }

  @Test
  void testBuildIsDeletedPositionAndEqualityDeletes() {
    // Define raw equality delete predicate — delete rows where value == 42
    Predicate<InternalRow> rawEqDelete = row -> row.getInt(0) == 42;

    // Mimic real eqDeletedRowFilter(): keep row only if it does NOT match delete condition
    Predicate<InternalRow> eqDeletePredicate =
        Stream.of(rawEqDelete).map(Predicate::negate).reduce(Predicate::and).orElse(t -> true);
    when(deleteFilter.eqDeletedRowFilter()).thenReturn(eqDeletePredicate);

    when(deleteFilter.hasPosDeletes()).thenReturn(true);
    when(deleteFilter.hasEqDeletes()).thenReturn(true);
    when(deleteFilter.deletedRowPositions()).thenReturn(positionIndex(1, 4)); // 41 and 44

    var isDeleted = ColumnarBatchUtil.buildIsDeleted(columnVectors, deleteFilter, 0, 5);

    for (int i = 0; i < isDeleted.length; i++) {
      if (i == 0 || i == 3) {
        assertThat(isDeleted[i]).isFalse();
      } else {
        assertThat(isDeleted[i]).isTrue(); // 42, 41, 44 are deleted
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

  @Test
  void testBuildRowIdMappingNonZeroBatchStart() {
    // batches after the first one start at a non-zero position in the file
    long rowStartPosInBatch = 1_000_000L;
    when(deleteFilter.hasPosDeletes()).thenReturn(true);
    when(deleteFilter.deletedRowPositions())
        .thenReturn(positionIndex(1_000_000L, 1_000_003L, 1_000_009L));

    var rowIdMapping =
        ColumnarBatchUtil.buildRowIdMapping(columnVectors, deleteFilter, rowStartPosInBatch, 10);

    assertThat(rowIdMapping).isNotNull();
    int[] rowIds = (int[]) rowIdMapping.first();
    int liveRows = (Integer) rowIdMapping.second();

    assertThat(liveRows).isEqualTo(7);
    assertThat(Arrays.copyOf(rowIds, liveRows)).containsExactly(1, 2, 4, 5, 6, 7, 8);
    verify(deleteFilter, times(3)).incrementDeleteCount();
  }

  @Test
  void testBuildRowIdMappingDeletesOutsideBatch() {
    // the index covers the whole file, so most batches see no deletes at all
    when(deleteFilter.hasPosDeletes()).thenReturn(true);
    when(deleteFilter.deletedRowPositions()).thenReturn(positionIndex(5L, 6L, 500L, 501L));

    var rowIdMapping = ColumnarBatchUtil.buildRowIdMapping(columnVectors, deleteFilter, 100L, 100);

    assertThat(rowIdMapping).isNull();
    verify(deleteFilter, times(0)).incrementDeleteCount();
  }

  @Test
  void testBuildRowIdMappingAllRowsDeletedByPosition() {
    when(deleteFilter.hasPosDeletes()).thenReturn(true);
    when(deleteFilter.deletedRowPositions()).thenReturn(positionIndex(0L, 1L, 2L, 3L, 4L));

    var rowIdMapping = ColumnarBatchUtil.buildRowIdMapping(columnVectors, deleteFilter, 0, 5);

    assertThat(rowIdMapping).isNotNull();
    assertThat((Integer) rowIdMapping.second()).isEqualTo(0);
    verify(deleteFilter, times(5)).incrementDeleteCount();
  }

  @Test
  void testBuildIsDeletedNonZeroBatchStart() {
    when(deleteFilter.deletedRowPositions()).thenReturn(positionIndex(1_000_002L, 1_000_004L));

    var isDeleted = ColumnarBatchUtil.buildIsDeleted(columnVectors, deleteFilter, 1_000_000L, 5);

    assertThat(isDeleted).containsExactly(false, false, true, false, true);
    verify(deleteFilter, times(2)).incrementDeleteCount();
  }

  @Test
  void testPositionOnlyPathMatchesPerRowPath() {
    // the range traversal must agree with a straightforward per-row probe on every batch
    java.util.Random random = new java.util.Random(20260818L);
    long fileSize = 300_000L;
    List<Long> deletedPositions = Lists.newArrayList();
    for (long pos = 0; pos < fileSize; pos++) {
      if (random.nextInt(1000) < 7) {
        deletedPositions.add(pos);
      }
    }

    PositionDeleteIndex index = positionIndex(deletedPositions);
    int batchSize = 5000;

    for (long batchStart = 0; batchStart < fileSize; batchStart += batchSize) {
      DeleteFilter<InternalRow> filter = mock(DeleteFilter.class);
      when(filter.deletedRowPositions()).thenReturn(index);

      var rowIdMapping =
          ColumnarBatchUtil.buildRowIdMapping(columnVectors, filter, batchStart, batchSize);

      int[] expected = new int[batchSize];
      int expectedLiveRows = 0;
      for (int rowId = 0; rowId < batchSize; rowId++) {
        if (!index.isDeleted(batchStart + rowId)) {
          expected[expectedLiveRows] = rowId;
          expectedLiveRows++;
        }
      }

      if (expectedLiveRows == batchSize) {
        assertThat(rowIdMapping).as("batch at %s", batchStart).isNull();
      } else {
        assertThat(rowIdMapping).as("batch at %s", batchStart).isNotNull();
        assertThat((Integer) rowIdMapping.second()).isEqualTo(expectedLiveRows);
        assertThat(Arrays.copyOf((int[]) rowIdMapping.first(), expectedLiveRows))
            .as("batch at %s", batchStart)
            .containsExactly(Arrays.copyOf(expected, expectedLiveRows));
        verify(filter, times(batchSize - expectedLiveRows)).incrementDeleteCount();
      }
    }
  }

  private static PositionDeleteIndex positionIndex(long... positions) {
    List<Long> list = Lists.newArrayList();
    for (long position : positions) {
      list.add(position);
    }
    return positionIndex(list);
  }

  private static PositionDeleteIndex positionIndex(List<Long> positions) {
    return Deletes.toPositionIndex(CloseableIterable.withNoopClose(positions));
  }

  private ColumnVector[] mockColumnVector() {
    // Create a mocked Int column vector with values: 40, 41, 42, 43, 44
    ColumnVector intVector = mock(ColumnVector.class);
    when(intVector.getInt(0)).thenReturn(40);
    when(intVector.getInt(1)).thenReturn(41);
    when(intVector.getInt(2)).thenReturn(42);
    when(intVector.getInt(3)).thenReturn(43);
    when(intVector.getInt(4)).thenReturn(44);

    return new ColumnVector[] {intVector};
  }
}
