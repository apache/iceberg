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
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Stream;
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

  @BeforeEach
  public void before() {
    columnVectors = mockColumnVector();
    deleteFilter = mock(DeleteFilter.class);
  }

  @Test
  public void testBuildRowIdMappingNoDeletes() {
    when(deleteFilter.hasPosDeletes()).thenReturn(true);
    PositionDeleteIndex deletedRowPos = mock(PositionDeleteIndex.class);

    for (long i = 0; i <= 10; i++) {
      when(deletedRowPos.isDeleted(i)).thenReturn(false);
    }

    when(deleteFilter.deletedRowPositions()).thenReturn(deletedRowPos);
    var rowIdMapping = ColumnarBatchUtil.buildRowIdMapping(columnVectors, deleteFilter, 0, 10);
    assertThat(rowIdMapping).isNull();
  }

  @Test
  public void testBuildRowIdMappingPositionDeletesOnly() {
    when(deleteFilter.hasPosDeletes()).thenReturn(true);
    PositionDeleteIndex deletedRowPos = mock(PositionDeleteIndex.class);

    // 5 position deletes
    for (long i = 98; i < 103; i++) {
      when(deletedRowPos.isDeleted(i)).thenReturn(true);
    }

    when(deleteFilter.deletedRowPositions()).thenReturn(deletedRowPos);

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

    PositionDeleteIndex deletedRowPos = mock(PositionDeleteIndex.class);
    when(deletedRowPos.isDeleted(1)).thenReturn(true); // 41
    when(deletedRowPos.isDeleted(4)).thenReturn(true); // 44
    when(deleteFilter.hasPosDeletes()).thenReturn(true);
    when(deleteFilter.deletedRowPositions()).thenReturn(deletedRowPos);

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

    PositionDeleteIndex deletedRowPos = mock(PositionDeleteIndex.class);
    when(deletedRowPos.isDeleted(1)).thenReturn(true);
    when(deletedRowPos.isDeleted(4)).thenReturn(true);
    when(deleteFilter.hasPosDeletes()).thenReturn(true);
    when(deleteFilter.deletedRowPositions()).thenReturn(deletedRowPos);

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

    PositionDeleteIndex deletedRowPos = mock(PositionDeleteIndex.class);
    when(deletedRowPos.isDeleted(0)).thenReturn(true); // 40
    when(deletedRowPos.isDeleted(1)).thenReturn(true); // 41
    when(deletedRowPos.isDeleted(4)).thenReturn(true); // 44
    when(deleteFilter.hasPosDeletes()).thenReturn(true);
    when(deleteFilter.deletedRowPositions()).thenReturn(deletedRowPos);

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
    PositionDeleteIndex deletedRowPos = mock(PositionDeleteIndex.class);
    when(deleteFilter.deletedRowPositions()).thenReturn(deletedRowPos);

    for (long i = 98; i < 100; i++) {
      when(deletedRowPos.isDeleted(i)).thenReturn(true);
    }

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

    PositionDeleteIndex deletedRowPos = mock(PositionDeleteIndex.class);
    when(deletedRowPos.isDeleted(1)).thenReturn(true); // 41
    when(deletedRowPos.isDeleted(4)).thenReturn(true); // 44
    when(deleteFilter.hasPosDeletes()).thenReturn(true);
    when(deleteFilter.deletedRowPositions()).thenReturn(deletedRowPos);

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
