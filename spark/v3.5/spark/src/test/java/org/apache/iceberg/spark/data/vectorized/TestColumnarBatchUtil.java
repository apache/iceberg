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

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestColumnarBatchUtil {

  private ColumnVector[] columnVectors;
  private DeleteFilter deleteFilter;

  @BeforeEach
  public void before() {
    columnVectors = new ColumnVector[2];
    columnVectors[0] = mock(ColumnVector.class);
    columnVectors[1] = mock(ColumnVector.class);
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
  public void testBuildRowIdMapping() {
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
  void testBuildIsDeletedWithDeletedRange() {
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
    assertThat(isDeleted[97]).isFalse();
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
}
