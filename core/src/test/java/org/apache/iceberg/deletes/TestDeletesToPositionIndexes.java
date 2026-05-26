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
package org.apache.iceberg.deletes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceMap;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestDeletesToPositionIndexes {

  @Test
  public void emptyInputProducesEmptyMap() {
    CharSequenceMap<PositionDeleteIndex> indexes = toIndexes(Lists.newArrayList());
    assertThat(indexes.keySet()).isEmpty();
    assertThat(indexes.get("file_a.parquet")).isNull();
  }

  @Test
  public void singlePathCoalescesContiguousRun() {
    List<StructLike> deletes = Lists.newArrayList();
    for (long pos = 0; pos < 1024; pos++) {
      deletes.add(Row.of("file_a.parquet", pos));
    }

    CharSequenceMap<PositionDeleteIndex> indexes = toIndexes(deletes);

    assertThat(indexes.keySet()).hasSize(1);
    PositionDeleteIndex index = indexes.get("file_a.parquet");
    assertThat(index).as("index for file_a.parquet").isNotNull();
    assertThat(index).isInstanceOf(BitmapPositionDeleteIndex.class);
    for (long pos = 0; pos < 1024; pos++) {
      assertThat(index.isDeleted(pos)).as("pos %s should be deleted", pos).isTrue();
    }
    assertThat(index.isDeleted(1024L)).isFalse();
  }

  @Test
  public void multiplePathsBuildIndependentIndexes() {
    List<StructLike> deletes =
        Lists.newArrayList(
            Row.of("file_a.parquet", 0L),
            Row.of("file_a.parquet", 1L),
            Row.of("file_a.parquet", 5L),
            Row.of(new Utf8("file_b.parquet"), 7L),
            Row.of("file_b.parquet", 8L),
            Row.of("file_b.parquet", 9L),
            Row.of("file_c.parquet", 42L));

    CharSequenceMap<PositionDeleteIndex> indexes = toIndexes(deletes);

    assertThat(indexes.keySet()).hasSize(3);

    PositionDeleteIndex indexA = indexes.get("file_a.parquet");
    assertThat(indexA).as("index for file_a.parquet").isNotNull();
    assertThat(indexA.isDeleted(0L)).isTrue();
    assertThat(indexA.isDeleted(1L)).isTrue();
    assertThat(indexA.isDeleted(2L)).isFalse();
    assertThat(indexA.isDeleted(5L)).isTrue();
    assertThat(indexA.isDeleted(7L)).isFalse();

    PositionDeleteIndex indexB = indexes.get("file_b.parquet");
    assertThat(indexB).as("index for file_b.parquet").isNotNull();
    assertThat(indexB.isDeleted(7L)).isTrue();
    assertThat(indexB.isDeleted(8L)).isTrue();
    assertThat(indexB.isDeleted(9L)).isTrue();
    assertThat(indexB.isDeleted(10L)).isFalse();

    PositionDeleteIndex indexC = indexes.get("file_c.parquet");
    assertThat(indexC).as("index for file_c.parquet").isNotNull();
    assertThat(indexC.isDeleted(42L)).isTrue();
    assertThat(indexC.isDeleted(0L)).isFalse();
  }

  @Test
  public void outOfOrderPathRevisitMergesIntoExistingIndex() {
    // Unsorted input: same data file appears on either side of a different path. The implementation
    // must still produce a single bitmap for file_a containing positions from both runs.
    List<StructLike> deletes =
        Lists.newArrayList(
            Row.of("file_a.parquet", 1L),
            Row.of("file_a.parquet", 2L),
            Row.of("file_b.parquet", 100L),
            Row.of("file_a.parquet", 50L),
            Row.of("file_a.parquet", 51L));

    CharSequenceMap<PositionDeleteIndex> indexes = toIndexes(deletes);

    assertThat(indexes.keySet()).hasSize(2);

    PositionDeleteIndex indexA = indexes.get("file_a.parquet");
    assertThat(indexA).as("index for file_a.parquet").isNotNull();
    assertThat(indexA.isDeleted(1L)).isTrue();
    assertThat(indexA.isDeleted(2L)).isTrue();
    assertThat(indexA.isDeleted(50L)).isTrue();
    assertThat(indexA.isDeleted(51L)).isTrue();
    assertThat(indexA.isDeleted(3L)).isFalse();

    PositionDeleteIndex indexB = indexes.get("file_b.parquet");
    assertThat(indexB).as("index for file_b.parquet").isNotNull();
    assertThat(indexB.isDeleted(100L)).isTrue();
    assertThat(indexB.isDeleted(1L)).isFalse();
  }

  @Test
  public void mixedCharSequenceTypesShareIndex() {
    // Adjacent records use Utf8 vs String for the same logical path. CharSequenceUtil treats them
    // as equal, so they must accumulate into a single index without spuriously closing the group.
    List<StructLike> deletes =
        Lists.newArrayList(
            Row.of("file_a.parquet", 0L),
            Row.of(new Utf8("file_a.parquet"), 1L),
            Row.of("file_a.parquet", 2L),
            Row.of(new Utf8("file_a.parquet"), 3L));

    CharSequenceMap<PositionDeleteIndex> indexes = toIndexes(deletes);

    assertThat(indexes.keySet()).hasSize(1);
    PositionDeleteIndex index = indexes.get("file_a.parquet");
    assertThat(index.isDeleted(0L)).isTrue();
    assertThat(index.isDeleted(1L)).isTrue();
    assertThat(index.isDeleted(2L)).isTrue();
    assertThat(index.isDeleted(3L)).isTrue();
    assertThat(index.isDeleted(4L)).isFalse();
  }

  @Test
  public void sparseSinglePathRecordsExactPositions() {
    List<StructLike> deletes =
        Lists.newArrayList(
            Row.of("file_a.parquet", 1L),
            Row.of("file_a.parquet", 17L),
            Row.of("file_a.parquet", 256L),
            Row.of("file_a.parquet", 4096L));

    CharSequenceMap<PositionDeleteIndex> indexes = toIndexes(deletes);

    PositionDeleteIndex index = indexes.get("file_a.parquet");
    assertThat(index.isDeleted(0L)).isFalse();
    assertThat(index.isDeleted(1L)).isTrue();
    assertThat(index.isDeleted(2L)).isFalse();
    assertThat(index.isDeleted(17L)).isTrue();
    assertThat(index.isDeleted(256L)).isTrue();
    assertThat(index.isDeleted(4096L)).isTrue();
    assertThat(index.isDeleted(4097L)).isFalse();
  }

  @Test
  public void deleteFilePropagatesToEachIndex() {
    DeleteFile file = Mockito.mock(DeleteFile.class);
    List<StructLike> deletes =
        Lists.newArrayList(
            Row.of("file_a.parquet", 0L),
            Row.of("file_a.parquet", 1L),
            Row.of("file_b.parquet", 5L));

    CharSequenceMap<PositionDeleteIndex> indexes =
        Deletes.toPositionIndexes(CloseableIterable.withNoopClose(deletes), file);

    assertThat(indexes.keySet()).hasSize(2);
    assertThat(indexes.get("file_a.parquet").deleteFiles())
        .as("file_a should record its source delete file")
        .containsExactly(file);
    assertThat(indexes.get("file_b.parquet").deleteFiles())
        .as("file_b should record its source delete file")
        .containsExactly(file);
  }

  @Test
  public void closeFailureSurfacesAsUncheckedIOException() {
    List<StructLike> rows = Lists.newArrayList(Row.of("file_a.parquet", 0L));
    CloseableIterable<StructLike> throwingOnClose =
        new CloseableIterable<StructLike>() {
          @Override
          public void close() throws IOException {
            throw new IOException("boom");
          }

          @Override
          public CloseableIterator<StructLike> iterator() {
            return CloseableIterator.withClose(rows.iterator());
          }
        };

    assertThatThrownBy(() -> Deletes.toPositionIndexes(throwingOnClose))
        .isInstanceOf(UncheckedIOException.class)
        .hasMessageContaining("Failed to close position delete source")
        .hasCauseInstanceOf(IOException.class);
  }

  private static CharSequenceMap<PositionDeleteIndex> toIndexes(Iterable<StructLike> rows) {
    return Deletes.toPositionIndexes(CloseableIterable.withNoopClose(rows));
  }
}
