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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestPositionFilter {
  @Test
  public void testPositionFileFilter() {
    List<StructLike> positionDeletes =
        Lists.newArrayList(
            Row.of("file_a.avro", 0L),
            Row.of("file_a.avro", 3L),
            Row.of(new Utf8("file_a.avro"), 9L),
            Row.of("file_a.avro", 22L),
            Row.of("file_a.avro", 56L),
            Row.of(new Utf8("file_b.avro"), 16L),
            Row.of("file_b.avro", 19L),
            Row.of("file_b.avro", 63L),
            Row.of("file_b.avro", 70L),
            Row.of("file_b.avro", 91L));

    assertThat(
            Deletes.deletePositions(
                "file_a.avro", CloseableIterable.withNoopClose(positionDeletes)))
        .as("Should contain only file_a positions")
        .containsExactly(0L, 3L, 9L, 22L, 56L);

    assertThat(
            Deletes.deletePositions(
                "file_b.avro", CloseableIterable.withNoopClose(positionDeletes)))
        .as("Should contain only file_b positions")
        .containsExactly(16L, 19L, 63L, 70L, 91L);

    assertThat(
            Deletes.deletePositions(
                "file_c.avro", CloseableIterable.withNoopClose(positionDeletes)))
        .as("Should contain no positions for file_c")
        .isEmpty();
  }

  @Test
  public void testPositionMerging() {
    List<StructLike> positionDeletes1 =
        Lists.newArrayList(
            Row.of("file_a.avro", 0L),
            Row.of("file_a.avro", 3L),
            Row.of("file_a.avro", 9L),
            Row.of("file_a.avro", 22L),
            Row.of("file_a.avro", 56L));

    List<StructLike> positionDeletes2 =
        Lists.newArrayList(
            Row.of("file_a.avro", 16L),
            Row.of("file_a.avro", 19L),
            Row.of("file_a.avro", 63L),
            Row.of("file_a.avro", 70L),
            Row.of("file_a.avro", 91L));

    List<StructLike> positionDeletes3 =
        Lists.newArrayList(
            Row.of("file_a.avro", 3L), Row.of("file_a.avro", 19L), Row.of("file_a.avro", 22L));

    List<CloseableIterable<StructLike>> deletes =
        Lists.newArrayList(
            CloseableIterable.withNoopClose(positionDeletes1),
            CloseableIterable.withNoopClose(positionDeletes2),
            CloseableIterable.withNoopClose(positionDeletes3));

    assertThat(Deletes.deletePositions("file_a.avro", deletes))
        .as("Should merge deletes in order, with duplicates")
        .containsExactly(0L, 3L, 3L, 9L, 16L, 19L, 19L, 22L, 22L, 56L, 63L, 70L, 91L);
  }

  @Test
  public void testPositionStreamRowFilter() {
    CloseableIterable<StructLike> rows =
        CloseableIterable.withNoopClose(
            Lists.newArrayList(
                Row.of(0L, "a"),
                Row.of(1L, "b"),
                Row.of(2L, "c"),
                Row.of(3L, "d"),
                Row.of(4L, "e"),
                Row.of(5L, "f"),
                Row.of(6L, "g"),
                Row.of(7L, "h"),
                Row.of(8L, "i"),
                Row.of(9L, "j")));

    CloseableIterable<Long> deletes =
        CloseableIterable.withNoopClose(Lists.newArrayList(0L, 3L, 4L, 7L, 9L));

    CloseableIterable<StructLike> actual =
        Deletes.streamingFilter(rows, row -> row.get(0, Long.class), deletes);

    assertThat(Iterables.transform(actual, row -> row.get(0, Long.class)))
        .as("Filter should produce expected rows")
        .containsExactlyElementsOf(Lists.newArrayList(1L, 2L, 5L, 6L, 8L));
  }

  @Test
  public void testPositionStreamRowDeleteMarker() {
    CloseableIterable<StructLike> rows =
        CloseableIterable.withNoopClose(
            Lists.newArrayList(
                Row.of(0L, "a", false),
                Row.of(1L, "b", false),
                Row.of(2L, "c", false),
                Row.of(3L, "d", false),
                Row.of(4L, "e", false),
                Row.of(5L, "f", false),
                Row.of(6L, "g", false),
                Row.of(7L, "h", false),
                Row.of(8L, "i", false),
                Row.of(9L, "j", false)));

    CloseableIterable<Long> deletes =
        CloseableIterable.withNoopClose(Lists.newArrayList(0L, 3L, 4L, 7L, 9L));

    CloseableIterable<StructLike> actual =
        Deletes.streamingMarker(
            rows,
            row -> row.get(0, Long.class), /* row to position */
            deletes,
            row -> row.set(2, true) /* delete marker */);

    assertThat(Iterables.transform(actual, row -> row.get(2, Boolean.class)))
        .as("Filter should produce expected rows")
        .containsExactlyElementsOf(
            Lists.newArrayList(true, false, false, true, true, false, false, true, false, true));
  }

  @Test
  public void testPositionStreamRowFilterWithDuplicates() {
    CloseableIterable<StructLike> rows =
        CloseableIterable.withNoopClose(
            Lists.newArrayList(
                Row.of(0L, "a"),
                Row.of(1L, "b"),
                Row.of(2L, "c"),
                Row.of(3L, "d"),
                Row.of(4L, "e"),
                Row.of(5L, "f"),
                Row.of(6L, "g"),
                Row.of(7L, "h"),
                Row.of(8L, "i"),
                Row.of(9L, "j")));

    CloseableIterable<Long> deletes =
        CloseableIterable.withNoopClose(Lists.newArrayList(0L, 0L, 0L, 3L, 4L, 7L, 7L, 9L, 9L, 9L));

    CloseableIterable<StructLike> actual =
        Deletes.streamingFilter(rows, row -> row.get(0, Long.class), deletes);

    assertThat(Iterables.transform(actual, row -> row.get(0, Long.class)))
        .as("Filter should produce expected rows")
        .containsExactlyElementsOf(Lists.newArrayList(1L, 2L, 5L, 6L, 8L));
  }

  @Test
  public void testPositionStreamRowFilterWithRowGaps() {
    // test the case where row position is greater than the delete position
    CloseableIterable<StructLike> rows =
        CloseableIterable.withNoopClose(
            Lists.newArrayList(Row.of(2L, "c"), Row.of(3L, "d"), Row.of(5L, "f"), Row.of(6L, "g")));

    CloseableIterable<Long> deletes =
        CloseableIterable.withNoopClose(Lists.newArrayList(0L, 2L, 3L, 4L, 7L, 9L));

    CloseableIterable<StructLike> actual =
        Deletes.streamingFilter(rows, row -> row.get(0, Long.class), deletes);

    assertThat(Iterables.transform(actual, row -> row.get(0, Long.class)))
        .as("Filter should produce expected rows")
        .containsExactlyElementsOf(Lists.newArrayList(5L, 6L));
  }

  @Test
  public void testCombinedPositionStreamRowFilter() {
    CloseableIterable<StructLike> positionDeletes1 =
        CloseableIterable.withNoopClose(
            Lists.newArrayList(
                Row.of("file_a.avro", 0L),
                Row.of("file_a.avro", 3L),
                Row.of("file_a.avro", 9L),
                Row.of("file_b.avro", 5L),
                Row.of("file_b.avro", 6L)));

    CloseableIterable<StructLike> positionDeletes2 =
        CloseableIterable.withNoopClose(
            Lists.newArrayList(
                Row.of("file_a.avro", 3L),
                Row.of("file_a.avro", 4L),
                Row.of("file_a.avro", 7L),
                Row.of("file_b.avro", 2L)));

    CloseableIterable<StructLike> rows =
        CloseableIterable.withNoopClose(
            Lists.newArrayList(
                Row.of(0L, "a"),
                Row.of(1L, "b"),
                Row.of(2L, "c"),
                Row.of(3L, "d"),
                Row.of(4L, "e"),
                Row.of(5L, "f"),
                Row.of(6L, "g"),
                Row.of(7L, "h"),
                Row.of(8L, "i"),
                Row.of(9L, "j")));

    CloseableIterable<StructLike> actual =
        Deletes.streamingFilter(
            rows,
            row -> row.get(0, Long.class),
            Deletes.deletePositions(
                "file_a.avro", ImmutableList.of(positionDeletes1, positionDeletes2)));

    assertThat(Iterables.transform(actual, row -> row.get(0, Long.class)))
        .as("Filter should produce expected rows")
        .containsExactlyElementsOf(Lists.newArrayList(1L, 2L, 5L, 6L, 8L));
  }

  @Test
  public void testPositionSetRowFilter() {
    CloseableIterable<StructLike> rows =
        CloseableIterable.withNoopClose(
            Lists.newArrayList(
                Row.of(0L, "a"),
                Row.of(1L, "b"),
                Row.of(2L, "c"),
                Row.of(3L, "d"),
                Row.of(4L, "e"),
                Row.of(5L, "f"),
                Row.of(6L, "g"),
                Row.of(7L, "h"),
                Row.of(8L, "i"),
                Row.of(9L, "j")));

    CloseableIterable<Long> deletes =
        CloseableIterable.withNoopClose(Lists.newArrayList(0L, 3L, 4L, 7L, 9L));

    Predicate<StructLike> shouldKeep =
        row -> !Deletes.toPositionIndex(deletes).isDeleted(row.get(0, Long.class));
    CloseableIterable<StructLike> actual = CloseableIterable.filter(rows, shouldKeep);

    assertThat(Iterables.transform(actual, row -> row.get(0, Long.class)))
        .as("Filter should produce expected rows")
        .containsExactlyElementsOf(Lists.newArrayList(1L, 2L, 5L, 6L, 8L));
  }

  static Stream<ExecutorService> executorServiceProvider() {
    return Stream.of(
        null,
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(4)));
  }

  @ParameterizedTest
  @MethodSource("executorServiceProvider")
  public void testCombinedPositionSetRowFilter(ExecutorService executorService) {
    CloseableIterable<StructLike> positionDeletes1 =
        CloseableIterable.withNoopClose(
            Lists.newArrayList(
                Row.of("file_a.avro", 0L),
                Row.of("file_a.avro", 3L),
                Row.of("file_a.avro", 9L),
                Row.of("file_b.avro", 5L),
                Row.of("file_b.avro", 6L)));

    CloseableIterable<StructLike> positionDeletes2 =
        CloseableIterable.withNoopClose(
            Lists.newArrayList(
                Row.of("file_a.avro", 3L),
                Row.of("file_a.avro", 4L),
                Row.of("file_a.avro", 7L),
                Row.of("file_b.avro", 2L)));

    CloseableIterable<StructLike> rows =
        CloseableIterable.withNoopClose(
            Lists.newArrayList(
                Row.of(0L, "a"),
                Row.of(1L, "b"),
                Row.of(2L, "c"),
                Row.of(3L, "d"),
                Row.of(4L, "e"),
                Row.of(5L, "f"),
                Row.of(6L, "g"),
                Row.of(7L, "h"),
                Row.of(8L, "i"),
                Row.of(9L, "j")));

    Predicate<StructLike> isDeleted =
        row ->
            Deletes.toPositionIndex(
                    "file_a.avro",
                    ImmutableList.of(positionDeletes1, positionDeletes2),
                    executorService)
                .isDeleted(row.get(0, Long.class));
    CloseableIterable<StructLike> actual = CloseableIterable.filter(rows, isDeleted.negate());

    assertThat(Iterables.transform(actual, row -> row.get(0, Long.class)))
        .as("Filter should produce expected rows")
        .containsExactlyElementsOf(Lists.newArrayList(1L, 2L, 5L, 6L, 8L));
  }

  @Test
  public void testClosePositionStreamRowDeleteMarker() {
    List<Long> deletes = Lists.newArrayList(1L, 2L);

    List<StructLike> records =
        Lists.newArrayList(
            Row.of(29, "a", 1L), Row.of(43, "b", 2L), Row.of(61, "c", 3L), Row.of(89, "d", 4L));

    CheckingClosableIterable<StructLike> data = new CheckingClosableIterable<>(records);
    CheckingClosableIterable<Long> deletePositions = new CheckingClosableIterable<>(deletes);

    CloseableIterable<StructLike> posDeletesIterable =
        Deletes.streamingFilter(data, row -> row.get(2, Long.class), deletePositions);

    // end iterator is always wrapped with FilterIterator
    CloseableIterable<StructLike> eqDeletesIterable =
        Deletes.filterDeleted(posDeletesIterable, i -> false, new DeleteCounter());
    List<StructLike> result = Lists.newArrayList(eqDeletesIterable.iterator());

    // as first two records deleted, expect only last two records
    assertThat(Iterables.transform(result, row -> row.get(2, Long.class)))
        .as("Filter should produce expected rows")
        .containsExactlyElementsOf(Lists.newArrayList(3L, 4L));

    assertThat(data.isClosed).isTrue();
    assertThat(deletePositions.isClosed).isTrue();
  }

  @Test
  public void testDeleteMarkerFileClosed() {

    List<Long> deletes = Lists.newArrayList(1L, 2L);

    List<StructLike> records =
        Lists.newArrayList(
            Row.of(29, "a", 1L, false),
            Row.of(43, "b", 2L, false),
            Row.of(61, "c", 3L, false),
            Row.of(89, "d", 4L, false));

    CheckingClosableIterable<StructLike> data = new CheckingClosableIterable<>(records);
    CheckingClosableIterable<Long> deletePositions = new CheckingClosableIterable<>(deletes);

    CloseableIterable<StructLike> resultIterable =
        Deletes.streamingMarker(
            data, row -> row.get(2, Long.class), deletePositions, row -> row.set(3, true));

    // end iterator is always wrapped with FilterIterator
    CloseableIterable<StructLike> eqDeletesIterable =
        Deletes.filterDeleted(resultIterable, i -> false, new DeleteCounter());
    List<StructLike> result = Lists.newArrayList(eqDeletesIterable.iterator());

    // as first two records deleted, expect only those two records marked
    assertThat(Iterables.transform(result, row -> row.get(3, Boolean.class)))
        .as("Filter should produce expected rows")
        .containsExactlyElementsOf(Lists.newArrayList(true, true, false, false));

    assertThat(data.isClosed).isTrue();
    assertThat(deletePositions.isClosed).isTrue();
  }

  private static class CheckingClosableIterable<E> implements CloseableIterable<E> {
    AtomicBoolean isClosed = new AtomicBoolean(false);
    final Iterable<E> iterable;

    CheckingClosableIterable(Iterable<E> iterable) {
      this.iterable = iterable;
    }

    public boolean isClosed() {
      return isClosed.get();
    }

    @Override
    public void close() throws IOException {
      isClosed.set(true);
    }

    @Override
    public CloseableIterator<E> iterator() {
      Iterator<E> it = iterable.iterator();
      return new CloseableIterator<E>() {

        @Override
        public boolean hasNext() {
          return it.hasNext();
        }

        @Override
        public E next() {
          return it.next();
        }

        @Override
        public void close() {
          isClosed.set(true);
        }
      };
    }
  }
}
