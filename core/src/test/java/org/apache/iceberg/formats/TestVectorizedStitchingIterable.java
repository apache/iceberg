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
package org.apache.iceberg.formats;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.SkippingCloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

class TestVectorizedStitchingIterable {
  private final NameStitcher stitcher = new NameStitcher();

  @Test
  void stitchesBatchesWithDifferentBoundaries() {
    List<List<String>> batches =
        stitch(
            new BatchedIterable("b", 3, range(0, 10)), new BatchedIterable("c", 4, range(0, 10)));

    assertThat(batches).extracting(List::size).containsExactly(3, 1, 2, 2, 1, 1);
    assertThat(Iterables.concat(batches)).containsExactlyElementsOf(rows(0, 10, "b", "c"));
  }

  @Test
  void skipsRowsNotReadByEveryPart() {
    // the data file is read from row 4 and a filter skipped rows 2 to 5 of a column file
    List<List<String>> batches =
        stitch(
            new BatchedIterable("b", 3, range(4, 10)),
            new BatchedIterable("c", 4, range(0, 10)),
            new BatchedIterable("d", 2, range(0, 2), range(6, 10)));

    assertThat(Iterables.concat(batches)).containsExactlyElementsOf(rows(6, 10, "b", "c", "d"));
  }

  @Test
  void slicesOnlyPartialBatches() {
    stitch(new BatchedIterable("b", 4, range(0, 8)), new BatchedIterable("c", 2, range(0, 8)));

    assertThat(stitcher.slices)
        .hasSize(4)
        .allSatisfy(slice -> assertThat(slice.get(0)).startsWith("b"));
  }

  @Test
  void readsNoBatchSkippedByAnotherPart() {
    BatchedIterable base = new BatchedIterable("b", 3, range(0, 10));
    stitch(base, new BatchedIterable("c", 4, range(6, 10)));

    assertThat(base.readPositions).containsExactly(6L, 9L);
  }

  @Test
  void closesPartIteratorsWhenClosed() throws IOException {
    BatchedIterable base = new BatchedIterable("b", 3, range(0, 4));
    BatchedIterable columnFile = new BatchedIterable("c", 2, range(0, 4));
    VectorizedStitchingIterable<List<String>> iterable =
        new VectorizedStitchingIterable<>(ImmutableList.of(base, columnFile), stitcher);
    iterable.iterator().next();
    iterable.close();

    assertThat(base.closedIterators).isEqualTo(1);
    assertThat(columnFile.closedIterators).isEqualTo(1);
  }

  private List<List<String>> stitch(BatchedIterable... parts) {
    return Lists.newArrayList(new VectorizedStitchingIterable<>(Arrays.asList(parts), stitcher));
  }

  private static List<String> rows(long start, long end, String... parts) {
    return LongStream.range(start, end)
        .mapToObj(
            pos -> Arrays.stream(parts).map(part -> part + pos).collect(Collectors.joining("-")))
        .collect(Collectors.toList());
  }

  private static long[] range(long start, long end) {
    return new long[] {start, end};
  }

  /** Stitches batches of row names by joining the names of the rows at the same index. */
  private static class NameStitcher implements VectorizedStitcher<List<String>> {
    private final List<List<String>> slices = Lists.newArrayList();

    @Override
    public List<String> stitch(List<List<String>> parts, int count) {
      List<String> rows = Lists.newArrayListWithCapacity(count);
      for (int row = 0; row < count; row += 1) {
        int index = row;
        rows.add(parts.stream().map(part -> part.get(index)).collect(Collectors.joining("-")));
      }

      return rows;
    }

    @Override
    public int numRows(List<String> batch) {
      return batch.size();
    }

    @Override
    public List<String> slice(List<String> batch, int offset, int count) {
      List<String> slice = ImmutableList.copyOf(batch.subList(offset, offset + count));
      slices.add(slice);
      return slice;
    }
  }

  /** Reads ranges of rows in batches, naming each row after the part and its position. */
  private static class BatchedIterable implements CloseableIterable<List<String>> {
    private final List<Long> starts = Lists.newArrayList();
    private final List<List<String>> batches = Lists.newArrayList();
    private final List<Long> readPositions = Lists.newArrayList();
    private int closedIterators = 0;

    private BatchedIterable(String name, int batchSize, long[]... ranges) {
      for (long[] range : ranges) {
        for (long start = range[0]; start < range[1]; start += batchSize) {
          starts.add(start);
          batches.add(
              LongStream.range(start, Math.min(start + batchSize, range[1]))
                  .mapToObj(pos -> name + pos)
                  .collect(Collectors.toList()));
        }
      }
    }

    @Override
    public CloseableIterator<List<String>> iterator() {
      return new SkippingCloseableIterator<>() {
        private int index = 0;

        @Override
        public long position() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }

          return starts.get(index);
        }

        @Override
        public void advanceTo(long target) {
          while (hasNext() && starts.get(index) + batches.get(index).size() <= target) {
            index += 1;
          }
        }

        @Override
        public boolean hasNext() {
          return index < batches.size();
        }

        @Override
        public List<String> next() {
          readPositions.add(starts.get(index));
          List<String> batch = batches.get(index);
          index += 1;
          return batch;
        }

        @Override
        public void close() {
          closedIterators += 1;
        }
      };
    }

    @Override
    public void close() {}
  }
}
