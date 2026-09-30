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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.SkippingCloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

class TestRowAlignedStitchingIterable {

  @SuppressWarnings("unchecked")
  private static final Stitcher<String> STITCHER = mock(Stitcher.class);

  private static final Stitcher<String> CONCAT = (rows, count) -> String.join("-", rows);

  @Test
  void closesEveryPart() throws IOException {
    PositionedIterable base = new PositionedIterable("b");
    PositionedIterable columnFile = new PositionedIterable("c");

    RowAlignedStitchingIterable<String> iterable =
        new RowAlignedStitchingIterable<>(ImmutableList.of(base, columnFile), STITCHER);
    iterable.close();

    assertThat(base.closed).isTrue();
    assertThat(columnFile.closed).isTrue();
  }

  @Test
  void stitchesRowsAtTheSamePosition() {
    assertThat(stitch(new PositionedIterable("b", 0, 1), new PositionedIterable("c", 0, 1)))
        .containsExactly("b0-c0", "b1-c1");
  }

  @Test
  void skipsColumnFileRowsOutsideTheDataFileRange() {
    assertThat(
            stitch(new PositionedIterable("b", 2, 3), new PositionedIterable("c", 0, 1, 2, 3, 4)))
        .containsExactly("b2-c2", "b3-c3");
  }

  @Test
  void skipsRowsNotReadFromTheDataFile() {
    assertThat(stitch(new PositionedIterable("b", 1, 3), new PositionedIterable("c", 0, 1, 2, 3)))
        .containsExactly("b1-c1", "b3-c3");
  }

  @Test
  void skipsRowsNotReadFromAColumnFile() {
    assertThat(
            stitch(
                new PositionedIterable("b", 0, 1, 2, 3),
                new PositionedIterable("c", 0, 1, 3),
                new PositionedIterable("d", 1, 2, 3)))
        .containsExactly("b1-c1-d1", "b3-c3-d3");
  }

  @Test
  void rejectsPartsWithoutRowPositions() {
    RowAlignedStitchingIterable<String> iterable =
        new RowAlignedStitchingIterable<>(
            ImmutableList.of(
                new PositionedIterable("b", 0), CloseableIterable.withNoopClose(List.of("c0"))),
            CONCAT);

    assertThatThrownBy(iterable::iterator)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot stitch vertical split 1: row positions are not tracked");
  }

  @Test
  void rejectsInvalidArguments() {
    List<CloseableIterable<String>> parts =
        ImmutableList.of(new PositionedIterable("b"), new PositionedIterable("c"));

    assertThatThrownBy(() -> new RowAlignedStitchingIterable<>(null, STITCHER))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid parts: null");

    List<CloseableIterable<String>> onePart = ImmutableList.of(new PositionedIterable("b"));
    assertThatThrownBy(() -> new RowAlignedStitchingIterable<>(onePart, STITCHER))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid parts: 1 (must be > 1)");

    assertThatThrownBy(() -> new RowAlignedStitchingIterable<>(parts, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid stitcher: null");
  }

  private static List<String> stitch(PositionedIterable... parts) {
    return Lists.newArrayList(
        new RowAlignedStitchingIterable<String>(Arrays.asList(parts), CONCAT));
  }

  /** Returns the rows at the given positions, each named after the part and its position. */
  private static class PositionedIterable implements CloseableIterable<String> {
    private final String name;
    private final long[] positions;
    private boolean closed = false;

    private PositionedIterable(String name, long... positions) {
      this.name = name;
      this.positions = positions;
    }

    @Override
    public CloseableIterator<String> iterator() {
      return new SkippingCloseableIterator<>() {
        private int index = 0;

        @Override
        public long position() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }

          return positions[index];
        }

        @Override
        public void advanceTo(long target) {
          while (hasNext() && positions[index] < target) {
            index += 1;
          }
        }

        @Override
        public boolean hasNext() {
          return index < positions.length;
        }

        @Override
        public String next() {
          String row = name + positions[index];
          index += 1;
          return row;
        }

        @Override
        public void close() {}
      };
    }

    @Override
    public void close() {
      this.closed = true;
    }
  }
}
