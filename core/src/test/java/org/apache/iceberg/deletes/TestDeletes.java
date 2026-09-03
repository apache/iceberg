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

import java.nio.CharBuffer;
import java.util.List;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceMap;
import org.junit.jupiter.api.Test;

public class TestDeletes {
  static final String PATH_A = "file_a";
  static final String PATH_B = "file_b";

  @Test
  public void testPositionIndexesWithNonStringPaths() {
    // paths are typed as CharSequence, so readers are free to return any implementation
    List<StructLike> positionDeletes =
        Lists.newArrayList(
            Row.of(CharBuffer.wrap(PATH_A), 0L),
            Row.of(CharBuffer.wrap(PATH_A), 5L),
            Row.of(CharBuffer.wrap(PATH_B), 1L),
            Row.of(CharBuffer.wrap(PATH_B), 2L));

    CharSequenceMap<PositionDeleteIndex> indexes =
        Deletes.toPositionIndexes(CloseableIterable.withNoopClose(positionDeletes), null);

    assertThat(indexes).hasSize(2);
    assertThat(collect(indexes.get(PATH_A))).containsExactlyInAnyOrder(0L, 5L);
    assertThat(collect(indexes.get(PATH_B))).containsExactlyInAnyOrder(1L, 2L);
  }

  @Test
  public void testToPositionIndexesWithInterleavedPaths() {
    List<StructLike> positionDeletes =
        Lists.newArrayList(
            Row.of(PATH_A, 0L), Row.of(PATH_B, 1L), Row.of(PATH_A, 5L), Row.of(PATH_B, 2L));

    CharSequenceMap<PositionDeleteIndex> indexes =
        Deletes.toPositionIndexes(CloseableIterable.withNoopClose(positionDeletes), null);

    assertThat(indexes).hasSize(2);
    assertThat(collect(indexes.get(PATH_A))).containsExactlyInAnyOrder(0L, 5L);
    assertThat(collect(indexes.get(PATH_B))).containsExactlyInAnyOrder(1L, 2L);
  }

  private static List<Long> collect(PositionDeleteIndex index) {
    List<Long> positions = Lists.newArrayList();
    index.forEach(positions::add);
    return positions;
  }
}
