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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for #15622 — {@code BaseFile.splitOffsets()} previously allocated a fresh
 * unmodifiable {@code List<Long>} wrapper on every call. After the fix, repeated calls return the
 * same cached instance, while still preserving value-equality with the underlying long[] and the
 * documented null-when-corrupted contract.
 */
class TestBaseFileSplitOffsets {

  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
  private static final List<Long> OFFSETS = List.of(0L, 1024L, 2048L);

  @Test
  void splitOffsetsReturnsSameInstanceOnRepeatedCalls() {
    DataFile file =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(4096)
            .withRecordCount(10)
            .withSplitOffsets(OFFSETS)
            .build();

    List<Long> first = file.splitOffsets();
    List<Long> second = file.splitOffsets();

    // Before the fix, splitOffsets() wrapped the long[] in a new
    // ArrayUtil.toUnmodifiableLongList on every invocation, so
    // first != second by reference. After the fix, the wrapper is
    // cached and the two calls share the same instance.
    assertThat(second).as("splitOffsets() should reuse a cached wrapper").isSameAs(first);
    assertThat(first).containsExactlyElementsOf(OFFSETS);
  }

  @Test
  void splitOffsetsNullWhenCorruptedThenReturnsNullStably() {
    // Corruption is signalled by the last offset being >= fileSize. The
    // file below has split offsets that exceed the declared file size so
    // BaseFile.hasWellDefinedOffsets() returns false and splitOffsets()
    // must return null. Caching must not turn a null into a stale empty
    // list across calls.
    DataFile file =
        DataFiles.builder(SPEC)
            .withPath("/path/to/corrupt.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(5)
            .withSplitOffsets(List.of(0L, 50L, 200L))
            .build();

    assertThat(file.splitOffsets()).isNull();
    assertThat(file.splitOffsets()).isNull();
  }

  @Test
  void copyConstructorProducesIndependentCachedView() {
    // BaseFile copy constructor reassigns splitOffsets via Arrays.copyOf;
    // the cached wrapper must be invalidated so that the copy produces
    // its own list view (not the source's cached one).
    DataFile original =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(4096)
            .withRecordCount(10)
            .withSplitOffsets(OFFSETS)
            .build();
    List<Long> originalView = original.splitOffsets();

    DataFile copy = original.copy();
    List<Long> copyView = copy.splitOffsets();

    // Equal contents...
    assertThat(copyView).containsExactlyElementsOf(originalView);
    // ...but not the same cached instance — each BaseFile owns its own
    // long[] (via Arrays.copyOf in the copy constructor) and therefore
    // its own cached wrapper.
    assertThat(copyView).isNotSameAs(originalView);
    // The copy's own cache is also stable across repeated calls.
    assertThat(copy.splitOffsets()).isSameAs(copyView);
  }

  @Test
  void splitOffsetsListIsUnmodifiable() {
    DataFile file =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(4096)
            .withRecordCount(10)
            .withSplitOffsets(OFFSETS)
            .build();

    List<Long> offsets = file.splitOffsets();
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> offsets.set(0, 99L))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
