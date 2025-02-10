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
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.io.Resources;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestBitmapPositionDeleteIndex {

  private static final long BITMAP_OFFSET = 0xFFFFFFFFL + 1L;
  private static final long CONTAINER_OFFSET = Character.MAX_VALUE + 1L;

  @Test
  public void testForEach() {
    long pos1 = 10L; // Container 0 (high bits = 0)
    long pos2 = 1L << 33; // Container 1 (high bits = 1)
    long pos3 = pos2 + 1; // Container 1 (high bits = 1)
    long pos4 = 2L << 33; // Container 2 (high bits = 2)
    long pos5 = pos4 + 1; // Container 2 (high bits = 2)
    long pos6 = 3L << 33; // Container 3 (high bits = 3)

    PositionDeleteIndex index = new BitmapPositionDeleteIndex();

    // add in any order
    index.delete(pos1);
    index.delete(pos6);
    index.delete(pos2);
    index.delete(pos3);
    index.delete(pos5);
    index.delete(pos4);

    // output must be sorted in ascending order across containers
    List<Long> positions = collect(index);
    assertThat(positions).containsExactly(pos1, pos2, pos3, pos4, pos5, pos6);
  }

  @Test
  public void testForEachEmptyBitmapIndex() {
    PositionDeleteIndex index = new BitmapPositionDeleteIndex();
    List<Long> positions = collect(index);
    assertThat(positions).isEmpty();
  }

  @Test
  public void testForEachEmptyIndex() {
    PositionDeleteIndex index = PositionDeleteIndex.empty();
    List<Long> positions = collect(index);
    assertThat(positions).isEmpty();
  }

  @Test
  public void testMergeBitmapIndexWithNonEmpty() {
    long pos1 = 10L; // Container 0 (high bits = 0)
    long pos2 = 1L << 33; // Container 1 (high bits = 1)
    long pos3 = pos2 + 1; // Container 1 (high bits = 1)
    long pos4 = 2L << 33; // Container 2 (high bits = 2)

    BitmapPositionDeleteIndex index1 = new BitmapPositionDeleteIndex();
    index1.delete(pos2);
    index1.delete(pos1);

    BitmapPositionDeleteIndex index2 = new BitmapPositionDeleteIndex();
    index2.delete(pos4);
    index2.delete(pos3);

    index1.merge(index2);

    // output must be sorted in ascending order across containers
    List<Long> positions = collect(index1);
    assertThat(positions).containsExactly(pos1, pos2, pos3, pos4);
  }

  @Test
  public void testMergeBitmapIndexWithEmpty() {
    long pos1 = 10L; // Container 0 (high bits = 0)
    long pos2 = 1L << 33; // Container 1 (high bits = 1)
    long pos3 = pos2 + 1; // Container 1 (high bits = 1)
    long pos4 = 2L << 33; // Container 2 (high bits = 2)

    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    index.delete(pos2);
    index.delete(pos1);
    index.delete(pos3);
    index.delete(pos4);
    index.merge(PositionDeleteIndex.empty());

    // output must be sorted in ascending order across containers
    List<Long> positions = collect(index);
    assertThat(positions).containsExactly(pos1, pos2, pos3, pos4);
  }

  @Test
  public void testEmptyIndexSerialization() throws Exception {
    PositionDeleteIndex index = new BitmapPositionDeleteIndex();
    validate(index, "empty-position-index.bin");
  }

  @Test
  public void testSmallAlternatingValuesIndexSerialization() throws Exception {
    PositionDeleteIndex index = new BitmapPositionDeleteIndex();
    index.delete(1L);
    index.delete(3L);
    index.delete(5L);
    index.delete(7L);
    index.delete(9L);
    validate(index, "small-alternating-values-position-index.bin");
  }

  @Test
  public void testSmallAndLargeValuesIndexSerialization() throws Exception {
    PositionDeleteIndex index = new BitmapPositionDeleteIndex();
    index.delete(100L);
    index.delete(101L);
    index.delete(Integer.MAX_VALUE + 100L);
    index.delete(Integer.MAX_VALUE + 101L);
    validate(index, "small-and-large-values-position-index.bin");
  }

  @Test
  public void testAllContainerTypesIndexSerialization() throws Exception {
    PositionDeleteIndex index = new BitmapPositionDeleteIndex();

    // bitmap 0, container 0 (array)
    index.delete(position(0 /* bitmap */, 0 /* container */, 5L));
    index.delete(position(0 /* bitmap */, 0 /* container */, 7L));

    // bitmap 0, container 1 (array that can be compressed)
    index.delete(
        position(0 /* bitmap */, 1 /* container */, 1L),
        position(0 /* bitmap */, 1 /* container */, 1000L));

    // bitmap 1, container 2 (bitset)
    index.delete(
        position(0 /* bitmap */, 2 /* container */, 1L),
        position(0 /* bitmap */, 2 /* container */, CONTAINER_OFFSET - 1L));

    // bitmap 1, container 0 (array)
    index.delete(position(1 /* bitmap */, 0 /* container */, 10L));
    index.delete(position(1 /* bitmap */, 0 /* container */, 20L));

    // bitmap 1, container 1 (array that can be compressed)
    index.delete(
        position(1 /* bitmap */, 1 /* container */, 10L),
        position(1 /* bitmap */, 1 /* container */, 500L));

    // bitmap 1, container 2 (bitset)
    index.delete(
        position(1 /* bitmap */, 2 /* container */, 1L),
        position(1 /* bitmap */, 2 /* container */, CONTAINER_OFFSET - 1));

    validate(index, "all-container-types-position-index.bin");
  }

  private static void validate(PositionDeleteIndex index, String goldenFile) throws Exception {
    ByteBuffer buffer = index.serialize();
    byte[] bytes = buffer.array();
    DeleteFile dv = mockDV(bytes.length, index.cardinality());
    PositionDeleteIndex indexCopy = PositionDeleteIndex.deserialize(bytes, dv);
    assertEqual(index, indexCopy);
    byte[] goldenBytes = readTestResource(goldenFile);
    assertThat(bytes).isEqualTo(goldenBytes);
    PositionDeleteIndex goldenIndex = PositionDeleteIndex.deserialize(goldenBytes, dv);
    assertEqual(index, goldenIndex);
  }

  private static DeleteFile mockDV(long contentSize, long cardinality) {
    DeleteFile mock = Mockito.mock(DeleteFile.class);
    Mockito.when(mock.contentSizeInBytes()).thenReturn(contentSize);
    Mockito.when(mock.recordCount()).thenReturn(cardinality);
    return mock;
  }

  private static void assertEqual(PositionDeleteIndex index, PositionDeleteIndex thatIndex) {
    assertThat(index.cardinality()).isEqualTo(thatIndex.cardinality());
    index.forEach(position -> assertThat(thatIndex.isDeleted(position)).isTrue());
    thatIndex.forEach(position -> assertThat(index.isDeleted(position)).isTrue());
  }

  private static long position(int bitmapIndex, int containerIndex, long value) {
    return bitmapIndex * BITMAP_OFFSET + containerIndex * CONTAINER_OFFSET + value;
  }

  private static byte[] readTestResource(String resourceName) throws IOException {
    URL resource = Resources.getResource(TestRoaringPositionBitmap.class, resourceName);
    return Resources.toByteArray(resource);
  }

  private List<Long> collect(PositionDeleteIndex index) {
    List<Long> positions = Lists.newArrayList();
    index.forEach(positions::add);
    return positions;
  }
}
