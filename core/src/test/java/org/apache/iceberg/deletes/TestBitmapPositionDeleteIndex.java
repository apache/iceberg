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

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

public class TestBitmapPositionDeleteIndex {

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

  private List<Long> collect(PositionDeleteIndex index) {
    List<Long> positions = Lists.newArrayList();
    index.forEach(positions::add);
    return positions;
  }
}
