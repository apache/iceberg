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
package org.apache.iceberg.tornadovm;

import java.util.Arrays;

/** Open-addressing sets and maps over primitive keys, so the CPU baseline does not box per row. */
final class PrimitiveHash {
  private PrimitiveHash() {}

  static int mix(long key) {
    long hash = key * 0x9E3779B97F4A7C15L;
    return (int) (hash ^ (hash >>> 32));
  }

  /** A set of longs, built once and then only probed. */
  static final class LongSet {
    private static final long EMPTY = Long.MIN_VALUE;
    private final long[] slots;
    private final int mask;
    private boolean containsEmpty = false;

    LongSet(long[] keys) {
      int capacity = Integer.highestOneBit(Math.max(4, keys.length * 2 - 1)) << 1;
      this.slots = new long[capacity];
      this.mask = capacity - 1;
      Arrays.fill(slots, EMPTY);
      for (long key : keys) {
        add(key);
      }
    }

    private void add(long key) {
      if (key == EMPTY) {
        containsEmpty = true;
        return;
      }
      int slot = mix(key) & mask;
      while (slots[slot] != EMPTY) {
        if (slots[slot] == key) {
          return;
        }
        slot = (slot + 1) & mask;
      }
      slots[slot] = key;
    }

    boolean contains(long key) {
      if (key == EMPTY) {
        return containsEmpty;
      }
      int slot = mix(key) & mask;
      while (slots[slot] != EMPTY) {
        if (slots[slot] == key) {
          return true;
        }
        slot = (slot + 1) & mask;
      }
      return false;
    }
  }

  /** Int keys to dense indexes 0, 1, 2, ... in order of first appearance. */
  static final class IntIndex {
    private int[] keys = new int[64];
    private int[] values = new int[64];
    private boolean[] used = new boolean[64];
    private int size = 0;

    int indexOf(int key) {
      int mask = keys.length - 1;
      int slot = mix(key) & mask;
      while (used[slot]) {
        if (keys[slot] == key) {
          return values[slot];
        }
        slot = (slot + 1) & mask;
      }
      used[slot] = true;
      keys[slot] = key;
      values[slot] = size;
      size++;
      if (size * 2 > keys.length) {
        grow();
      }
      return size - 1;
    }

    private void grow() {
      int[] oldKeys = keys;
      int[] oldValues = values;
      boolean[] oldUsed = used;
      keys = new int[oldKeys.length * 2];
      values = new int[oldKeys.length * 2];
      used = new boolean[oldKeys.length * 2];
      int mask = keys.length - 1;
      for (int j = 0; j < oldKeys.length; j++) {
        if (oldUsed[j]) {
          int slot = mix(oldKeys[j]) & mask;
          while (used[slot]) {
            slot = (slot + 1) & mask;
          }
          used[slot] = true;
          keys[slot] = oldKeys[j];
          values[slot] = oldValues[j];
        }
      }
    }

    int size() {
      return size;
    }

    /** The key of each index, in index order. */
    int[] keysByIndex() {
      int[] byIndex = new int[size];
      for (int j = 0; j < keys.length; j++) {
        if (used[j]) {
          byIndex[values[j]] = keys[j];
        }
      }
      return byIndex;
    }
  }
}
