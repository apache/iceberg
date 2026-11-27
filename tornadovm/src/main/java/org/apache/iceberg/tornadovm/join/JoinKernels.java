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
package org.apache.iceberg.tornadovm.join;

import uk.ac.manchester.tornado.api.KernelContext;
import uk.ac.manchester.tornado.api.types.arrays.ByteArray;
import uk.ac.manchester.tornado.api.types.arrays.DoubleArray;
import uk.ac.manchester.tornado.api.types.arrays.IntArray;
import uk.ac.manchester.tornado.api.types.arrays.LongArray;

/**
 * The kernels of device joins ({@link StarStage}): dimension lookups and gathers, probes into a
 * build table, and dense grouping. Each takes a {@link KernelContext} and guards its row index.
 */
final class JoinKernels {
  private JoinKernels() {}

  /** Ints of a star-join spec a dimension: key kind, slot, validity slot, first key, keys, mask. */
  static final int JOIN_SPEC_INTS = 6;

  /** Ints of a star-key spec a gathered key: key column, first key, keys, codes. */
  static final int GATHER_SPEC_INTS = 4;

  /**
   * Inner joins of the fact rows with dimension tables whose keys are dense integers: a row stays
   * live only if, for every dimension, its foreign key is non-null, within the dimension's key
   * range, and kept by the dimension's mask in {@code dimensions} (an int a key, 0 dropped). {@code
   * spec} holds, a dimension, the foreign key's kind (0 INT32, 1 INT64), slot and validity slot (or
   * -1), the dimension's first key, its key count and the offset of its mask.
   */
  static void starFilter(
      KernelContext context,
      IntArray spec,
      int joins,
      IntArray dimensions,
      IntArray ints,
      LongArray longs,
      ByteArray valid,
      int stride,
      ByteArray live) {
    int row = context.globalIdx;
    if (row < stride) {
      if (live.get(row) == 1) {
        boolean keep = true;
        for (int j = 0; j < joins; j++) {
          int base = j * JOIN_SPEC_INTS;
          int slot = spec.get(base + 1);
          int validSlot = spec.get(base + 2);
          if (validSlot >= 0 && valid.get(validSlot * stride + row) == 0) {
            keep = false;
          } else {
            long key =
                spec.get(base) == 0
                    ? ints.get(slot * stride + row)
                    : longs.get((long) slot * stride + row);
            long index = key - spec.get(base + 3);
            if (index < 0
                || index >= spec.get(base + 4)
                || dimensions.get(spec.get(base + 5) + (int) index) == 0) {
              keep = false;
            }
          }
        }
        if (!keep) {
          live.set(row, (byte) 0);
        }
      }
    }
  }

  /**
   * Replaces foreign keys among the group-key columns with a dimension attribute's codes: for a
   * live row, key column {@code k} holds a foreign key that {@link #starFilter} kept, and becomes
   * {@code dimensions[codes + key - first]}. {@code spec} holds, a gathered key, its key column,
   * the dimension's first key, its key count and the offset of the attribute's codes.
   */
  static void starKeys(
      KernelContext context,
      IntArray spec,
      int gathers,
      IntArray dimensions,
      ByteArray live,
      int stride,
      LongArray keys) {
    int row = context.globalIdx;
    if (row < stride) {
      if (live.get(row) == 1) {
        for (int g = 0; g < gathers; g++) {
          int base = g * GATHER_SPEC_INTS;
          long at = (long) spec.get(base) * stride + row;
          int index = (int) (keys.get(at) - spec.get(base + 1));
          keys.set(at, dimensions.get(spec.get(base + 3) + index));
        }
      }
    }
  }

  /** Ints of a probe spec before its key parts: parts, build rows, comparison, value column. */
  static final int PROBE_HEADER_INTS = 9;

  /**
   * Ints of a probe key part: kind, column kind, slot, first key, codes offset, the multiplier's
   * low 32 bits, the part's offset, its range (values outside [offset, offset + range) join
   * nothing) and the multiplier's high 32 bits.
   */
  static final int PROBE_PART_INTS = 9;

  /**
   * An inner join with a small table given on the host as sorted keys and one value each: a live
   * row's key is {@code sum(part * multiplier)}, each part a fact column (kind 0: its value) or a
   * dimension attribute (kind 1: the code of the row's foreign key), and its weight the number of
   * build rows with that key whose value passes the comparison with the row's value column (0 any,
   * 1 row < build, 2 row <= build, 3 row = build, 4 row > build, 5 row >= build). A row of weight 0
   * is dropped; the others count as many times as their weight ({@link #applyWeights}). The build
   * keys are binary searched; a key's rows are few.
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  static void probeWeights(
      KernelContext context,
      IntArray spec,
      IntArray dimensions,
      LongArray buildKeys,
      LongArray buildValues,
      IntArray ints,
      LongArray longs,
      ByteArray valid,
      int stride,
      ByteArray live,
      DoubleArray weights,
      DoubleArray buildSums,
      DoubleArray rowSums) {
    int row = context.globalIdx;
    if (row < stride) {
      double weight = 0.0;
      int sums = spec.get(6);
      int buildCapacity = spec.get(7);
      for (int k = 0; k < sums; k++) {
        rowSums.set((2L * k) * stride + row, 0.0);
        rowSums.set((2L * k + 1) * stride + row, 0.0);
      }
      if (live.get(row) == 1) {
        int parts = spec.get(0);
        int rows = spec.get(1);
        int comparison = spec.get(2);
        long key = 0;
        boolean inRange = true;
        for (int p = 0; p < parts; p++) {
          int base = PROBE_HEADER_INTS + p * PROBE_PART_INTS;
          int slot = spec.get(base + 2);
          long value =
              spec.get(base + 1) == 0
                  ? ints.get(slot * stride + row)
                  : longs.get((long) slot * stride + row);
          if (spec.get(base) == 1) {
            value = dimensions.get(spec.get(base + 4) + (int) (value - spec.get(base + 3)));
          }
          long part = value - spec.get(base + 6);
          if (part < 0 || part >= spec.get(base + 7)) {
            inRange = false;
          }
          long multiplier = ((long) spec.get(base + 8) << 32) | (spec.get(base + 5) & 0xFFFFFFFFL);
          key += part * multiplier;
        }
        long rowValue = 0;
        boolean rowNull = false;
        if (comparison != 0) {
          int slot = spec.get(4);
          int validSlot = spec.get(5);
          rowNull = validSlot >= 0 && valid.get(validSlot * stride + row) == 0;
          rowValue =
              spec.get(3) == 0
                  ? ints.get(slot * stride + row)
                  : longs.get((long) slot * stride + row);
        }
        int low = 0;
        int high = inRange ? rows : 0;
        while (low < high) {
          int middle = (low + high) >>> 1;
          if (buildKeys.get(middle) < key) {
            low = middle + 1;
          } else {
            high = middle;
          }
        }
        for (int j = low; j < rows && buildKeys.get(j) == key; j++) {
          long buildValue = buildValues.get(j);
          boolean pass;
          if (comparison == 0) {
            pass = true;
          } else if (rowNull) {
            pass = false;
          } else if (comparison == 1) {
            pass = rowValue < buildValue;
          } else if (comparison == 2) {
            pass = rowValue <= buildValue;
          } else if (comparison == 3) {
            pass = rowValue == buildValue;
          } else if (comparison == 4) {
            pass = rowValue > buildValue;
          } else {
            pass = rowValue >= buildValue;
          }
          if (pass) {
            weight += 1.0;
            for (int k = 0; k < sums; k++) {
              double buildSum = buildSums.get((long) k * buildCapacity + j);
              if (Math.abs(buildSum) <= Double.MAX_VALUE) { // not NaN: a null build value
                long at = (2L * k) * stride + row;
                rowSums.set(at, rowSums.get(at) + buildSum);
                rowSums.set(at + stride, rowSums.get(at + stride) + 1.0);
              }
            }
          }
        }
        if (weight == 0.0) {
          // A left outer join keeps an unmatched row once, its build sums 0.
          if (spec.get(8) == 1) {
            weight = 1.0;
          } else {
            live.set(row, (byte) 0);
          }
        }
      }
      weights.set(row, weight);
    }
  }

  /** Ints of a probe value's spec: its value column, build column, count column and mask. */
  static final int PROBE_VALUE_INTS = 4;

  /**
   * A probe's build-side values as value columns: for each of {@code probes} probe values ({@code
   * spec}: its value column, its build column, its non-null count column, and the count column of a
   * fact value it is masked by or -1), the sum of the build rows' values a row joined and how many
   * were not null -- 0 and 0 where the masking fact value is null, as SQL's {@code a - COALESCE(b,
   * 0)} is null when {@code a} is. After {@link #applyWeights}, which these must not be multiplied
   * by.
   */
  static void probeValues(
      KernelContext context,
      IntArray spec,
      int probes,
      DoubleArray rowSums,
      int values,
      int stride,
      DoubleArray valueData) {
    int row = context.globalIdx;
    if (row < stride) {
      for (int v = 0; v < probes; v++) {
        int base = v * PROBE_VALUE_INTS;
        int column = spec.get(base);
        int buildColumn = spec.get(base + 1);
        int count = spec.get(base + 2);
        int mask = spec.get(base + 3);
        boolean kept = mask < 0 || valueData.get((long) (values + 1 + mask) * stride + row) != 0.0;
        valueData.set(
            (long) column * stride + row,
            kept ? rowSums.get(2L * buildColumn * stride + row) : 0.0);
        valueData.set(
            (long) (values + 1 + count) * stride + row,
            kept ? rowSums.get((2L * buildColumn + 1) * stride + row) : 0.0);
      }
    }
  }

  /** Multiplies each of a row's {@code columns} value columns by its join weight. */
  static void applyWeights(
      KernelContext context, DoubleArray weights, int columns, int stride, DoubleArray values) {
    int row = context.globalIdx;
    if (row < stride) {
      double weight = weights.get(row);
      for (int c = 0; c < columns; c++) {
        long at = (long) c * stride + row;
        values.set(at, values.get(at) * weight);
      }
    }
  }

  /**
   * Zeroes {@code out}'s first {@code size} doubles: each thread every {@code stride}-th from its
   * index, so a grid over the batch clears an array of any size.
   */
  static void denseClear(KernelContext context, int size, int stride, DoubleArray out) {
    int row = context.globalIdx;
    if (row < stride) {
      for (int j = row; j < size; j += stride) {
        out.set(j, 0.0);
      }
    }
  }

  /**
   * A star aggregate whose group keys are all dimension codes, aggregated into a dense array
   * instead of by cuDF: a live row adds each of its {@code columns} value columns (sums, the row
   * count, non-null counts) to group {@code sum(code_k * spec[k])} of {@code out}, {@code groups}
   * doubles a column. Rows the joins dropped cost a load of {@code live}; a hash group-by would
   * group them all under one key.
   */
  static void denseAggregate(
      KernelContext context,
      IntArray spec,
      int keys,
      ByteArray live,
      LongArray keyColumns,
      DoubleArray values,
      int columns,
      int stride,
      int groups,
      DoubleArray out) {
    int row = context.globalIdx;
    if (row < stride) {
      if (live.get(row) == 1) {
        int group = 0;
        for (int k = 0; k < keys; k++) {
          group += (int) keyColumns.get((long) k * stride + row) * spec.get(k);
        }
        for (int c = 0; c < columns; c++) {
          context.atomicAdd(out, c * groups + group, values.get((long) c * stride + row));
        }
      }
    }
  }
}
