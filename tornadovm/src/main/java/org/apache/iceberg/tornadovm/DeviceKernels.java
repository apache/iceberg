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

import uk.ac.manchester.tornado.api.KernelContext;
import uk.ac.manchester.tornado.api.math.TornadoMath;
import uk.ac.manchester.tornado.api.types.arrays.ByteArray;
import uk.ac.manchester.tornado.api.types.arrays.DoubleArray;
import uk.ac.manchester.tornado.api.types.arrays.IntArray;
import uk.ac.manchester.tornado.api.types.arrays.LongArray;

/**
 * The Iceberg-specific parts of a merge-on-read scan, as TornadoVM kernels.
 *
 * <p>These are plain Java methods: TornadoVM JIT-compiles them to CUDA at the first execution of
 * the plan that holds them. cuDF does the work that needs cross-row cooperation (Parquet decoding,
 * the equality-delete hash probe, grouping, compaction); what is left is per-row and specific to
 * the table format, and is written here once, in Java.
 *
 * <p>Every kernel runs over the plan's capacity rather than the current file's row count, because a
 * plan is built once and reused for every file; {@code meta[META_ROWS]} says how many rows are
 * real.
 */
final class DeviceKernels {

  /** Rows in the current file. */
  static final int META_ROWS = 0;

  /** 1 when the current file has position deletes or a deletion vector. */
  static final int META_HAS_POSITION_DELETES = 1;

  /** 1 when the current file has equality deletes to apply. */
  static final int META_HAS_EQUALITY_DELETES = 2;

  /**
   * 1 when the equality-delete set contains a null key, which deletes every row whose key is null.
   */
  static final int META_EQUALITY_SET_HAS_NULL = 3;

  /** 1 when the equality key column is optional, so its validity bytes have to be consulted. */
  static final int META_KEY_NULLABLE = 4;

  /** Position-delete entries read on the device for the current batch. */
  static final int META_POSITION_ROWS = 5;

  /** Runs of those entries, one a data file that has them. */
  static final int META_POSITION_SEGMENTS = 6;

  static final int META_SIZE = 7;

  /**
   * The group key of a deleted or padding row. It is grouped like any other key and dropped on the
   * host, which lets cuDF's group-by run over the plan's whole capacity without a compaction first.
   */
  static final int DEAD_GROUP = Integer.MIN_VALUE;

  private DeviceKernels() {}

  /**
   * Merge-on-read in one pass: a row is live when it is a real row, its position is not set in the
   * file's deletion bitmap, and its key is not in the equality-delete set.
   *
   * <p>Equality deletes follow Iceberg's semantics for nulls: a null in the delete set deletes rows
   * whose key is null, and a null key is never deleted by a non-null entry.
   *
   * @param positionBits one bit a row, bit {@code i & 7} of byte {@code i >> 3}; set means deleted
   * @param inEqualitySet one byte a row from cuDF's set probe; read only when the file has equality
   *     deletes
   * @param valid validity bytes from the Parquet read; read only when the key is nullable
   * @param keyValidOffset where the key column's validity bytes start in {@code valid}
   */
  static void liveRows(
      KernelContext context,
      IntArray meta,
      ByteArray positionBits,
      ByteArray inEqualitySet,
      ByteArray valid,
      int keyValidOffset,
      ByteArray live) {
    int row = context.globalIdx;
    if (row < live.getSize()) {
      byte keep = 0;
      if (row < meta.get(META_ROWS)) {
        keep = 1;
        if (meta.get(META_HAS_POSITION_DELETES) == 1
            && ((positionBits.get(row >> 3) >> (row & 7)) & 1) == 1) {
          keep = 0;
        }
        if (meta.get(META_HAS_EQUALITY_DELETES) == 1) {
          boolean keyIsNull =
              meta.get(META_KEY_NULLABLE) == 1 && valid.get(keyValidOffset + row) == 0;
          if (keyIsNull) {
            if (meta.get(META_EQUALITY_SET_HAS_NULL) == 1) {
              keep = 0;
            }
          } else if (inEqualitySet.get(row) == 1) {
            keep = 0;
          }
        }
      }
      live.set(row, keep);
    }
  }

  /**
   * A column's value types for {@link #statsHashes}: 4-byte int/date, 8-byte long/timestamp,
   * double.
   */
  static final int STATS_INT = 0;

  static final int STATS_LONG = 1;

  static final int STATS_DOUBLE = 2;

  /**
   * Each live, non-null value's theta-sketch hash, as Apache DataSketches computes it for Iceberg's
   * {@code compute_table_stats}: MurmurHash3 x64 128 (seed 9001) of the value's single-value
   * serialization ({@code Conversions.toByteBuffer}: 4 or 8 bytes, little-endian), first half,
   * shifted right by one. Rows that are not live, null or past the batch get {@code
   * Long.MAX_VALUE}, which sorts last and is never kept.
   *
   * @param spec {type, slot, validity index or -1}
   */
  static void statsHashes(
      KernelContext context,
      IntArray meta,
      ByteArray live,
      IntArray ints,
      LongArray longs,
      DoubleArray doubles,
      ByteArray valid,
      IntArray spec,
      int capacity,
      LongArray out) {
    int row = context.globalIdx;
    if (row < capacity) {
      long hash = Long.MAX_VALUE;
      int validIndex = spec.get(2);
      boolean present =
          row < meta.get(META_ROWS)
              && live.get(row) == 1
              && (validIndex < 0 || valid.get(validIndex * capacity + row) == 1);
      if (present) {
        int type = spec.get(0);
        int slot = spec.get(1);
        long k1;
        long length;
        if (type == STATS_INT) {
          k1 = ints.get(slot * capacity + row) & 0xFFFFFFFFL;
          length = 4;
        } else if (type == STATS_LONG) {
          k1 = longs.get(slot * capacity + row);
          length = 8;
        } else {
          k1 = Double.doubleToRawLongBits(doubles.get(slot * capacity + row));
          length = 8;
        }
        // MurmurHash3 x64 128 of fewer than 16 bytes: the tail's first 8 bytes only.
        long h1 = 9001L;
        long h2 = 9001L;
        k1 *= 0x87c37b91114253d5L;
        k1 = (k1 << 31) | (k1 >>> 33);
        k1 *= 0x4cf5ad432745937fL;
        h1 ^= k1;
        h1 ^= length;
        h2 ^= length;
        h1 += h2;
        h2 += h1;
        h1 ^= h1 >>> 33;
        h1 *= 0xff51afd7ed558ccdL;
        h1 ^= h1 >>> 33;
        h1 *= 0xc4ceb9fe1a85ec53L;
        h1 ^= h1 >>> 33;
        h2 ^= h2 >>> 33;
        h2 *= 0xff51afd7ed558ccdL;
        h2 ^= h2 >>> 33;
        h2 *= 0xc4ceb9fe1a85ec53L;
        h2 ^= h2 >>> 33;
        h1 += h2;
        hash = h1 >>> 1;
      }
      out.set(row, hash);
    }
  }

  /**
   * {@code mark[i] = 1} for the first of each run of equal sorted hashes, other than the filler.
   */
  static void firstOfRuns(KernelContext context, LongArray sorted, int capacity, ByteArray mark) {
    int row = context.globalIdx;
    if (row < capacity) {
      long value = sorted.get(row);
      mark.set(
          row,
          (byte) (value != Long.MAX_VALUE && (row == 0 || sorted.get(row - 1) != value) ? 1 : 0));
    }
  }

  /** The first {@code keep} distinct hashes, in order: {@code out[j] = sorted[firsts[j]]}. */
  static void gatherFirsts(
      KernelContext context,
      IntArray firsts,
      IntArray count,
      LongArray sorted,
      int keep,
      LongArray out) {
    int index = context.globalIdx;
    if (index < keep) {
      out.set(index, index < count.get(0) ? sorted.get(firsts.get(index)) : Long.MAX_VALUE);
    }
  }

  /**
   * The rows of a batch that merge-on-read deletes: {@code dead[i] = 1} for a read row whose live
   * byte is 0, and 0 past the rows read -- the rows a deletion vector of the batch's files keeps.
   */
  static void deadRows(KernelContext context, IntArray meta, ByteArray live, ByteArray dead) {
    int row = context.globalIdx;
    if (row < dead.getSize()) {
      dead.set(row, (byte) (row < meta.get(META_ROWS) && live.get(row) == 0 ? 1 : 0));
    }
  }

  /**
   * Applies position-delete files the device read itself: entry {@code j} is a row position within
   * its data file, and the run of entries it belongs to says where that file starts in the batch.
   * Runs are few (one a data file of the batch), so each thread finds its own by a short scan. Only
   * zeroes are written, so threads that delete the same row do not race.
   *
   * @param segmentStarts the first entry of each run
   * @param segmentOffsets the batch row at which each run's data file starts
   */
  static void deletePositions(
      KernelContext context,
      LongArray positions,
      IntArray meta,
      IntArray segmentStarts,
      IntArray segmentOffsets,
      ByteArray live) {
    int entry = context.globalIdx;
    if (entry < positions.getSize()) {
      if (entry < meta.get(META_POSITION_ROWS)) {
        int segments = meta.get(META_POSITION_SEGMENTS);
        int segment = 0;
        while (segment + 1 < segments && segmentStarts.get(segment + 1) <= entry) {
          segment++;
        }
        long row = segmentOffsets.get(segment) + positions.get(entry);
        live.set((int) row, (byte) 0);
      }
    }
  }

  /**
   * Applies a scan's residual filter to the live rows: a conjunction of compiled terms (see {@link
   * DevicePredicate}), each a column compared with constants. A row stays live only if every term
   * holds. Nulls follow Iceberg's row evaluator, which orders them first.
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  static void filterRows(
      KernelContext context,
      IntArray terms,
      LongArray longConstants,
      DoubleArray doubleConstants,
      IntArray ints,
      LongArray longs,
      DoubleArray doubles,
      ByteArray valid,
      int stride,
      ByteArray live) {
    int row = context.globalIdx;
    if (row < live.getSize()) {
      if (live.get(row) == 1) {
        int count = terms.get(0);
        boolean keep = true;
        for (int t = 0; t < count; t++) {
          int base = 1 + t * DevicePredicate.TERM_INTS;
          int kind = terms.get(base);
          int op = terms.get(base + 1);
          int offset = terms.get(base + 2);
          int validColumn = terms.get(base + 3);
          int first = terms.get(base + 4);
          int constants = terms.get(base + 5);
          boolean isNull = validColumn >= 0 && valid.get(validColumn * stride + row) == 0;
          boolean pass;
          if (op == DevicePredicate.OP_FALSE) {
            pass = false;
          } else if (op == DevicePredicate.OP_IS_NULL) {
            pass = isNull;
          } else if (op == DevicePredicate.OP_NOT_NULL) {
            pass = !isNull;
          } else if (isNull) {
            // Iceberg's own evaluator compares with nulls first: a null is below every value.
            pass =
                op == DevicePredicate.OP_LT
                    || op == DevicePredicate.OP_LT_EQ
                    || op == DevicePredicate.OP_NOT_EQ;
          } else if (kind == 2) {
            pass = compareDouble(doubles.get(offset + row), op, doubleConstants, first, constants);
          } else if (kind == 1) {
            pass = compareLong(longs.get(offset + row), op, longConstants, first, constants);
          } else {
            pass = compareLong(ints.get(offset + row), op, longConstants, first, constants);
          }
          keep = keep && pass;
        }
        if (!keep) {
          live.set(row, (byte) 0);
        }
      }
    }
  }

  private static boolean compareLong(
      long value, int op, LongArray constants, int first, int count) {
    boolean pass;
    if (op == DevicePredicate.OP_IN) {
      pass = false;
      for (int c = 0; c < count; c++) {
        if (value == constants.get(first + c)) {
          pass = true;
        }
      }
    } else {
      long constant = constants.get(first);
      if (op == DevicePredicate.OP_LT) {
        pass = value < constant;
      } else if (op == DevicePredicate.OP_LT_EQ) {
        pass = value <= constant;
      } else if (op == DevicePredicate.OP_GT) {
        pass = value > constant;
      } else if (op == DevicePredicate.OP_GT_EQ) {
        pass = value >= constant;
      } else if (op == DevicePredicate.OP_EQ) {
        pass = value == constant;
      } else {
        pass = value != constant;
      }
    }
    return pass;
  }

  private static boolean compareDouble(
      double value, int op, DoubleArray constants, int first, int count) {
    boolean pass;
    if (op == DevicePredicate.OP_IN) {
      pass = false;
      for (int c = 0; c < count; c++) {
        if (value == constants.get(first + c)) {
          pass = true;
        }
      }
    } else {
      double constant = constants.get(first);
      if (op == DevicePredicate.OP_LT) {
        pass = value < constant;
      } else if (op == DevicePredicate.OP_LT_EQ) {
        pass = value <= constant;
      } else if (op == DevicePredicate.OP_GT) {
        pass = value > constant;
      } else if (op == DevicePredicate.OP_GT_EQ) {
        pass = value >= constant;
      } else if (op == DevicePredicate.OP_EQ) {
        pass = value == constant;
      } else {
        pass = value != constant;
      }
    }
    return pass;
  }

  /**
   * The equality-delete probe over a set sorted on the device: one byte a row, 1 when the row's key
   * (slot 0 of the INT64 buffer) is in the first {@code size[0]} sorted keys. A binary search: the
   * set is sorted once and every batch after it searches the same copy.
   */
  static void probeSortedLongs(
      KernelContext context, LongArray sorted, IntArray size, LongArray keys, ByteArray found) {
    int row = context.globalIdx;
    if (row < found.getSize()) {
      long key = keys.get(row);
      int low = 0;
      int high = size.get(0) - 1;
      byte hit = 0;
      while (low <= high) {
        int mid = (low + high) >>> 1;
        long value = sorted.get(mid);
        if (value < key) {
          low = mid + 1;
        } else if (value > key) {
          high = mid - 1;
        } else {
          hit = 1;
          low = high + 1;
        }
      }
      found.set(row, hit);
    }
  }

  /** {@link #probeSortedLongs} for INT32 keys. */
  static void probeSortedInts(
      KernelContext context, IntArray sorted, IntArray size, IntArray keys, ByteArray found) {
    int row = context.globalIdx;
    if (row < found.getSize()) {
      int key = keys.get(row);
      int low = 0;
      int high = size.get(0) - 1;
      byte hit = 0;
      while (low <= high) {
        int mid = (low + high) >>> 1;
        int value = sorted.get(mid);
        if (value < key) {
          low = mid + 1;
        } else if (value > key) {
          high = mid - 1;
        } else {
          hit = 1;
          low = high + 1;
        }
      }
      found.set(row, hit);
    }
  }

  /**
   * The key each row is grouped under: its value of the group column, or {@link #DEAD_GROUP} when
   * the row is deleted. With no group column ({@code keyOffset < 0}) every live row is group 0.
   */
  static void groupKeys(
      KernelContext context, ByteArray live, IntArray ints, int keyOffset, IntArray groups) {
    int row = context.globalIdx;
    if (row < groups.getSize()) {
      int key = DEAD_GROUP;
      if (live.get(row) == 1) {
        key = keyOffset < 0 ? 0 : ints.get(keyOffset + row);
      }
      groups.set(row, key);
    }
  }

  /**
   * Zeroes the nulls of optional value columns, so that cuDF's SUM ignores them as SQL's does.
   *
   * @param validColumn for each FP64 slot, the index of its column in the validity bytes, or -1 for
   *     a required column
   */
  static void zeroNulls(
      KernelContext context,
      DoubleArray doubles,
      ByteArray valid,
      IntArray validColumn,
      int slots,
      int stride) {
    int row = context.globalIdx;
    if (row < stride) {
      for (int s = 0; s < slots; s++) {
        int column = validColumn.get(s);
        if (column >= 0 && valid.get(column * stride + row) == 0) {
          doubles.set(s * stride + row, 0.0);
        }
      }
    }
  }

  /** Moves the surviving rows of every INT32 slot to the front, in their original order. */
  static void gatherInts(
      KernelContext context,
      IntArray positions,
      IntArray count,
      IntArray source,
      int slots,
      int stride,
      IntArray target) {
    int index = context.globalIdx;
    if (index < stride) {
      if (index < count.get(0)) {
        int row = positions.get(index);
        for (int s = 0; s < slots; s++) {
          target.set(s * stride + index, source.get(s * stride + row));
        }
      }
    }
  }

  /**
   * The v3 row lineage of each compacted row, into two INT64 slots of {@code target} from {@code
   * slot}: {@code _row_id}, the first row id of the row's data file plus its position there, and
   * {@code _last_updated_sequence_number}, the file's data sequence number -- what a reader would
   * have inherited, written out so the rewritten rows keep it. {@code fileStarts} holds where each
   * of the batch's {@code files[0]} data files starts, in order.
   */
  static void lineage(
      KernelContext context,
      IntArray positions,
      IntArray count,
      IntArray fileStarts,
      IntArray files,
      LongArray firstRowIds,
      LongArray sequences,
      int slot,
      int stride,
      LongArray target) {
    int index = context.globalIdx;
    if (index < stride) {
      if (index < count.get(0)) {
        int row = positions.get(index);
        int fileCount = files.get(0);
        int file = 0;
        while (file + 1 < fileCount && fileStarts.get(file + 1) <= row) {
          file++;
        }
        target.set(slot * stride + index, firstRowIds.get(file) + (row - fileStarts.get(file)));
        target.set((slot + 1) * stride + index, sequences.get(file));
      }
    }
  }

  /**
   * One step of the hash of an equality-delete key of several columns, the same on the host and the
   * device: the running hash mixed with a column's value, or with a constant when it is null
   * (splitmix64's finalizer). Matches are always confirmed column by column, so a collision costs a
   * comparison, never a wrong row.
   */
  static long keyHash(long hash, long value, int isNull) {
    long mixed = hash ^ (isNull == 1 ? 0x7F4A7C159E3779B9L : value);
    mixed += 0x9E3779B97F4A7C15L;
    mixed = (mixed ^ (mixed >>> 30)) * 0xBF58476D1CE4E5B9L;
    mixed = (mixed ^ (mixed >>> 27)) * 0x94D049BB133111EBL;
    return mixed ^ (mixed >>> 31);
  }

  static final long KEY_HASH_SEED = 0x2545F4914F6CDD1DL;

  /**
   * Puts the rows of a multi-column equality-delete set in hash order, from the order cuDF
   * computed: hashes, key values and null flags, a column at {@code stride}. Runs only when {@code
   * changed[0]} is 1, so a plan re-sorts only for a new set.
   */
  static void gatherKeySet(
      KernelContext context,
      IntArray changed,
      IntArray size,
      IntArray order,
      LongArray hashes,
      LongArray keys,
      ByteArray nulls,
      int columns,
      int stride,
      LongArray sortedHashes,
      LongArray sortedKeys,
      ByteArray sortedNulls) {
    int index = context.globalIdx;
    if (index < stride) {
      if (changed.get(0) == 1 && index < size.get(0)) {
        int from = order.get(index);
        sortedHashes.set(index, hashes.get(from));
        for (int c = 0; c < columns; c++) {
          sortedKeys.set((long) c * stride + index, keys.get((long) c * stride + from));
          sortedNulls.set(c * stride + index, nulls.get(c * stride + from));
        }
      }
    }
  }

  /**
   * Marks each row whose key -- several integer columns -- is in the equality-delete set, as
   * Iceberg compares them: a null matches a null. The row's hash is binary-searched among the set's
   * sorted hashes, and every entry with that hash is compared column by column.
   *
   * <p>{@code spec} holds, a key column, its kind (0 INT32 or DATE, 1 INT64 or TIMESTAMP), its slot
   * and its validity slot, or -1 when it cannot be null.
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  static void probeKeySet(
      KernelContext context,
      IntArray spec,
      int columns,
      LongArray sortedHashes,
      LongArray sortedKeys,
      ByteArray sortedNulls,
      IntArray size,
      int setStride,
      IntArray ints,
      LongArray longs,
      ByteArray valid,
      int stride,
      ByteArray found) {
    int row = context.globalIdx;
    if (row < stride) {
      long hash = KEY_HASH_SEED;
      for (int c = 0; c < columns; c++) {
        hash =
            keyHash(
                hash,
                keyValue(spec, c, row, ints, longs, stride),
                keyNull(spec, c, row, valid, stride));
      }
      int lo = 0;
      int hi = size.get(0);
      while (lo < hi) {
        int mid = (lo + hi) >>> 1;
        if (sortedHashes.get(mid) < hash) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      byte hit = 0;
      for (int j = lo; j < size.get(0) && hit == 0 && sortedHashes.get(j) == hash; j++) {
        int same = 1;
        for (int c = 0; c < columns; c++) {
          int isNull = keyNull(spec, c, row, valid, stride);
          if (sortedNulls.get(c * setStride + j) != isNull) {
            same = 0;
          } else if (isNull == 0
              && sortedKeys.get((long) c * setStride + j)
                  != keyValue(spec, c, row, ints, longs, stride)) {
            same = 0;
          }
        }
        if (same == 1) {
          hit = 1;
        }
      }
      found.set(row, hit);
    }
  }

  private static long keyValue(
      IntArray spec, int column, int row, IntArray ints, LongArray longs, int stride) {
    int slot = spec.get(3 * column + 1);
    return spec.get(3 * column) == 0
        ? (long) ints.get(slot * stride + row)
        : longs.get(slot * stride + row);
  }

  private static int keyNull(IntArray spec, int column, int row, ByteArray valid, int stride) {
    int validSlot = spec.get(3 * column + 2);
    return validSlot >= 0 && valid.get(validSlot * stride + row) == 0 ? 1 : 0;
  }

  /**
   * Packs each string of {@code strings} cuDF-layout columns into its INT64 slot: up to 8 bytes,
   * big-endian, zero-padded, sign bit flipped -- so signed order is byte order, as Iceberg orders
   * strings. A longer value sets {@code errors[s]}; the host refuses the batch only when that
   * column is compared on the device. Column s has its {@code stride + 1} offsets at {@code s *
   * (stride + 1)} and its bytes at {@code s * charsStride}; a null packs to whatever, its validity
   * byte says it is null.
   */
  static void packStrings(
      KernelContext context,
      IntArray meta,
      IntArray offsets,
      ByteArray chars,
      int strings,
      int firstSlot,
      int stride,
      int charsStride,
      LongArray longs,
      IntArray errors) {
    int row = context.globalIdx;
    if (row < stride) {
      if (row < meta.get(META_ROWS)) {
        for (int s = 0; s < strings; s++) {
          int from = offsets.get(s * (stride + 1) + row);
          int length = offsets.get(s * (stride + 1) + row + 1) - from;
          long packed = 0L;
          if (length > 8) {
            errors.set(s, 1);
          } else {
            long base = (long) s * charsStride + from;
            for (int b = 0; b < 8; b++) {
              long next = b < length ? (chars.get((int) (base + b)) & 0xFFL) : 0L;
              packed = (packed << 8) | next;
            }
          }
          longs.set((long) (firstSlot + s) * stride + row, packed ^ Long.MIN_VALUE);
        }
      }
    }
  }

  /** The first key column of a row a batch drops: its group is discarded on the host. */
  static final long DEAD_KEY = Long.MIN_VALUE;

  /** A null group-key value. */
  static final long NULL_KEY = Long.MIN_VALUE + 1;

  /**
   * Group-key columns as INT64, for a group-by over several keys: each key column's value widened
   * (a packed string as it is), {@link #NULL_KEY} for a null; a dropped row gets {@link #DEAD_KEY}
   * in the first. With no key columns every live row gets key 0. {@code spec} holds, a key, its
   * kind (0 INT32, 1 INT64), slot and validity slot (or -1).
   */
  static void groupKeyColumns(
      KernelContext context,
      IntArray spec,
      int keys,
      ByteArray live,
      IntArray ints,
      LongArray longs,
      ByteArray valid,
      int stride,
      LongArray out) {
    int row = context.globalIdx;
    if (row < stride) {
      if (live.get(row) == 0) {
        out.set(row, DEAD_KEY);
        for (int k = 1; k < keys; k++) {
          out.set((long) k * stride + row, 0L);
        }
      } else if (keys == 0) {
        out.set(row, 0L);
      } else {
        for (int k = 0; k < keys; k++) {
          int slot = spec.get(3 * k + 1);
          int validSlot = spec.get(3 * k + 2);
          long value;
          if (validSlot >= 0 && valid.get(validSlot * stride + row) == 0) {
            value = NULL_KEY;
          } else if (spec.get(3 * k) == 0) {
            value = ints.get(slot * stride + row);
          } else {
            value = longs.get((long) slot * stride + row);
          }
          out.set((long) k * stride + row, value);
        }
      }
    }
  }

  /** Group keys a dense group-by holds, the null key's slot not counted. */
  static final int DENSE_SLOTS = 4096;

  /** Copies of a dense group-by's sums: blocks add into copy {@code group % DENSE_COPIES}. */
  static final int DENSE_COPIES = 32;

  /**
   * A group-by over one integer key whose values lie in a small range ({@code dense}: on, the
   * smallest key, the slots with the null key's last): each live row adds its value columns to its
   * key's slot in one of {@link #DENSE_COPIES} copies, so the blocks of a batch contend on as many
   * addresses as the copies hold, not on a few hash slots. A key outside the range is counted in
   * {@code misses}.
   */
  static void denseGroups(
      KernelContext context,
      LongArray dense,
      IntArray spec,
      ByteArray live,
      IntArray ints,
      LongArray longs,
      ByteArray valid,
      int stride,
      DoubleArray values,
      int columns,
      DoubleArray copies,
      IntArray misses) {
    int row = context.globalIdx;
    if (row < stride && dense.get(0) != 0 && live.get(row) != 0) {
      int slots = (int) dense.get(2);
      int validSlot = spec.get(2);
      long slot;
      if (validSlot >= 0 && valid.get(validSlot * stride + row) == 0) {
        slot = slots - 1L;
      } else if (spec.get(0) == 0) {
        slot = ints.get(spec.get(1) * stride + row) - dense.get(1);
        slot = slot < slots - 1 ? slot : -1;
      } else {
        slot = longs.get((long) spec.get(1) * stride + row) - dense.get(1);
        slot = slot < slots - 1 ? slot : -1;
      }
      if (slot < 0) {
        context.atomicAdd(misses, 0, 1);
      } else {
        int base = ((context.groupIdx % DENSE_COPIES) * (DENSE_SLOTS + 1) + (int) slot) * columns;
        for (int c = 0; c < columns; c++) {
          double value = values.get((long) c * stride + row);
          if (value != 0.0) {
            context.atomicAdd(copies, base + c, value);
          }
        }
      }
    }
  }

  /**
   * Sums the copies of {@link #denseGroups} into the output a hash group-by writes -- keys, then
   * the value columns of each slot, {@code stride} apart, and the slot count -- and zeroes them for
   * the next batch. Slots no row reached have a row count of 0.
   */
  static void denseReduce(
      KernelContext context,
      LongArray dense,
      int columns,
      DoubleArray copies,
      int stride,
      LongArray keys,
      DoubleArray sums,
      IntArray groups) {
    if (dense.get(0) != 0) {
      int slots = (int) dense.get(2);
      for (int t = context.globalIdx; t < slots * columns; t += context.globalGroupSizeX) {
        int slot = t / columns;
        int column = t - slot * columns;
        double total = 0.0;
        for (int copy = 0; copy < DENSE_COPIES; copy++) {
          int at = (copy * (DENSE_SLOTS + 1) + slot) * columns + column;
          total += copies.get(at);
          copies.set(at, 0.0);
        }
        sums.set((long) column * stride + slot, total);
        if (column == 0) {
          keys.set(slot, slot == slots - 1 ? NULL_KEY : dense.get(1) + slot);
        }
      }
      if (context.globalIdx == 0) {
        groups.set(0, slots);
      }
    }
  }

  /**
   * Ints of {@code spec} a value: its factor count, then kind, slot and validity slot a factor,
   * then the index of its non-null count column ({@code -1} for a value that cannot be null).
   */
  /**
   * Per value: factor count, 3 ints a factor, the part (DeviceValue.WHOLE..LOW), the count column.
   */
  static final int VALUE_SPEC_INTS = 3 + 3 * DeviceValue.MAX_FACTORS;

  /**
   * Each {@link DeviceValue} of every live row, a column at {@code stride}, then a column of 1 a
   * live row to count with, then for each value that can be null a column of 1 where it is not: a
   * product of {@code (offset + scale * column)} factors, 0 when a factor's column is null, as
   * SQL's SUM skips it. A dropped row gets 0 everywhere. {@code constants} holds offset and scale a
   * factor, {@code 2 * MAX_FACTORS} a value.
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  static void valueColumns(
      KernelContext context,
      IntArray spec,
      DoubleArray constants,
      int values,
      ByteArray live,
      IntArray ints,
      LongArray longs,
      DoubleArray doubles,
      ByteArray valid,
      int stride,
      DoubleArray out) {
    int row = context.globalIdx;
    if (row < stride) {
      int alive = live.get(row);
      for (int v = 0; v < values; v++) {
        int base = v * VALUE_SPEC_INTS;
        // A sum of terms, each a product of factors: a factor with a non-zero coefficient starts a
        // new term, the coefficient its sign and weight.
        double total = 0.0;
        double product = 0.0;
        int isNull = 0;
        for (int f = 0; f < spec.get(base); f++) {
          int kind = spec.get(base + 1 + 3 * f);
          int slot = spec.get(base + 2 + 3 * f);
          int validSlot = spec.get(base + 3 + 3 * f);
          if (validSlot >= 0 && valid.get(validSlot * stride + row) == 0) {
            isNull = 1;
          }
          double operand;
          if (kind == 0) {
            operand = ints.get(slot * stride + row);
          } else if (kind == 1) {
            operand = longs.get((long) slot * stride + row);
          } else {
            operand = doubles.get((long) slot * stride + row);
          }
          int factorBase = 3 * (v * DeviceValue.MAX_FACTORS + f);
          double coefficient = constants.get(factorBase + 2);
          if (coefficient != 0.0) {
            total += product;
            product = coefficient;
          }
          product = product * (constants.get(factorBase) + constants.get(factorBase + 1) * operand);
        }
        total += product;
        // A part of the (integer) value: high * 2^40 + middle * 2^20 + low, exact in doubles.
        int part = spec.get(base + VALUE_SPEC_INTS - 2);
        if (part != 0) {
          double high = TornadoMath.floor(total / 1099511627776.0);
          double rest = total - high * 1099511627776.0;
          double middle = TornadoMath.floor(rest / 1048576.0);
          if (part == 1) {
            total = high;
          } else if (part == 2) {
            total = middle;
          } else {
            total = rest - middle * 1048576.0;
          }
        }
        out.set((long) v * stride + row, alive == 1 && isNull == 0 ? total : 0.0);
        int countColumn = spec.get(base + VALUE_SPEC_INTS - 1);
        if (countColumn >= 0) {
          out.set(
              (long) (values + 1 + countColumn) * stride + row,
              alive == 1 && isNull == 0 ? 1.0 : 0.0);
        }
      }
      out.set((long) values * stride + row, alive == 1 ? 1.0 : 0.0);
    }
  }

  /** Most columns a Z-order key interleaves; Spark's rewrites rarely use more. */
  static final int MAX_ZORDER_COLUMNS = 4;

  /**
   * Iceberg's Z-order value of every row, as {@code columns + 1} INT64 sort words a row at {@code
   * stride}: word 0 is 0 for a live row and 1 otherwise, so the rows a batch drops sort after the
   * rows it keeps; words 1.. are the interleaved bits, flipped in their sign bit so that a signed
   * comparison orders them as Iceberg's unsigned bytes do.
   *
   * <p>Each column is first made order-preserving as {@code ZOrderByteUtils} does: an integer has
   * its sign bit flipped, a double {@code bits ^ ((bits >> 63) | MIN_VALUE)} (NaN canonical, as
   * {@code doubleToLongBits}), and a null is all zeros, as Spark's Z-order function writes it. The
   * bits then interleave most significant first, one from each column in turn -- {@code
   * interleaveBits} with every column 8 bytes wide.
   *
   * <p>{@code spec} holds, a column, its kind (0 INT32 or DATE, 1 INT64 or TIMESTAMP, 2 FLOAT64),
   * its slot in that kind's buffer and its validity slot, or -1 when it cannot be null.
   */
  static void zorderKeys(
      KernelContext context,
      IntArray spec,
      int columns,
      ByteArray live,
      IntArray ints,
      LongArray longs,
      DoubleArray doubles,
      ByteArray valid,
      int stride,
      LongArray keys) {
    int row = context.globalIdx;
    if (row < stride) {
      if (live.get(row) == 0) {
        keys.set(row, 1L);
        for (int w = 1; w <= columns; w++) {
          keys.set((long) w * stride + row, Long.MAX_VALUE);
        }
      } else {
        keys.set(row, 0L);
        long u0 = orderedBits(spec, 0, columns, row, ints, longs, doubles, valid, stride);
        long u1 = orderedBits(spec, 1, columns, row, ints, longs, doubles, valid, stride);
        long u2 = orderedBits(spec, 2, columns, row, ints, longs, doubles, valid, stride);
        long u3 = orderedBits(spec, 3, columns, row, ints, longs, doubles, valid, stride);
        long word = 0L;
        int filled = 0;
        int out = 1;
        for (int b = 63; b >= 0; b--) {
          for (int c = 0; c < columns; c++) {
            long bits = c == 0 ? u0 : c == 1 ? u1 : c == 2 ? u2 : u3;
            word = word | (((bits >>> b) & 1L) << (63 - filled));
            filled++;
            if (filled == 64) {
              keys.set((long) out * stride + row, word ^ Long.MIN_VALUE);
              out++;
              word = 0L;
              filled = 0;
            }
          }
        }
      }
    }
  }

  /**
   * A row's place in a table's sort order, as {@code 1 + 2 * columns} INT64 sort words a row at
   * {@code stride}, for a lexicographic signed sort: word 0 is 0 for a live row and 1 otherwise, as
   * in {@link #zorderKeys}; then, a column, its null word (a null sorts first or last as the sort
   * field's null order says) and its value word -- the value made order-preserving as {@link
   * #zorderKeys} does, so doubles order as {@code Double.compare} does with NaN last, and
   * complemented for a descending field.
   *
   * <p>{@code spec} is {@link #zorderKeys}'s; {@code orders} holds a column's flags: bit 0
   * descending, bit 1 nulls first.
   */
  static void sortOrderKeys(
      KernelContext context,
      IntArray spec,
      IntArray orders,
      int columns,
      ByteArray live,
      IntArray ints,
      LongArray longs,
      DoubleArray doubles,
      ByteArray valid,
      int stride,
      LongArray keys) {
    int row = context.globalIdx;
    if (row < stride) {
      if (live.get(row) == 0) {
        keys.set(row, 1L);
        for (int w = 1; w <= 2 * columns; w++) {
          keys.set((long) w * stride + row, Long.MAX_VALUE);
        }
      } else {
        keys.set(row, 0L);
        for (int c = 0; c < columns; c++) {
          int validSlot = spec.get(3 * c + 2);
          int flags = orders.get(c);
          long nullWord = 0L;
          long word = 0L;
          if (validSlot >= 0 && valid.get(validSlot * stride + row) == 0) {
            if ((flags & 2) == 0) {
              nullWord = 1L;
            }
          } else {
            if ((flags & 2) != 0) {
              nullWord = 1L;
            }
            word = orderedBits(spec, c, columns, row, ints, longs, doubles, valid, stride);
            word = word ^ Long.MIN_VALUE;
            if ((flags & 1) == 1) {
              word = ~word;
            }
          }
          keys.set((long) (1 + 2 * c) * stride + row, nullWord);
          keys.set((long) (2 + 2 * c) * stride + row, word);
        }
      }
    }
  }

  // value != value is NaN in a form the kernel compiler takes; Double.isNaN may not be.
  @SuppressWarnings("IdentityBinaryExpression")
  private static long orderedBits(
      IntArray spec,
      int column,
      int columns,
      int row,
      IntArray ints,
      LongArray longs,
      DoubleArray doubles,
      ByteArray valid,
      int stride) {
    long bits = 0L;
    if (column < columns) {
      int kind = spec.get(3 * column);
      int slot = spec.get(3 * column + 1);
      int validSlot = spec.get(3 * column + 2);
      if (validSlot < 0 || valid.get(validSlot * stride + row) == 1) {
        if (kind == 0) {
          bits = ((long) ints.get(slot * stride + row)) ^ Long.MIN_VALUE;
        } else if (kind == 1) {
          bits = longs.get(slot * stride + row) ^ Long.MIN_VALUE;
        } else {
          double value = doubles.get(slot * stride + row);
          long raw = value != value ? 0x7ff8000000000000L : Double.doubleToRawLongBits(value);
          bits = raw ^ ((raw >> 63) | Long.MIN_VALUE);
        }
      }
    }
    return bits;
  }

  /** {@link #gatherInts} for INT64 slots. */
  static void gatherLongs(
      KernelContext context,
      IntArray positions,
      IntArray count,
      LongArray source,
      int slots,
      int stride,
      LongArray target) {
    int index = context.globalIdx;
    if (index < stride) {
      if (index < count.get(0)) {
        int row = positions.get(index);
        for (int s = 0; s < slots; s++) {
          target.set(s * stride + index, source.get(s * stride + row));
        }
      }
    }
  }

  /** {@link #gatherInts} for FP64 slots. */
  static void gatherDoubles(
      KernelContext context,
      IntArray positions,
      IntArray count,
      DoubleArray source,
      int slots,
      int stride,
      DoubleArray target) {
    int index = context.globalIdx;
    if (index < stride) {
      if (index < count.get(0)) {
        int row = positions.get(index);
        for (int s = 0; s < slots; s++) {
          target.set(s * stride + index, source.get(s * stride + row));
        }
      }
    }
  }

  /** {@link #gatherInts} for validity bytes. */
  static void gatherBytes(
      KernelContext context,
      IntArray positions,
      IntArray count,
      ByteArray source,
      int slots,
      int stride,
      ByteArray target) {
    int index = context.globalIdx;
    if (index < stride) {
      if (index < count.get(0)) {
        int row = positions.get(index);
        for (int s = 0; s < slots; s++) {
          target.set(s * stride + index, source.get(s * stride + row));
        }
      }
    }
  }

  /**
   * Groups live rows by Iceberg's {@code bucket[N]} partition transform of a long column: Murmur3
   * x86 32-bit of the value's 8 little-endian bytes, seed 0, made non-negative and taken modulo N
   * -- the function of {@code BucketUtil}, so a row lands in the bucket Iceberg would write it to.
   * Deleted and padding rows get {@link #DEAD_GROUP}.
   */
  static void bucketGroupKeys(
      KernelContext context,
      ByteArray live,
      LongArray longs,
      int offset,
      int buckets,
      IntArray groups) {
    int row = context.globalIdx;
    if (row < groups.getSize()) {
      int key = DEAD_GROUP;
      if (live.get(row) == 1) {
        long value = longs.get(offset + row);
        int h1 = mixH1(0, mixK1((int) value));
        h1 = mixH1(h1, mixK1((int) (value >>> 32)));
        key = (fmix(h1, 8) & Integer.MAX_VALUE) % buckets;
      }
      groups.set(row, key);
    }
  }

  private static int mixK1(int value) {
    int k1 = value * 0xcc9e2d51;
    k1 = (k1 << 15) | (k1 >>> 17);
    return k1 * 0x1b873593;
  }

  private static int mixH1(int hash, int k1) {
    int h1 = hash ^ k1;
    h1 = (h1 << 13) | (h1 >>> 19);
    return h1 * 5 + 0xe6546b64;
  }

  private static int fmix(int hash, int length) {
    int h1 = hash ^ length;
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;
    return h1;
  }
}
