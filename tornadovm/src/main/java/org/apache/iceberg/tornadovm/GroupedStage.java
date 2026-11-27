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

import java.util.List;
import uk.ac.manchester.tornado.api.KernelContext;
import uk.ac.manchester.tornado.api.TaskGraph;
import uk.ac.manchester.tornado.api.TornadoExecutionPlan;
import uk.ac.manchester.tornado.api.TornadoExecutionResult;
import uk.ac.manchester.tornado.api.types.arrays.ByteArray;
import uk.ac.manchester.tornado.api.types.arrays.DoubleArray;
import uk.ac.manchester.tornado.api.types.arrays.IntArray;
import uk.ac.manchester.tornado.api.types.arrays.LongArray;

/**
 * An extension of the device's grouped aggregate ({@link DeviceMergeOnReadScan#aggregate(List,
 * List, GroupedStage)}): kernels that run on each batch's rows between the scan's own -- after the
 * live-row filter, after the group keys, after the value columns -- and, optionally, the grouping
 * itself. The scan knows nothing of what a stage does; joins are stages ({@code
 * org.apache.iceberg.tornadovm.join}).
 *
 * <p>A stage describes one run. The scan keeps device buffers per aggregate and stage {@link
 * #shape()}: a later run of the same shape {@link Instance#bind binds} its data to the buffers the
 * first one opened.
 */
public interface GroupedStage {
  /** Fact columns the stage's kernels read, besides the aggregate's group and value columns. */
  List<String> columns();

  /**
   * Value columns the stage fills itself, after the aggregate's own, each with a non-null count
   * column: the aggregate's results hold them in this order.
   */
  int values();

  /** What the stage's buffers depend on: runs of one shape share them. */
  String shape();

  /** The stage's buffers and kernels for one device stream. */
  Instance open(Context context);

  /**
   * After the run's last batch, once: a stage whose instances fold their groups across batches
   * instead of merging each batch's into the result ({@link Instance#collect}) reports them here.
   */
  default void finish(GroupedAggregate result) {}

  /** A stage on one device stream. */
  interface Instance {
    /** Takes a run's data, which reaches the device with the next batch ({@link #stage}). */
    void bind(GroupedStage run);

    /** The names of its kernels, each run over the batch's rows. */
    List<String> tasks();

    /** Kernels after the live-row filter: they may drop rows ({@link Context#live()}). */
    default void afterFilter(TaskGraph graph) {}

    /** Kernels after the group-key columns are written ({@link Context#keys()}). */
    default void afterKeys(TaskGraph graph) {}

    /** Kernels after the value columns are written ({@link Context#values()}). */
    default void afterValues(TaskGraph graph) {}

    /** Whether the stage groups the rows itself; cuDF does otherwise. */
    default boolean groups() {
      return false;
    }

    /**
     * A batch's groups, when the stage groups: merged into {@code result}, or folded until {@link
     * GroupedStage#finish}; its live rows.
     */
    default long collect(TornadoExecutionResult executed, GroupedAggregate result) {
      throw new UnsupportedOperationException("The stage does not group");
    }

    /** Before a batch runs: puts on the device what the last {@link #bind} changed. */
    void stage(TornadoExecutionPlan plan);
  }

  /** What a stage sees of the scan's buffers for one device stream. */
  interface Context {
    KernelContext kernel();

    /** Rows of a batch: every per-row array's stride. */
    int capacity();

    /** Host buffers the stream owns and frees. */
    IntArray ints(long elements);

    LongArray longs(long elements);

    DoubleArray doubles(long elements);

    /** Per row, whether it is still live (1). */
    ByteArray live();

    /** The read columns, by {@link Column#slot()}, and their validity by {@link Column#valid()}. */
    IntArray intColumns();

    LongArray longColumns();

    DoubleArray doubleColumns();

    ByteArray validity();

    /** The group-key columns, as INT64, {@link #capacity()} rows each. */
    LongArray keys();

    /** The value columns: sums, the row count, then non-null counts, {@link #capacity()} each. */
    DoubleArray values();

    /** Group-key columns. */
    int groupKeys();

    /** Value columns, the aggregate's and the stage's. */
    int valueCount();

    /** Value {@code value}'s non-null count column (after the row count), or -1 for none. */
    int countColumn(int value);

    /** Where a fact column's values are read. */
    Column column(String name);
  }

  /**
   * A read column: its kind (0 INT32, 1 INT64, 2 FP64), its slot in the array of its kind, and its
   * validity's slot in {@link Context#validity()} (-1 when it cannot be null).
   */
  record Column(int kind, int slot, int valid) {}
}
