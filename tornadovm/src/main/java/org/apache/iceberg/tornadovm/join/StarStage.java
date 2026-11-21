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

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.tornadovm.GroupedAggregate;
import org.apache.iceberg.tornadovm.GroupedStage;
import uk.ac.manchester.tornado.api.DataRange;
import uk.ac.manchester.tornado.api.TaskGraph;
import uk.ac.manchester.tornado.api.TornadoExecutionPlan;
import uk.ac.manchester.tornado.api.TornadoExecutionResult;
import uk.ac.manchester.tornado.api.enums.DataTransferMode;
import uk.ac.manchester.tornado.api.types.arrays.DoubleArray;
import uk.ac.manchester.tornado.api.types.arrays.IntArray;
import uk.ac.manchester.tornado.api.types.arrays.LongArray;

/**
 * A star join as a stage of the device's grouped aggregate: the fact rows that join every dimension
 * (by key lookup), optionally joined to a {@link DeviceProbe}'s build table, grouped by dimension
 * attributes and fact columns. One run's data; its shape -- the joins, the keys, the room their
 * arrays need rounded up to a power of two -- lets later runs reuse the device buffers.
 */
final class StarStage implements GroupedStage {
  // Most ints of masks and codes the dimensions take on the device (1 GiB).
  private static final long MAX_DIMENSION_INTS = 1L << 28;
  // Most groups a star of dimension codes alone aggregates densely, not through cuDF.
  private static final long MAX_DENSE_GROUPS = 1L << 20;

  private final List<DeviceDimension> dimensions;
  private final List<DeviceDimension.Key> keys;
  private final DeviceProbe probe;
  private final List<StarAggregation.ProbeSum> probeSums;
  // The aggregate's own values: the probe sums' value columns follow them.
  private final int factValues;
  private final long capacity;
  // Per key, how many codes it has and its weight in a dense group's index; the groups, and the
  // dense array's room (0 when cuDF groups).
  private final int[] cardinalities;
  private final int[] multipliers;
  private final int denseGroups;
  private final int denseCapacity;
  private final int probeCapacity;
  // The run's dense groups folded over its batches on the host: per column of the dense output,
  // denseGroups totals. Merging every batch's groups into the result's map would cost a boxed key
  // a group a batch (q23's ~500K customers: ~1 s a star).
  private double[] folded;
  private int[] foldedCounts;

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  StarStage(
      List<DeviceDimension> dimensions,
      List<DeviceDimension.Key> keys,
      DeviceProbe probe,
      List<StarAggregation.ProbeSum> probeSums,
      int factValues) {
    this.dimensions = dimensions;
    this.keys = keys;
    this.probe = probe;
    this.probeSums = probeSums;
    this.factValues = factValues;
    if (probe == null && !probeSums.isEmpty()) {
      throw new IllegalArgumentException("Probe sums without a probe");
    }
    for (StarAggregation.ProbeSum sum : probeSums) {
      if (sum.column() >= probe.sums().size() || sum.maskedBy() >= factValues) {
        throw new IllegalArgumentException("No such build column or masking value: " + sum);
      }
    }
    this.probeCapacity =
        probe == null ? 0 : (int) Long.highestOneBit(Math.max(1, probe.rows() - 1)) << 1;
    this.cardinalities = new int[keys.size()];
    this.multipliers = new int[keys.size()];
    long groups = keys.isEmpty() ? 0 : 1;
    for (int k = keys.size() - 1; k >= 0; k--) {
      DeviceDimension.Key key = keys.get(k);
      if (!key.isAttribute()) {
        groups = 0;
        break;
      }
      int[] codes = dimensions.get(key.dimension()).attributes().get(key.attribute());
      cardinalities[k] = 1 + Arrays.stream(codes).max().orElse(0);
      multipliers[k] = (int) Math.min(groups, Integer.MAX_VALUE);
      groups *= cardinalities[k];
      if (groups > MAX_DENSE_GROUPS) {
        groups = 0;
        break;
      }
    }
    this.denseGroups = (int) groups;
    this.denseCapacity = groups == 0 ? 0 : (int) Long.highestOneBit(Math.max(1, groups - 1)) << 1;
    long ints = dimensions.stream().mapToLong(DeviceDimension::ints).sum();
    if (ints > MAX_DIMENSION_INTS) {
      throw new UnsupportedOperationException(
          "Dimensions of " + ints + " ints; the device takes up to " + MAX_DIMENSION_INTS);
    }
    this.capacity = Long.highestOneBit(Math.max(1, ints - 1)) << 1;
  }

  /** The fact columns the aggregate groups by: an attribute's foreign key, or the column. */
  List<String> groupColumns() {
    List<String> columns = Lists.newArrayList();
    for (DeviceDimension.Key key : keys) {
      columns.add(key.isAttribute() ? dimensions.get(key.dimension()).factColumn() : key.column());
    }
    return columns;
  }

  @Override
  public List<String> columns() {
    List<String> columns = Lists.newArrayList();
    dimensions.forEach(d -> columns.add(d.factColumn()));
    if (probe != null) {
      for (DeviceProbe.Part part : probe.parts()) {
        if (!part.isAttribute()) {
          columns.add(part.column());
        }
      }
      if (probe.column() != null) {
        columns.add(probe.column());
      }
    }
    return columns;
  }

  @Override
  public int values() {
    return probeSums.size();
  }

  @Override
  public String shape() {
    StringBuilder shape = new StringBuilder("star[");
    dimensions.forEach(d -> shape.append(d.factColumn()).append(','));
    shape.append(keys).append(capacity).append('/').append(denseCapacity);
    if (probe != null) {
      shape
          .append(" probe ")
          .append(probe.parts())
          .append(probe.column())
          .append(probe.outer() ? " outer " : " ")
          .append(probeCapacity)
          .append(" sums ")
          .append(probe.sums().size())
          .append(probeSums);
    }
    return shape.append(']').toString();
  }

  @Override
  public Instance open(Context context) {
    return new Stream(context);
  }

  /**
   * Adds a batch's dense output ({@code columns} columns of {@code denseCapacity} doubles: the
   * sums, the row count, the non-null counts) to the run's totals; returns the batch's live rows.
   */
  private synchronized long fold(DoubleArray out, int columns, int[] countColumns) {
    if (folded == null) {
      folded = new double[columns * denseGroups];
      foldedCounts = countColumns;
    }
    for (int c = 0; c < columns; c++) {
      long from = (long) c * denseCapacity;
      int to = c * denseGroups;
      for (int g = 0; g < denseGroups; g++) {
        folded[to + g] += out.get(from + g);
      }
    }
    double rows = 0;
    for (int g = 0; g < denseGroups; g++) {
      rows += out.get((long) countColumns.length * denseCapacity + g);
    }
    return (long) rows;
  }

  /** Every dense group with rows, its key the codes its index holds. */
  @Override
  public synchronized void finish(GroupedAggregate result) {
    if (folded == null) {
      return;
    }
    int values = foldedCounts.length;
    double[] sums = new double[values];
    double[] nonNull = new double[values];
    for (int g = 0; g < denseGroups; g++) {
      double rows = folded[values * denseGroups + g];
      if (rows == 0) {
        continue;
      }
      List<Object> key = Lists.newArrayListWithCapacity(keys.size());
      for (int k = 0; k < keys.size(); k++) {
        key.add((long) ((g / multipliers[k]) % cardinalities[k]));
      }
      for (int v = 0; v < values; v++) {
        sums[v] = folded[v * denseGroups + g];
        int count = foldedCounts[v];
        nonNull[v] = count < 0 ? rows : folded[(values + 1 + count) * denseGroups + g];
      }
      result.add(key, sums, rows, nonNull);
    }
    folded = null;
  }

  /** The star's buffers and kernels on one device stream. */
  private final class Stream implements Instance {
    private final Context context;
    private final IntArray joinSpec;
    private final IntArray gatherSpec;
    private final IntArray dimensionData;
    private final IntArray denseSpec;
    private final DoubleArray denseOut;
    private final boolean probing;
    private final IntArray probeSpec;
    private final LongArray buildKeys;
    private final LongArray buildValues;
    private final DoubleArray weights;
    private final DoubleArray buildSums;
    private final DoubleArray rowSums;
    private final IntArray probeValueSpec;
    private final int columns;
    private StarStage current;
    private boolean changed;

    Stream(Context context) {
      this.context = context;
      int rows = context.capacity();
      this.joinSpec =
          context.ints(Math.max(1L, (long) dimensions.size() * JoinKernels.JOIN_SPEC_INTS));
      this.gatherSpec = context.ints(Math.max(1L, (long) gathers() * JoinKernels.GATHER_SPEC_INTS));
      this.dimensionData = context.ints(capacity);
      this.denseSpec = context.ints(Math.max(1, keys.size()));
      this.columns = context.valueCount() + 1 + countColumns();
      this.denseOut = context.doubles(Math.max(1L, (long) denseCapacity * columns));
      this.probing = probe != null;
      this.probeSpec =
          context.ints(
              probing
                  ? JoinKernels.PROBE_HEADER_INTS
                      + (long) probe.parts().size() * JoinKernels.PROBE_PART_INTS
                  : 1);
      this.buildKeys = context.longs(probing ? probeCapacity : 1);
      this.buildValues = context.longs(probing ? probeCapacity : 1);
      this.weights = context.doubles(probing ? rows : 1);
      int buildColumns = probing ? probe.sums().size() : 0;
      this.buildSums = context.doubles(Math.max(1L, (long) buildColumns * probeCapacity));
      this.rowSums = context.doubles(Math.max(1L, 2L * buildColumns * rows));
      this.probeValueSpec =
          context.ints(Math.max(1L, (long) probeSums.size() * JoinKernels.PROBE_VALUE_INTS));
      for (int j = 0; j < probeSums.size(); j++) {
        StarAggregation.ProbeSum sum = probeSums.get(j);
        int base = j * JoinKernels.PROBE_VALUE_INTS;
        probeValueSpec.set(base, factValues + j);
        probeValueSpec.set(base + 1, sum.column());
        probeValueSpec.set(base + 2, context.countColumn(factValues + j));
        probeValueSpec.set(base + 3, sum.maskedBy() < 0 ? -1 : context.countColumn(sum.maskedBy()));
      }
    }

    private int countColumns() {
      int counts = 0;
      for (int v = 0; v < context.valueCount(); v++) {
        counts += context.countColumn(v) >= 0 ? 1 : 0;
      }
      return counts;
    }

    @Override
    public void bind(GroupedStage run) {
      StarStage star = (StarStage) run;
      if (star.capacity > dimensionData.getSize()) {
        throw new IllegalStateException("Dimensions outgrew the stream; the shape should differ");
      }
      if (probing && star.probe.rows() > buildKeys.getSize()) {
        throw new IllegalStateException("The probe outgrew the stream; the shape should differ");
      }
      star.fill(context, joinSpec, gatherSpec, dimensionData, denseSpec);
      if (probing) {
        star.fillProbe(context, probeSpec, buildKeys, buildValues, buildSums);
      }
      current = star;
      changed = true;
    }

    @Override
    public List<String> tasks() {
      return List.of(
          "starFilter",
          "starKeys",
          "probeWeights",
          "applyWeights",
          "probeValues",
          "denseClear",
          "denseAggregate");
    }

    @Override
    public void afterFilter(TaskGraph graph) {
      graph
          .transferToDevice(
              DataTransferMode.FIRST_EXECUTION, joinSpec, gatherSpec, dimensionData, denseSpec)
          .task(
              "starFilter",
              JoinKernels::starFilter,
              context.kernel(),
              joinSpec,
              dimensions.size(),
              dimensionData,
              context.intColumns(),
              context.longColumns(),
              context.validity(),
              context.capacity(),
              context.live());
      if (probing) {
        graph
            .transferToDevice(
                DataTransferMode.FIRST_EXECUTION,
                probeSpec,
                buildKeys,
                buildValues,
                buildSums,
                probeValueSpec)
            .task(
                "probeWeights",
                JoinKernels::probeWeights,
                context.kernel(),
                probeSpec,
                dimensionData,
                buildKeys,
                buildValues,
                context.intColumns(),
                context.longColumns(),
                context.validity(),
                context.capacity(),
                context.live(),
                weights,
                buildSums,
                rowSums);
      }
    }

    @Override
    public void afterKeys(TaskGraph graph) {
      if (gathers() > 0) {
        graph.task(
            "starKeys",
            JoinKernels::starKeys,
            context.kernel(),
            gatherSpec,
            gathers(),
            dimensionData,
            context.live(),
            context.capacity(),
            context.keys());
      }
    }

    @Override
    public void afterValues(TaskGraph graph) {
      if (probing) {
        graph.task(
            "applyWeights",
            JoinKernels::applyWeights,
            context.kernel(),
            weights,
            columns,
            context.capacity(),
            context.values());
        if (!probeSums.isEmpty()) {
          graph.task(
              "probeValues",
              JoinKernels::probeValues,
              context.kernel(),
              probeValueSpec,
              probeSums.size(),
              rowSums,
              context.valueCount(),
              context.capacity(),
              context.values());
        }
      }
      if (denseCapacity > 0) {
        graph
            .task(
                "denseClear",
                JoinKernels::denseClear,
                context.kernel(),
                columns * denseCapacity,
                context.capacity(),
                denseOut)
            .task(
                "denseAggregate",
                JoinKernels::denseAggregate,
                context.kernel(),
                denseSpec,
                context.groupKeys(),
                context.live(),
                context.keys(),
                context.values(),
                columns,
                context.capacity(),
                denseCapacity,
                denseOut)
            .transferToHost(DataTransferMode.UNDER_DEMAND, denseOut);
      }
    }

    @Override
    public boolean groups() {
      return denseCapacity > 0;
    }

    /** A dense batch, folded into the run's totals ({@link StarStage#finish}). */
    @Override
    public long collect(TornadoExecutionResult executed, GroupedAggregate result) {
      int values = context.valueCount();
      executed.transferToHost(
          new DataRange(denseOut).withOffset(0).withSize((long) columns * denseCapacity));
      int[] countColumns = new int[values];
      for (int v = 0; v < values; v++) {
        countColumns[v] = context.countColumn(v);
      }
      return current.fold(denseOut, columns, countColumns);
    }

    @Override
    public void stage(TornadoExecutionPlan plan) {
      if (changed) {
        plan.transferToDevice(joinSpec, gatherSpec, dimensionData, denseSpec);
        if (probing) {
          plan.transferToDevice(probeSpec, buildKeys, buildValues, buildSums);
        }
        changed = false;
      }
    }
  }

  private int gathers() {
    return (int) keys.stream().filter(DeviceDimension.Key::isAttribute).count();
  }

  /** Writes the specs and the arrays: per dimension its mask, then its attributes' codes. */
  private void fill(
      Context context, IntArray joinSpec, IntArray gatherSpec, IntArray data, IntArray denseSpec) {
    for (int k = 0; k < keys.size() && denseCapacity > 0; k++) {
      denseSpec.set(k, multipliers[k]);
    }
    int offset = 0;
    for (int d = 0; d < dimensions.size(); d++) {
      DeviceDimension dimension = dimensions.get(d);
      GroupedStage.Column column = context.column(dimension.factColumn());
      int base = d * JoinKernels.JOIN_SPEC_INTS;
      joinSpec.set(base, column.kind());
      joinSpec.set(base + 1, column.slot());
      joinSpec.set(base + 2, column.valid());
      joinSpec.set(base + 3, dimension.firstKey());
      joinSpec.set(base + 4, dimension.keys());
      joinSpec.set(base + 5, offset);
      copy(dimension.keep(), data, offset);
      offset += dimension.keys();
      for (int[] codes : dimension.attributes()) {
        copy(codes, data, offset);
        offset += dimension.keys();
      }
    }
    int gather = 0;
    for (int k = 0; k < keys.size(); k++) {
      DeviceDimension.Key key = keys.get(k);
      if (key.isAttribute()) {
        DeviceDimension dimension = dimensions.get(key.dimension());
        int base = gather * JoinKernels.GATHER_SPEC_INTS;
        gatherSpec.set(base, k);
        gatherSpec.set(base + 1, dimension.firstKey());
        gatherSpec.set(base + 2, dimension.keys());
        gatherSpec.set(base + 3, attributeOffset(key.dimension(), key.attribute()));
        gather++;
      }
    }
  }

  /** Where attribute {@code attribute} of dimension {@code d} starts in the dimensions' data. */
  private int attributeOffset(int dimension, int attribute) {
    int offset = 0;
    for (int i = 0; i < dimension; i++) {
      offset += (int) dimensions.get(i).ints();
    }
    return offset + (1 + attribute) * dimensions.get(dimension).keys();
  }

  private void fillProbe(
      Context context,
      IntArray probeSpec,
      LongArray buildKeys,
      LongArray buildValues,
      DoubleArray buildSums) {
    probeSpec.set(0, probe.parts().size());
    probeSpec.set(1, probe.rows());
    probeSpec.set(2, probe.comparison().code());
    if (probe.column() != null) {
      GroupedStage.Column column = context.column(probe.column());
      probeSpec.set(3, column.kind());
      probeSpec.set(4, column.slot());
      probeSpec.set(5, column.valid());
    }
    probeSpec.set(6, probe.sums().size());
    probeSpec.set(7, probeCapacity);
    probeSpec.set(8, probe.outer() ? 1 : 0);
    for (int p = 0; p < probe.parts().size(); p++) {
      DeviceProbe.Part part = probe.parts().get(p);
      int base = JoinKernels.PROBE_HEADER_INTS + p * JoinKernels.PROBE_PART_INTS;
      GroupedStage.Column column =
          context.column(
              part.isAttribute() ? dimensions.get(part.dimension()).factColumn() : part.column());
      probeSpec.set(base, part.isAttribute() ? 1 : 0);
      probeSpec.set(base + 1, column.kind());
      probeSpec.set(base + 2, column.slot());
      if (part.isAttribute()) {
        probeSpec.set(base + 3, dimensions.get(part.dimension()).firstKey());
        probeSpec.set(base + 4, attributeOffset(part.dimension(), part.attribute()));
      }
      probeSpec.set(base + 5, (int) part.multiplier());
      probeSpec.set(base + 6, part.offset());
      probeSpec.set(base + 7, part.range());
      probeSpec.set(base + 8, (int) (part.multiplier() >>> 32));
    }
    MemorySegment.copy(
        MemorySegment.ofArray(probe.keys()),
        0,
        buildKeys.getSegment(),
        0,
        (long) probe.rows() * Long.BYTES);
    MemorySegment.copy(
        MemorySegment.ofArray(probe.values()),
        0,
        buildValues.getSegment(),
        0,
        (long) probe.rows() * Long.BYTES);
    for (int k = 0; k < probe.sums().size(); k++) {
      MemorySegment.copy(
          MemorySegment.ofArray(probe.sums().get(k)),
          0,
          buildSums.getSegment(),
          (long) k * probeCapacity * Double.BYTES,
          (long) probe.rows() * Double.BYTES);
    }
  }

  private static void copy(int[] from, IntArray to, int offset) {
    MemorySegment.copy(
        MemorySegment.ofArray(from),
        0,
        to.getSegment(),
        (long) offset * Integer.BYTES,
        (long) from.length * Integer.BYTES);
  }
}
