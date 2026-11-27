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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.LongStream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;
import org.apache.iceberg.tornadovm.DeviceValue;
import org.apache.iceberg.tornadovm.bench.MorTables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Star joins on the device ({@link StarAggregation}) against IcebergGenerics' rows of the same
 * tables, under position and equality deletes: dimension joins and gathers, dense grouping, probes
 * with key parts, comparisons, build-side sums and left outer joins.
 */
public class TestStarAggregation {
  @TempDir private Path temp;

  @BeforeAll
  public static void deviceMustBeAvailable() {
    assumeThat(DeviceMergeOnReadScan.isAvailable())
        .as("TornadoVM with a cuDF shim that has readParquetColumns")
        .isTrue();
  }

  private static Record row(Schema schema, long fileIndex, long rowIndex) {
    GenericRecord record = GenericRecord.create(schema);
    record.setField("id", fileIndex * 1_000_000L + rowIndex);
    record.setField("k", (int) (rowIndex % 7));
    record.setField("v1", rowIndex * 0.25);
    record.setField("v2", (double) -rowIndex);
    record.setField("v3", (double) (rowIndex % 3));
    record.setField("v4", 1.0);
    return record;
  }

  private static List<DataFile> writeFiles(Table table, int firstIndex, long... rows) {
    List<DataFile> files = Lists.newArrayList();
    for (int f = 0; f < rows.length; f++) {
      long index = firstIndex + f;
      files.add(MorTables.writeDataFile(table, rows[f], i -> row(table.schema(), index, i)));
    }
    MorTables.append(table, files);
    return files;
  }

  private static long[] everyNth(long rows, int every) {
    return LongStream.range(0, rows).filter(i -> i % every == 0).toArray();
  }

  private static long[] randomPositions(long rows, double fraction, long seed) {
    Random random = new Random(seed);
    return LongStream.range(0, rows).filter(i -> random.nextDouble() < fraction).toArray();
  }

  /**
   * Star aggregates: inner joins with dimensions given as masks and codes over a key range, grouped
   * by an attribute and a fact column, on one scan whose dimensions change between runs -- a mask
   * over part of the keys, then a range starting below the keys, with other codes.
   */
  @Test
  public void testStarAggregate() throws IOException {
    Table table = MorTables.create(temp.resolve("star").toString(), MorTables.SCHEMA, 2);
    List<DataFile> files = writeFiles(table, 0, 3000, 4000);
    MorTables.deletePositions(
        table, files, ImmutableList.of(everyNth(3000, 5), randomPositions(4000, 0.2, 3)));
    MorTables.deleteEqual(table, "id", ImmutableList.of(1L, 2L, 1_000_003L));
    table.refresh();

    try (DeviceMergeOnReadScan device =
        DeviceMergeOnReadScan.of(table, org.apache.iceberg.expressions.Expressions.alwaysTrue())) {
      // k is 0..6. First: keys 0..6 with 1, 2, 4, 5 kept, parity as the attribute; and a second
      // dimension on k over keys 2..5 only, which drops k = 1 and 6 by range.
      boolean[] keep = {false, true, true, false, true, true, false};
      int[] parity = {0, 1, 0, 1, 0, 1, 0};
      assertStarMatches(
          table,
          device,
          ImmutableList.of(
              DeviceDimension.of("k", 0, keep).withAttribute(parity),
              DeviceDimension.of("k", 2, new boolean[] {true, true, true, true})),
          k -> k >= 2 && k <= 5 && keep[k],
          k -> parity[k],
          true);
      // An attribute alone: grouped densely on the device, not by cuDF.
      assertStarMatches(
          table,
          device,
          ImmutableList.of(DeviceDimension.of("k", 0, keep).withAttribute(parity)),
          k -> keep[k],
          k -> parity[k],
          false);
      // Then: keys from -3, every one kept, k / 3 + 1 as the attribute.
      boolean[] all = new boolean[10];
      Arrays.fill(all, true);
      int[] third = new int[10];
      for (int key = -3; key < 7; key++) {
        third[key + 3] = Math.floorDiv(key, 3) + 1;
      }
      assertStarMatches(
          table,
          device,
          ImmutableList.of(DeviceDimension.of("k", -3, all).withAttribute(third)),
          k -> true,
          k -> k / 3 + 1,
          true);
    }
  }

  /**
   * A star aggregate joined with a small build table ({@link DeviceProbe}): the key a fact column
   * plus a dimension attribute's code, build keys repeated or absent, and a comparison of a fact
   * column with each build row's value; each row counts once a build row it joins.
   */
  @Test
  public void testStarAggregateWithProbe() throws IOException {
    Table table = MorTables.create(temp.resolve("probe").toString(), MorTables.SCHEMA, 2);
    List<DataFile> files = writeFiles(table, 0, 2000, 3000);
    MorTables.deletePositions(
        table, files, ImmutableList.of(everyNth(2000, 7), randomPositions(3000, 0.1, 5)));
    MorTables.deleteEqual(table, "id", ImmutableList.of(3L, 1_000_004L));
    table.refresh();

    int[] parity = {0, 1, 0, 1, 0, 1, 0};
    boolean[] all = new boolean[7];
    Arrays.fill(all, true);
    // Build rows keyed parity * 10 + k: k = 1 (twice), 2, 4 (three times), 6 never.
    long[] keys = {11, 11, 2, 4, 4, 4, 15};
    long[] values = {10, 1, 5, 0, 7, 9, 2};
    DeviceProbe probe =
        DeviceProbe.of(keys, values)
            .keyAttribute(0, 0, 10)
            .keyColumn("k", 1)
            .where("k", DeviceProbe.Comparison.LESS);

    Map<List<Object>, double[]> expected = Maps.newHashMap();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        int kValue = (Integer) record.getField("k");
        long key = parity[kValue] * 10L + kValue;
        int weight = 0;
        for (int b = 0; b < keys.length; b++) {
          if (keys[b] == key && kValue < values[b]) {
            weight++;
          }
        }
        if (weight > 0) {
          double[] totals =
              expected.computeIfAbsent(
                  ImmutableList.of((long) parity[kValue]), ignored -> new double[2]);
          totals[0] += weight * (Double) record.getField("v1");
          totals[1] += weight;
        }
      }
    }
    try (DeviceMergeOnReadScan device =
        DeviceMergeOnReadScan.of(table, org.apache.iceberg.expressions.Expressions.alwaysTrue())) {
      for (int repeat = 0; repeat < 2; repeat++) {
        Map<List<Object>, double[]> actual =
            StarAggregation.run(
                    device,
                    ImmutableList.of(DeviceDimension.of("k", 0, all).withAttribute(parity)),
                    ImmutableList.of(DeviceDimension.Key.attribute(0, 0)),
                    ImmutableList.of(DeviceValue.of("v1")),
                    probe,
                    ImmutableList.of())
                .groups();
        assertThat(actual.keySet()).isEqualTo(expected.keySet());
        for (Map.Entry<List<Object>, double[]> entry : expected.entrySet()) {
          double[] got = actual.get(entry.getKey());
          assertThat(got[0])
              .as("%s sum", entry.getKey())
              .isCloseTo(entry.getValue()[0], org.assertj.core.data.Offset.offset(1e-6));
          assertThat(got[1]).as("%s rows", entry.getKey()).isEqualTo(entry.getValue()[1]);
        }
      }
    }
  }

  /**
   * A probe grouped by a fact column, which cuDF groups: build keys that repeat weigh each fact row
   * by their count, in its sums and its row count alike (k = 2 joins two build rows, k = 4 three).
   * No dimension: the fact joins the build side alone (q64's sales with their returns).
   */
  @Test
  public void testGroupedProbeWeights() throws IOException {
    Table table = MorTables.create(temp.resolve("weights").toString(), MorTables.SCHEMA, 2);
    writeFiles(table, 0, 1500);
    table.refresh();
    boolean[] all = new boolean[7];
    Arrays.fill(all, true);
    long[] keys = {2, 2, 4, 4, 4};
    DeviceProbe probe = DeviceProbe.of(keys, new long[keys.length]).keyColumn("k", 1, 0, 7);
    Map<List<Object>, double[]> expected = Maps.newHashMap();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        int kValue = (Integer) record.getField("k");
        int weight = kValue == 2 ? 2 : kValue == 4 ? 3 : 0;
        if (weight > 0) {
          double[] totals =
              expected.computeIfAbsent(ImmutableList.of((long) kValue), ignored -> new double[2]);
          totals[0] += weight * (Double) record.getField("v1");
          totals[1] += weight;
        }
      }
    }
    try (DeviceMergeOnReadScan device =
        DeviceMergeOnReadScan.of(table, org.apache.iceberg.expressions.Expressions.alwaysTrue())) {
      Map<List<Object>, double[]> actual =
          StarAggregation.run(
                  device,
                  ImmutableList.of(),
                  ImmutableList.of(DeviceDimension.Key.column("k")),
                  ImmutableList.of(DeviceValue.of("v1")),
                  probe,
                  ImmutableList.of())
              .groups();
      assertThat(actual.keySet()).isEqualTo(expected.keySet());
      for (Map.Entry<List<Object>, double[]> entry : expected.entrySet()) {
        double[] got = actual.get(entry.getKey());
        assertThat(got[1]).as("%s rows", entry.getKey()).isEqualTo(entry.getValue()[1]);
        assertThat(got[0])
            .as("%s sum", entry.getKey())
            .isCloseTo(entry.getValue()[0], org.assertj.core.data.Offset.offset(1e-6));
      }
    }
  }

  /**
   * A probe key part with an offset and a range: a fact value outside the range joins nothing, even
   * when its offset value would equal a build key (k = 5 against keys 0..2 of k - 2 with a range of
   * 3, where 5 - 2 = 3 is one past the range and a second part would make it alias).
   */
  @Test
  public void testProbeKeyRanges() throws IOException {
    Table table = MorTables.create(temp.resolve("ranges").toString(), MorTables.SCHEMA, 2);
    writeFiles(table, 0, 1500);
    table.refresh();
    boolean[] all = new boolean[7];
    Arrays.fill(all, true);
    int[] none = new int[7];
    // Key = (k - 2) * 1 + (code 0) * 3 over k in [2, 5): build keys 0 (k = 2) and 2 (k = 4); k = 5
    // gives part 3, outside the range, so it must not join key 3 (none) or anything else.
    long[] keys = {0, 2, 3};
    long[] values = {0, 0, 0};
    DeviceProbe probe =
        DeviceProbe.of(keys, values).keyColumn("k", 1, 2, 3).keyAttribute(0, 0, 3, 0, 1);
    Map<List<Object>, double[]> expected = Maps.newHashMap();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        int kValue = (Integer) record.getField("k");
        if (kValue == 2 || kValue == 4) {
          double[] totals =
              expected.computeIfAbsent(ImmutableList.of((long) kValue), ignored -> new double[2]);
          totals[0] += (Double) record.getField("v1");
          totals[1] += 1;
        }
      }
    }
    try (DeviceMergeOnReadScan device =
        DeviceMergeOnReadScan.of(table, org.apache.iceberg.expressions.Expressions.alwaysTrue())) {
      Map<List<Object>, double[]> actual =
          StarAggregation.run(
                  device,
                  ImmutableList.of(DeviceDimension.of("k", 0, all).withAttribute(none)),
                  ImmutableList.of(DeviceDimension.Key.column("k")),
                  ImmutableList.of(DeviceValue.of("v1")),
                  probe,
                  ImmutableList.of())
              .groups();
      assertThat(actual.keySet()).isEqualTo(expected.keySet());
      for (Map.Entry<List<Object>, double[]> entry : expected.entrySet()) {
        assertThat(actual.get(entry.getKey())[1]).isEqualTo(entry.getValue()[1]);
        assertThat(actual.get(entry.getKey())[0])
            .isCloseTo(entry.getValue()[0], org.assertj.core.data.Offset.offset(1e-6));
      }
    }
  }

  /**
   * A probe whose build rows carry a value column ({@link StarAggregation.ProbeSum}): each fact row
   * adds the values of the build rows it joins (one is null, skipped and not counted), next to its
   * own weighted sum and row count.
   */
  @Test
  public void testProbeBuildSums() throws IOException {
    Table table = MorTables.create(temp.resolve("sums").toString(), MorTables.SCHEMA, 2);
    List<DataFile> files = writeFiles(table, 0, 2500);
    MorTables.deletePositions(table, files, ImmutableList.of(everyNth(2500, 9)));
    table.refresh();
    int[] parity = {0, 1, 0, 1, 0, 1, 0};
    boolean[] all = new boolean[7];
    Arrays.fill(all, true);
    long[] keys = {1, 1, 2, 4, 4, 6};
    long[] values = new long[keys.length];
    double[] amounts = {10.5, 2.0, Double.NaN, 1.25, 3.0, 7.0};
    DeviceProbe probe = DeviceProbe.of(keys, values, ImmutableList.of(amounts)).keyColumn("k", 1);

    Map<List<Object>, double[]> expected = Maps.newHashMap();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        int kValue = (Integer) record.getField("k");
        int weight = 0;
        double sum = 0;
        int nonNull = 0;
        for (int b = 0; b < keys.length; b++) {
          if (keys[b] == kValue) {
            weight++;
            if (!Double.isNaN(amounts[b])) {
              sum += amounts[b];
              nonNull++;
            }
          }
        }
        if (weight > 0) {
          double[] totals =
              expected.computeIfAbsent(
                  ImmutableList.of((long) parity[kValue]), ignored -> new double[4]);
          totals[0] += weight * (Double) record.getField("v1");
          totals[1] += sum;
          totals[2] += weight;
          totals[3] += nonNull;
        }
      }
    }
    try (DeviceMergeOnReadScan device =
        DeviceMergeOnReadScan.of(table, org.apache.iceberg.expressions.Expressions.alwaysTrue())) {
      Map<List<Object>, double[]> actual =
          StarAggregation.run(
                  device,
                  ImmutableList.of(DeviceDimension.of("k", 0, all).withAttribute(parity)),
                  ImmutableList.of(DeviceDimension.Key.attribute(0, 0)),
                  ImmutableList.of(DeviceValue.of("v1")),
                  probe,
                  ImmutableList.of(StarAggregation.ProbeSum.of(0)))
              .groups();
      assertThat(actual.keySet()).isEqualTo(expected.keySet());
      for (Map.Entry<List<Object>, double[]> entry : expected.entrySet()) {
        double[] got = actual.get(entry.getKey()); // v1, probe, rows, v1 non-null, probe non-null
        double[] want = entry.getValue();
        assertThat(got[0]).isCloseTo(want[0], org.assertj.core.data.Offset.offset(1e-6));
        assertThat(got[1]).isCloseTo(want[1], org.assertj.core.data.Offset.offset(1e-9));
        assertThat(got[2]).isEqualTo(want[2]);
        assertThat(got[4]).isEqualTo(want[3]);
      }
    }
  }

  /** A left outer probe: unmatched fact rows count once, their build sums 0. */
  @Test
  public void testLeftOuterProbe() throws IOException {
    Table table = MorTables.create(temp.resolve("outer").toString(), MorTables.SCHEMA, 2);
    writeFiles(table, 0, 2100);
    table.refresh();
    int[] parity = {0, 1, 0, 1, 0, 1, 0};
    boolean[] all = new boolean[7];
    Arrays.fill(all, true);
    long[] keys = {1, 2, 2, 5};
    double[] amounts = {1.5, Double.NaN, 4.0, 2.25};
    DeviceProbe probe =
        DeviceProbe.of(keys, new long[keys.length], ImmutableList.of(amounts))
            .keyColumn("k", 1)
            .leftOuter();
    Map<List<Object>, double[]> expected = Maps.newHashMap();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record r : records) {
        int kValue = (Integer) r.getField("k");
        int weight = 0;
        double sum = 0;
        for (int b = 0; b < keys.length; b++) {
          if (keys[b] == kValue) {
            weight++;
            sum += Double.isNaN(amounts[b]) ? 0 : amounts[b];
          }
        }
        double[] totals =
            expected.computeIfAbsent(
                ImmutableList.of((long) parity[kValue]), ignored -> new double[3]);
        totals[0] += Math.max(1, weight) * (Double) r.getField("v1");
        totals[1] += sum;
        totals[2] += Math.max(1, weight);
      }
    }
    try (DeviceMergeOnReadScan device =
        DeviceMergeOnReadScan.of(table, org.apache.iceberg.expressions.Expressions.alwaysTrue())) {
      Map<List<Object>, double[]> actual =
          StarAggregation.run(
                  device,
                  ImmutableList.of(DeviceDimension.of("k", 0, all).withAttribute(parity)),
                  ImmutableList.of(DeviceDimension.Key.attribute(0, 0)),
                  ImmutableList.of(DeviceValue.of("v1")),
                  probe,
                  ImmutableList.of(StarAggregation.ProbeSum.of(0)))
              .groups();
      assertThat(actual.keySet()).isEqualTo(expected.keySet());
      for (Map.Entry<List<Object>, double[]> e : expected.entrySet()) {
        double[] got = actual.get(e.getKey());
        assertThat(got[0]).isCloseTo(e.getValue()[0], org.assertj.core.data.Offset.offset(1e-6));
        assertThat(got[1]).isCloseTo(e.getValue()[1], org.assertj.core.data.Offset.offset(1e-9));
        assertThat(got[2]).isEqualTo(e.getValue()[2]);
      }
    }
  }

  private static void assertStarMatches(
      Table table,
      DeviceMergeOnReadScan device,
      List<DeviceDimension> dimensions,
      java.util.function.IntPredicate joins,
      java.util.function.IntUnaryOperator attribute,
      boolean factKey)
      throws IOException {
    Map<List<Object>, double[]> expected = Maps.newHashMap();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        int kValue = (Integer) record.getField("k");
        if (joins.test(kValue)) {
          List<Object> key =
              factKey
                  ? ImmutableList.of((long) attribute.applyAsInt(kValue), (long) kValue)
                  : ImmutableList.of((long) attribute.applyAsInt(kValue));
          double[] totals = expected.computeIfAbsent(key, ignored -> new double[2]);
          totals[0] += (Double) record.getField("v1");
          totals[1] += 1;
        }
      }
    }
    Map<List<Object>, double[]> actual =
        StarAggregation.run(
                device,
                dimensions,
                factKey
                    ? ImmutableList.of(
                        DeviceDimension.Key.attribute(0, 0), DeviceDimension.Key.column("k"))
                    : ImmutableList.of(DeviceDimension.Key.attribute(0, 0)),
                ImmutableList.of(DeviceValue.of("v1")))
            .groups();
    assertThat(actual.keySet()).isEqualTo(expected.keySet());
    for (Map.Entry<List<Object>, double[]> entry : expected.entrySet()) {
      double[] got = actual.get(entry.getKey());
      assertThat(got[0])
          .as("%s sum", entry.getKey())
          .isCloseTo(entry.getValue()[0], org.assertj.core.data.Offset.offset(1e-6));
      assertThat(got[1]).as("%s rows", entry.getKey()).isEqualTo(entry.getValue()[1]);
    }
  }
}
