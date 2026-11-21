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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.LongStream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.tornadovm.bench.MorTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Every device scan is checked against Iceberg's own merge-on-read reader, {@link IcebergGenerics},
 * on tables that exercise one delete mechanism each and then all of them at once.
 */
public class TestDeviceMergeOnRead {

  private static final String[] SUMS = {"v1", "v2", "v3", "v4"};

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

  /** Position deletes, then equality deletes, then a file appended after them: v2. */
  @Test
  public void testPositionAndEqualityDeletesV2() throws IOException {
    Table table = MorTables.create(temp.resolve("v2").toString(), MorTables.SCHEMA, 2);
    List<DataFile> files = writeFiles(table, 0, 1000, 2500, 700);
    MorTables.deletePositions(
        table,
        files,
        ImmutableList.of(everyNth(1000, 10), randomPositions(2500, 0.3, 1), new long[0]));
    MorTables.deleteEqual(
        table, "id", ImmutableList.of(5L, 7L, 1_000_123L, 1_000_124L, 2_000_699L, 42_000_000L));
    // Appended after the equality delete, so id 5 in this file must survive it.
    List<DataFile> late = writeFiles(table, 0, 300);
    MorTables.deleteEqual(table, "id", ImmutableList.of(6L, 2_000_000L));

    assertMatchesIceberg(table);
    assertThat(late).hasSize(1);
  }

  /**
   * Concurrent readers of one table with more projections than the JVM keeps pools for: pools are
   * evicted and rebuilt while others read. A stopped region must never take a newer request's batch
   * into its freed buffers (it crashed the JVM in a native copy before each generation of regions
   * got its own queue); every read must return the table's live rows.
   */
  @Test
  public void testConcurrentReadersRebuildingPools() throws Exception {
    Table table = MorTables.create(temp.resolve("pools").toString(), MorTables.SCHEMA, 2);
    List<DataFile> files = writeFiles(table, 0, 4000, 3000, 2000, 1000);
    MorTables.deletePositions(
        table,
        files,
        ImmutableList.of(
            everyNth(4000, 7), randomPositions(3000, 0.2, 3), new long[0], everyNth(1000, 3)));
    MorTables.deleteEqual(table, "id", ImmutableList.of(11L, 1_000_013L, 2_000_017L, 3_000_019L));
    table.refresh();
    long expectedRows = 0;
    long expectedIds = 0;
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        expectedRows++;
        expectedIds += (Long) record.getField("id");
      }
    }
    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(planned);
    }
    // Requests of different sizes on the same projections: a pool's regions are rebuilt bigger,
    // or freed, while other requests are between sizing them and reading.
    List<List<FileScanTask>> subsets =
        ImmutableList.of(tasks, tasks.subList(0, 1), tasks.subList(0, 2), tasks.subList(3, 4));
    List<List<String>> projections =
        ImmutableList.of(
            ImmutableList.of("id"),
            ImmutableList.of("id", "k", "v1"),
            ImmutableList.of("id", "v1", "v2", "v3", "v4"));
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      long[][] expected = new long[subsets.size()][];
      for (int s = 0; s < subsets.size(); s++) {
        java.util.concurrent.atomic.LongAdder sum = new java.util.concurrent.atomic.LongAdder();
        long live =
            device.materialize(
                subsets.get(s),
                ImmutableList.of("id"),
                batch -> {
                  for (int r = 0; r < batch.rows(); r++) {
                    sum.add(batch.getLong("id", r));
                  }
                });
        expected[s] = new long[] {live, sum.sum()};
      }
      assertThat(expected[0]).containsExactly(expectedRows, expectedIds);
      java.util.concurrent.ExecutorService readers =
          java.util.concurrent.Executors.newFixedThreadPool(8);
      List<java.util.concurrent.Future<?>> futures = Lists.newArrayList();
      for (int t = 0; t < 8; t++) {
        int thread = t;
        futures.add(
            readers.submit(
                () -> {
                  for (int i = 0; i < 24; i++) {
                    int subset = (thread + i * 3) % subsets.size();
                    List<String> columns = projections.get((thread * 5 + i) % projections.size());
                    java.util.concurrent.atomic.LongAdder sum =
                        new java.util.concurrent.atomic.LongAdder();
                    long live =
                        device.materialize(
                            subsets.get(subset),
                            columns,
                            batch -> {
                              for (int r = 0; r < batch.rows(); r++) {
                                sum.add(batch.getLong("id", r));
                              }
                            });
                    assertThat(live)
                        .as("%s of subset %s", columns, subset)
                        .isEqualTo(expected[subset][0]);
                    assertThat(sum.sum())
                        .as("ids of subset %s", subset)
                        .isEqualTo(expected[subset][1]);
                  }
                  return null;
                }));
      }
      for (java.util.concurrent.Future<?> future : futures) {
        future.get();
      }
      readers.shutdown();
    }
  }

  /** A value that is a sum of terms: v1 - v2 + v3 * v4, per group of k, under deletes. */
  @Test
  public void testSumOfTerms() throws IOException {
    Table table = MorTables.create(temp.resolve("terms").toString(), MorTables.SCHEMA, 2);
    List<DataFile> files = writeFiles(table, 0, 3000);
    MorTables.deletePositions(table, files, ImmutableList.of(everyNth(3000, 4)));
    table.refresh();
    Map<List<Object>, double[]> expected = Maps.newHashMap();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record r : records) {
        double value =
            (Double) r.getField("v1")
                - (Double) r.getField("v2")
                + (Double) r.getField("v3") * (Double) r.getField("v4");
        double[] totals =
            expected.computeIfAbsent(
                ImmutableList.of((long) (Integer) r.getField("k")), ignored -> new double[2]);
        totals[0] += value;
        totals[1] += 1;
      }
    }
    try (DeviceMergeOnReadScan device =
        DeviceMergeOnReadScan.of(table, org.apache.iceberg.expressions.Expressions.alwaysTrue())) {
      Map<List<Object>, double[]> actual =
          device
              .aggregate(
                  ImmutableList.of("k"),
                  ImmutableList.of(
                      DeviceValue.of("v1")
                          .minus(DeviceValue.of("v2"))
                          .plus(DeviceValue.of("v3").times("v4"))))
              .groups();
      assertThat(actual.keySet()).isEqualTo(expected.keySet());
      for (Map.Entry<List<Object>, double[]> e : expected.entrySet()) {
        assertThat(actual.get(e.getKey())[0])
            .isCloseTo(e.getValue()[0], org.assertj.core.data.Offset.offset(1e-6));
        assertThat(actual.get(e.getKey())[1]).isEqualTo(e.getValue()[1]);
      }
    }
  }

  /** Deletion vectors and equality deletes: v3. */
  @Test
  public void testDeletionVectorsV3() throws IOException {
    Table table = MorTables.create(temp.resolve("v3").toString(), MorTables.SCHEMA, 3);
    List<DataFile> files = writeFiles(table, 0, 4000, 3000);
    MorTables.deletePositions(
        table, files, ImmutableList.of(randomPositions(4000, 0.05, 2), everyNth(3000, 3)));
    MorTables.deleteEqual(table, "id", ImmutableList.of(1L, 2L, 1_000_002L));
    assertMatchesIceberg(table);
  }

  /** Only appends: the scan must equal a plain read. */
  @Test
  public void testNoDeletes() throws IOException {
    Table table = MorTables.create(temp.resolve("plain").toString(), MorTables.SCHEMA, 2);
    writeFiles(table, 0, 1234, 1);
    assertMatchesIceberg(table);
  }

  /** Small row groups, so positions have to stay right across row-group boundaries. */
  @Test
  public void testManyRowGroups() throws IOException {
    Table table =
        MorTables.create(
            temp.resolve("groups").toString(),
            MorTables.SCHEMA,
            2,
            ImmutableMap.of(
                TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "16384",
                TableProperties.PARQUET_PAGE_ROW_LIMIT, "500"));
    List<DataFile> files = writeFiles(table, 0, 60_000);
    MorTables.deletePositions(table, files, ImmutableList.of(randomPositions(60_000, 0.2, 3)));
    assertMatchesIceberg(table);
  }

  /**
   * Optional columns: a null key is deleted only by a null in the delete set, and a null value is
   * skipped by SUM but its row still counts.
   */
  @Test
  public void testNullsV2() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "k", Types.IntegerType.get()),
            Types.NestedField.optional(3, "v1", Types.DoubleType.get()),
            Types.NestedField.required(4, "v2", Types.DoubleType.get()),
            Types.NestedField.required(5, "v3", Types.DoubleType.get()),
            Types.NestedField.required(6, "v4", Types.DoubleType.get()));
    Table table = MorTables.create(temp.resolve("nulls").toString(), schema, 2);
    DataFile file =
        MorTables.writeDataFile(
            table,
            2000,
            i -> {
              Record record = row(schema, 0, i);
              if (i % 5 == 0) {
                record.setField("id", null);
              }
              if (i % 4 == 0) {
                record.setField("v1", null);
              }
              return record;
            });
    MorTables.append(table, ImmutableList.of(file));
    MorTables.deletePositions(table, ImmutableList.of(file), ImmutableList.of(everyNth(2000, 7)));
    assertMatchesIceberg(table);

    List<Object> keys = Lists.newArrayList();
    keys.add(3L);
    keys.add(11L);
    keys.add(null);
    MorTables.deleteEqual(table, "id", keys);
    assertMatchesIceberg(table);
  }

  /**
   * Grouped by a key whose footer bounds span few values (a dense group-by, nulls in their own
   * slot) and by one whose bounds do not (the hash group-by), under position and equality deletes.
   */
  @Test
  public void testDenseGroupBy() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "k", Types.IntegerType.get()),
            Types.NestedField.required(3, "v1", Types.DoubleType.get()),
            Types.NestedField.required(4, "v2", Types.DoubleType.get()),
            Types.NestedField.required(5, "v3", Types.DoubleType.get()),
            Types.NestedField.required(6, "v4", Types.DoubleType.get()));
    Table table = MorTables.create(temp.resolve("dense").toString(), schema, 2);
    List<DataFile> files = Lists.newArrayList();
    for (int f = 0; f < 3; f++) {
      int first = f * 5000;
      files.add(
          MorTables.writeDataFile(
              table,
              5000,
              i -> {
                Record record = row(schema, 0, first + i);
                record.setField("id", (long) (first + i) * 1_000_003L);
                record.setField("k", i % 13 == 0 ? null : (int) (-40 + (first + i) % 97));
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deletePositions(
        table, files, ImmutableList.of(everyNth(5000, 7), everyNth(5000, 7), everyNth(5000, 7)));
    MorTables.deleteEqual(table, "id", ImmutableList.of(0L, 1_000_003L * 4321, 1_000_003L * 9999));
    table.refresh();
    for (String key : ImmutableList.of("k", "id")) {
      Map<List<Object>, double[]> expected = Maps.newHashMap();
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record r : records) {
          Object value = r.getField(key);
          List<Object> group = Lists.newArrayList();
          group.add(value == null ? null : ((Number) value).longValue());
          double[] totals = expected.computeIfAbsent(group, ignored -> new double[2]);
          totals[0] += (Double) r.getField("v1");
          totals[1] += 1;
        }
      }
      try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
        Map<List<Object>, double[]> actual =
            device
                .aggregate(ImmutableList.of(key), ImmutableList.of(DeviceValue.of("v1")))
                .groups();
        assertThat(actual.keySet()).as(key).isEqualTo(expected.keySet());
        for (Map.Entry<List<Object>, double[]> e : expected.entrySet()) {
          assertThat(actual.get(e.getKey())[0])
              .isCloseTo(e.getValue()[0], org.assertj.core.data.Offset.offset(1e-6));
          assertThat(actual.get(e.getKey())[1]).isEqualTo(e.getValue()[1]);
        }
      }
    }
  }

  /**
   * Equality deletes over two columns, (id, k), as CDC writes them: a pair deletes a row only when
   * both match, and a null id matches a null id. Thousands of random pairs exercise the hash
   * search; a file appended after the deletes keeps its rows.
   */
  @Test
  public void testEqualityDeletesOnTwoColumns() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "k", Types.IntegerType.get()),
            Types.NestedField.optional(3, "v1", Types.DoubleType.get()),
            Types.NestedField.required(4, "v2", Types.DoubleType.get()),
            Types.NestedField.required(5, "v3", Types.DoubleType.get()),
            Types.NestedField.required(6, "v4", Types.DoubleType.get()));
    Table table = MorTables.create(temp.resolve("eq2").toString(), schema, 2);
    List<DataFile> files = Lists.newArrayList();
    for (long f = 0; f < 3; f++) {
      long index = f;
      files.add(
          MorTables.writeDataFile(
              table,
              4000,
              i -> {
                Record record = row(schema, index, i);
                if (i % 5 == 0) {
                  record.setField("id", null);
                }
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deletePositions(
        table, files, ImmutableList.of(everyNth(4000, 9), new long[0], new long[0]));
    Random random = new Random(3);
    List<Object[]> keys = Lists.newArrayList();
    keys.add(new Object[] {3L, 3}); // matches
    keys.add(new Object[] {11L, 0}); // id 11 has k 4: no match
    keys.add(new Object[] {null, 0}); // every row with a null id and k 0
    for (int j = 0; j < 5000; j++) {
      long id = random.nextInt(3) * 1_000_000L + random.nextInt(4000);
      int kValue = random.nextBoolean() ? (int) ((id % 1_000_000L) % 7) : random.nextInt(7);
      keys.add(new Object[] {id, kValue});
    }
    MorTables.deleteEqualRows(table, ImmutableList.of("id", "k"), keys);
    writeFiles(table, 0, 500); // after the deletes: (3, 3) survives here
    assertMatchesIceberg(table);
  }

  /**
   * String columns (up to 8 bytes) as group keys and in filters, with nulls, UTF-8 and deletes;
   * sums of products of columns -- TPC-H Q1's shape -- checked against IcebergGenerics. A column
   * holding a longer string is refused, not cut.
   */
  @Test
  public void testStringGroupKeysAndValues() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "flag", Types.StringType.get()),
            Types.NestedField.optional(3, "mode", Types.StringType.get()),
            Types.NestedField.required(4, "price", Types.DoubleType.get()),
            Types.NestedField.required(5, "disc", Types.DoubleType.get()),
            Types.NestedField.required(6, "tax", Types.DoubleType.get()),
            Types.NestedField.required(7, "comment", Types.StringType.get()));
    Table table = MorTables.create(temp.resolve("strings").toString(), schema, 2);
    String[] modes = {"AIR", "REG AIR", "TRUCK", "Ωméga", "SHIP"};
    List<DataFile> files = Lists.newArrayList();
    for (long f = 0; f < 3; f++) {
      long index = f;
      files.add(
          MorTables.writeDataFile(
              table,
              5000,
              i -> {
                GenericRecord record = GenericRecord.create(schema);
                record.setField("id", index * 1_000_000L + i);
                record.setField("flag", String.valueOf("RAN".charAt((int) (i % 3))));
                record.setField("mode", i % 13 == 0 ? null : modes[(int) ((i + index) % 5)]);
                record.setField("price", 100.0 + i);
                record.setField("disc", (i % 10) / 100.0);
                record.setField("tax", (i % 8) / 100.0);
                record.setField("comment", "a comment longer than eight bytes " + i);
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deletePositions(
        table, files, ImmutableList.of(everyNth(5000, 7), new long[0], everyNth(5000, 11)));
    MorTables.deleteEqual(table, "id", ImmutableList.of(3L, 1_000_005L));
    table.refresh();

    org.apache.iceberg.expressions.Expression filter =
        org.apache.iceberg.expressions.Expressions.and(
            org.apache.iceberg.expressions.Expressions.notEqual("flag", "N"),
            org.apache.iceberg.expressions.Expressions.lessThan("price", 4000.0));
    Map<List<Object>, double[]> expected = Maps.newHashMap();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).where(filter).build()) {
      for (Record record : records) {
        double price = (Double) record.getField("price");
        double disc = (Double) record.getField("disc");
        double tax = (Double) record.getField("tax");
        List<Object> key = Lists.newArrayList(record.getField("flag"), record.getField("mode"));
        double[] totals = expected.computeIfAbsent(key, ignored -> new double[3]);
        totals[0] += price * (1 - disc);
        totals[1] += price * (1 - disc) * (1 + tax);
        totals[2] += 1;
      }
    }
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table, filter)) {
      for (int repeat = 0; repeat < 2; repeat++) {
        Map<List<Object>, double[]> actual =
            device
                .aggregate(
                    ImmutableList.of("flag", "mode"),
                    ImmutableList.of(
                        DeviceValue.of("price").timesOneMinus("disc"),
                        DeviceValue.of("price").timesOneMinus("disc").timesOnePlus("tax")))
                .groups();
        assertThat(actual.keySet()).isEqualTo(expected.keySet());
        for (Map.Entry<List<Object>, double[]> entry : expected.entrySet()) {
          double[] got = actual.get(entry.getKey());
          for (int v = 0; v < 3; v++) {
            assertThat(got[v])
                .as("%s value %d", entry.getKey(), v)
                .isCloseTo(entry.getValue()[v], org.assertj.core.data.Offset.offset(1e-6));
          }
        }
      }
    }

    // A filter on a string column the device packs; the long comment column is refused.
    try (DeviceMergeOnReadScan device =
        DeviceMergeOnReadScan.of(
            table, org.apache.iceberg.expressions.Expressions.in("mode", "AIR", "Ωméga"))) {
      long rows =
          device.aggregate(ImmutableList.of(), ImmutableList.of(DeviceValue.of("price"))).rows();
      long expectedRows = 0;
      try (CloseableIterable<Record> records =
          IcebergGenerics.read(table)
              .where(org.apache.iceberg.expressions.Expressions.in("mode", "AIR", "Ωméga"))
              .build()) {
        for (Record ignored : records) {
          expectedRows++;
        }
      }
      assertThat(rows).isEqualTo(expectedRows);
      org.assertj.core.api.Assertions.assertThatThrownBy(
              () ->
                  device.aggregate(
                      ImmutableList.of("comment"), ImmutableList.of(DeviceValue.of("price"))))
          .hasStackTraceContaining("8 bytes");
    }
  }

  /**
   * The flat schema of Iceberg's own delete benchmarks: float (read widened), date, timestamptz and
   * strings longer than 8 bytes, materialized with every value checked against IcebergGenerics,
   * under position and equality deletes. A float filter runs on the device; a filter on the long
   * string column is refused, not compared cut.
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  @Test
  public void testFloatsTimestampsAndLongStrings() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "longCol", Types.LongType.get()),
            Types.NestedField.required(2, "intCol", Types.IntegerType.get()),
            Types.NestedField.required(3, "floatCol", Types.FloatType.get()),
            Types.NestedField.optional(4, "doubleCol", Types.DoubleType.get()),
            Types.NestedField.optional(6, "dateCol", Types.DateType.get()),
            Types.NestedField.optional(7, "timestampCol", Types.TimestampType.withZone()),
            Types.NestedField.optional(8, "stringCol", Types.StringType.get()));
    Table table = MorTables.create(temp.resolve("flat").toString(), schema, 2);
    List<DataFile> files = Lists.newArrayList();
    for (long f = 0; f < 2; f++) {
      long index = f;
      files.add(
          MorTables.writeDataFile(
              table,
              3000,
              i -> {
                GenericRecord record = GenericRecord.create(schema);
                long id = index * 1_000_000L + i;
                record.setField("longCol", id);
                record.setField("intCol", (int) (id % 2147483647));
                record.setField("floatCol", (float) id / 3);
                record.setField("doubleCol", i % 9 == 0 ? null : (double) id);
                record.setField("dateCol", java.time.LocalDate.of(2026, 10, 1).plusDays(i % 30));
                record.setField(
                    "timestampCol",
                    i % 7 == 0
                        ? null
                        : java.time.OffsetDateTime.of(
                                2026, 10, 9, 18, 30, 0, 0, java.time.ZoneOffset.UTC)
                            .plusSeconds(i));
                record.setField(
                    "stringCol",
                    i % 11 == 0
                        ? null
                        : "2026-10-" + (i % 30) + "-é-" + "x".repeat((int) (i % 40)));
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deletePositions(
        table, files, ImmutableList.of(everyNth(3000, 5), randomPositions(3000, 0.1, 3)));
    MorTables.deleteEqual(table, "longCol", ImmutableList.of(7L, 1_000_008L));
    table.refresh();

    List<String> expected = Lists.newArrayList();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        expected.add(
            flatRow(
                record.getField("longCol"),
                record.getField("intCol"),
                record.getField("floatCol") == null
                    ? null
                    : ((Float) record.getField("floatCol")).doubleValue(),
                record.getField("doubleCol"),
                record.getField("dateCol") == null
                    ? null
                    : (long) ((java.time.LocalDate) record.getField("dateCol")).toEpochDay(),
                record.getField("timestampCol") == null
                    ? null
                    : org.apache.iceberg.util.DateTimeUtil.microsFromTimestamptz(
                        (java.time.OffsetDateTime) record.getField("timestampCol")),
                record.getField("stringCol")));
      }
    }
    expected.sort(String::compareTo);

    List<String> columns =
        ImmutableList.of(
            "longCol", "intCol", "floatCol", "doubleCol", "dateCol", "timestampCol", "stringCol");
    List<String> actual = java.util.Collections.synchronizedList(Lists.newArrayList());
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      for (int repeat = 0; repeat < 2; repeat++) {
        actual.clear();
        device.materialize(
            columns,
            batch -> {
              for (int r = 0; r < batch.rows(); r++) {
                actual.add(
                    flatRow(
                        batch.getLong("longCol", r),
                        (long) batch.getInt("intCol", r),
                        batch.getDouble("floatCol", r),
                        batch.isNull("doubleCol", r) ? null : batch.getDouble("doubleCol", r),
                        batch.isNull("dateCol", r) ? null : (long) batch.getInt("dateCol", r),
                        batch.isNull("timestampCol", r) ? null : batch.getLong("timestampCol", r),
                        batch.getString("stringCol", r)));
              }
            });
        List<String> sorted = Lists.newArrayList(actual);
        sorted.sort(String::compareTo);
        assertThat(sorted).containsExactlyElementsOf(expected);
      }
    }

    // A float filter runs on the device and agrees with Iceberg's evaluator.
    org.apache.iceberg.expressions.Expression floats =
        org.apache.iceberg.expressions.Expressions.greaterThan("floatCol", 500.25f);
    long expectedRows = 0;
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).where(floats).build()) {
      for (Record ignored : records) {
        expectedRows++;
      }
    }
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table, floats)) {
      assertThat(
              device
                  .aggregate(ImmutableList.of(), ImmutableList.of(DeviceValue.of("intCol")))
                  .rows())
          .isEqualTo(expectedRows);
    }
    try (DeviceMergeOnReadScan device =
        DeviceMergeOnReadScan.of(
            table, org.apache.iceberg.expressions.Expressions.equal("stringCol", "2026-10-1"))) {
      org.assertj.core.api.Assertions.assertThatThrownBy(
              () ->
                  device.aggregate(ImmutableList.of(), ImmutableList.of(DeviceValue.of("intCol"))))
          .hasStackTraceContaining("8 bytes");
    }
  }

  /**
   * Files batch apart when their string columns together need more than the one device buffer they
   * share. ClickBench's q24 (SELECT * over hits, dozens of string columns) batched a few files
   * whose strings were beyond it and failed; here the buffer holds one file's strings but not two,
   * which the row budget alone would batch together.
   */
  @Test
  public void testStringColumnsBeyondOneBufferBatchApart() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "url", Types.StringType.get()),
            Types.NestedField.required(3, "title", Types.StringType.get()));
    Table table = MorTables.create(temp.resolve("buffer").toString(), schema, 2);
    List<DataFile> files = Lists.newArrayList();
    for (long f = 0; f < 8; f++) {
      long index = f;
      files.add(
          MorTables.writeDataFile(
              table,
              3000,
              i -> {
                GenericRecord record = GenericRecord.create(schema);
                record.setField("id", index * 1_000_000L + i);
                record.setField(
                    "url",
                    i % 13 == 0 ? null : "https://example.com/" + "p".repeat((int) (i % 50)));
                record.setField("title", "title " + index + " " + i);
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deletePositions(
        table,
        files.subList(0, 2),
        ImmutableList.of(everyNth(3000, 7), randomPositions(3000, 0.1, 5)));
    table.refresh();

    long most = 0;
    for (DataFile file : files) {
      for (int fieldId : new int[] {2, 3}) {
        long bytes = HostDeletes.stringBytes(LocalFiles.path(file.location(), table.io()), fieldId);
        assertThat(bytes).as("string bytes in the footer of %s", file.location()).isPositive();
        most = Math.max(most, bytes);
      }
    }

    List<String> expected = Lists.newArrayList();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        expected.add(
            record.getField("id") + "|" + record.getField("url") + "|" + record.getField("title"));
      }
    }
    expected.sort(String::compareTo);

    long saved = DeviceMergeOnReadScan.stringBuffer;
    // Room for one file's strings in each of the two columns, not for two files'.
    DeviceMergeOnReadScan.stringBuffer = 2 * (most * 3 / 2 + 4096);
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      List<String> actual = java.util.Collections.synchronizedList(Lists.newArrayList());
      device.materialize(
          ImmutableList.of("id", "url", "title"),
          batch -> {
            for (int r = 0; r < batch.rows(); r++) {
              actual.add(
                  batch.getLong("id", r)
                      + "|"
                      + batch.getString("url", r)
                      + "|"
                      + batch.getString("title", r));
            }
          });
      List<String> sorted = Lists.newArrayList(actual);
      sorted.sort(String::compareTo);
      assertThat(sorted).containsExactlyElementsOf(expected);
    } finally {
      DeviceMergeOnReadScan.stringBuffer = saved;
    }
  }

  /**
   * Decimal columns of up to 18 digits: read as their unscaled values, filtered by a decimal
   * literal, an equality-delete key, a group key, and summed exactly -- each against Iceberg's own
   * reader.
   */
  @Test
  public void testDecimalColumns() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "code", Types.DecimalType.of(10, 2)),
            Types.NestedField.required(3, "price", Types.DecimalType.of(7, 2)),
            Types.NestedField.optional(4, "amount", Types.DecimalType.of(18, 4)),
            Types.NestedField.required(5, "band", Types.DecimalType.of(3, 1)));
    Table table = MorTables.create(temp.resolve("decimals").toString(), schema, 2);
    List<DataFile> files = Lists.newArrayList();
    for (long f = 0; f < 3; f++) {
      long index = f;
      files.add(
          MorTables.writeDataFile(
              table,
              4000,
              i -> {
                long id = index * 1_000_000L + i;
                GenericRecord record = GenericRecord.create(schema);
                record.setField("id", id);
                record.setField("code", BigDecimal.valueOf(id % 50_000, 2));
                record.setField(
                    "price", BigDecimal.valueOf((id * 7919) % 9_999_999 - 4_999_999, 2));
                record.setField(
                    "amount",
                    i % 6 == 0
                        ? null
                        : BigDecimal.valueOf(
                            (i % 2 == 0 ? 1 : -1) * (99_999_999_999_999L - id * 1_234_567L), 4));
                record.setField("band", BigDecimal.valueOf(id % 5, 1));
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deletePositions(
        table, files, ImmutableList.of(everyNth(4000, 9), new long[0], everyNth(4000, 13)));
    MorTables.deleteEqual(
        table,
        "code",
        ImmutableList.of(BigDecimal.valueOf(5, 2), BigDecimal.valueOf(1_000_007 % 50_000, 2)));
    table.refresh();

    org.apache.iceberg.expressions.Expression filter =
        org.apache.iceberg.expressions.Expressions.greaterThan(
            "price", new BigDecimal("-12345.67"));
    List<String> expected = Lists.newArrayList();
    Map<BigDecimal, BigDecimal[]> expectedGroups = new TreeMap<>();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).where(filter).build()) {
      for (Record record : records) {
        BigDecimal price = (BigDecimal) record.getField("price");
        if (price.compareTo(new BigDecimal("-12345.67")) <= 0) {
          continue; // IcebergGenerics applies the filter to files, not rows
        }
        BigDecimal amount = (BigDecimal) record.getField("amount");
        expected.add(
            flatRow(
                record.getField("id"),
                ((BigDecimal) record.getField("code")).unscaledValue().longValueExact(),
                price.unscaledValue().longValueExact(),
                amount == null ? null : amount.unscaledValue().longValueExact()));
        BigDecimal[] sums =
            expectedGroups.computeIfAbsent(
                (BigDecimal) record.getField("band"),
                ignored -> new BigDecimal[] {BigDecimal.ZERO, BigDecimal.ZERO});
        sums[0] = sums[0].add(price);
        sums[1] = sums[1].add(BigDecimal.ONE);
      }
    }
    expected.sort(String::compareTo);
    assertThat(expected).isNotEmpty();

    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table, filter)) {
      List<String> actual = java.util.Collections.synchronizedList(Lists.newArrayList());
      device.materialize(
          ImmutableList.of("id", "code", "price", "amount"),
          batch -> {
            for (int r = 0; r < batch.rows(); r++) {
              actual.add(
                  flatRow(
                      batch.getLong("id", r),
                      batch.getLong("code", r),
                      batch.getLong("price", r),
                      batch.isNull("amount", r) ? null : batch.getLong("amount", r)));
            }
          });
      List<String> sorted = Lists.newArrayList(actual);
      sorted.sort(String::compareTo);
      assertThat(sorted).containsExactlyElementsOf(expected);

      // Grouped by a decimal (its unscaled value), the decimal summed as its unscaled value:
      // exact, as the sums stay far below 2^53.
      GroupedAggregate groups =
          device.aggregate(ImmutableList.of("band"), ImmutableList.of(DeviceValue.of("price")));
      Map<BigDecimal, BigDecimal[]> actualGroups = new TreeMap<>();
      groups
          .groups()
          .forEach(
              (key, totals) ->
                  actualGroups.put(
                      BigDecimal.valueOf((Long) key.get(0), 1),
                      new BigDecimal[] {
                        BigDecimal.valueOf((long) totals[0], 2),
                        BigDecimal.valueOf((long) totals[1])
                      }));
      assertThat(actualGroups.keySet()).containsExactlyElementsOf(expectedGroups.keySet());
      expectedGroups.forEach(
          (band, sums) -> {
            assertThat(actualGroups.get(band)[0]).as("sum %s", band).isEqualByComparingTo(sums[0]);
            assertThat(actualGroups.get(band)[1]).as("rows %s", band).isEqualByComparingTo(sums[1]);
          });
    }
  }

  /**
   * A compaction on the device of a table with decimal and float columns: the rewritten files hold
   * exactly the live rows Iceberg read before, decimals at their scale, and no delete file is left.
   */
  @Test
  public void testRewriteDecimalsAndFloats() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "price", Types.DecimalType.of(7, 2)),
            Types.NestedField.optional(3, "amount", Types.DecimalType.of(18, 4)),
            Types.NestedField.required(4, "ratio", Types.FloatType.get()));
    Table table = MorTables.create(temp.resolve("rewrite-decimals").toString(), schema, 2);
    List<DataFile> files = Lists.newArrayList();
    for (long f = 0; f < 3; f++) {
      long index = f;
      files.add(
          MorTables.writeDataFile(
              table,
              3000,
              i -> {
                long id = index * 1_000_000L + i;
                GenericRecord record = GenericRecord.create(schema);
                record.setField("id", id);
                record.setField(
                    "price", BigDecimal.valueOf((id * 7919) % 9_999_999 - 4_999_999, 2));
                record.setField(
                    "amount",
                    i % 7 == 0
                        ? null
                        : BigDecimal.valueOf(99_999_999_999_999L - id * 1_234_567L, 4));
                record.setField("ratio", (float) id / 3);
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deletePositions(
        table, files, ImmutableList.of(everyNth(3000, 5), new long[0], everyNth(3000, 11)));
    MorTables.deleteEqual(table, "id", ImmutableList.of(4L, 1_000_009L));
    table.refresh();

    List<String> before = Lists.newArrayList();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      // The table's columns only: a file with position deletes reads back with _pos too.
      records.forEach(
          record ->
              before.add(
                  flatRow(
                      record.getField("id"),
                      record.getField("price"),
                      record.getField("amount"),
                      record.getField("ratio"))));
    }
    before.sort(String::compareTo);

    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      device.rewriteDataFiles();
    }
    table.refresh();
    List<String> after = Lists.newArrayList();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      // The table's columns only: a file with position deletes reads back with _pos too.
      records.forEach(
          record ->
              after.add(
                  flatRow(
                      record.getField("id"),
                      record.getField("price"),
                      record.getField("amount"),
                      record.getField("ratio"))));
    }
    after.sort(String::compareTo);
    assertThat(after).containsExactlyElementsOf(before);
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = table.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask task : tasks) {
        assertThat(task.deletes()).as("delete files after the rewrite").isEmpty();
      }
    }
  }

  private static String flatRow(Object... values) {
    StringBuilder row = new StringBuilder();
    for (Object value : values) {
      row.append(
              value instanceof Number number && !(value instanceof Double)
                  ? number.longValue()
                  : value)
          .append('|');
    }
    return row.toString();
  }

  /**
   * Filters: Iceberg prunes files with the filter's statistics, the device applies the residual to
   * the rows that survive the deletes. Ranges, IN, inequality and constants of every column type.
   */
  @Test
  public void testFilters() throws IOException {
    Table table = MorTables.create(temp.resolve("filters").toString(), MorTables.SCHEMA, 2);
    List<DataFile> files = writeFiles(table, 0, 1000, 2500, 700);
    MorTables.deletePositions(
        table,
        files,
        ImmutableList.of(everyNth(1000, 10), randomPositions(2500, 0.3, 4), new long[0]));
    MorTables.deleteEqual(table, "id", ImmutableList.of(5L, 7L, 1_000_123L, 2_000_699L));

    // Prunes the first file by its id bounds; the rest is a residual.
    assertMatchesIceberg(
        table, org.apache.iceberg.expressions.Expressions.greaterThanOrEqual("id", 1_000_000L));
    // Prunes all but the second file.
    assertMatchesIceberg(
        table,
        org.apache.iceberg.expressions.Expressions.and(
            org.apache.iceberg.expressions.Expressions.greaterThan("id", 1_000_100L),
            org.apache.iceberg.expressions.Expressions.lessThan("id", 1_002_000L)));
    // Residual only: every file has some rows each way.
    assertMatchesIceberg(
        table,
        org.apache.iceberg.expressions.Expressions.and(
            org.apache.iceberg.expressions.Expressions.in("k", 1, 3, 5),
            org.apache.iceberg.expressions.Expressions.lessThanOrEqual("v1", 400.0),
            org.apache.iceberg.expressions.Expressions.notEqual("v3", 2.0)));
    // Nothing matches.
    assertMatchesIceberg(table, org.apache.iceberg.expressions.Expressions.equal("v4", 2.0));
  }

  /** Filters on optional columns: a comparison with a null does not hold; IS [NOT] NULL do. */
  @Test
  public void testFiltersOnNulls() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "k", Types.IntegerType.get()),
            Types.NestedField.optional(3, "v1", Types.DoubleType.get()),
            Types.NestedField.required(4, "v2", Types.DoubleType.get()),
            Types.NestedField.required(5, "v3", Types.DoubleType.get()),
            Types.NestedField.required(6, "v4", Types.DoubleType.get()));
    Table table = MorTables.create(temp.resolve("filternulls").toString(), schema, 2);
    DataFile file =
        MorTables.writeDataFile(
            table,
            3000,
            i -> {
              Record record = row(schema, 0, i);
              if (i % 5 == 0) {
                record.setField("id", null);
              }
              if (i % 4 == 0) {
                record.setField("v1", null);
              }
              return record;
            });
    MorTables.append(table, ImmutableList.of(file));
    MorTables.deletePositions(table, ImmutableList.of(file), ImmutableList.of(everyNth(3000, 9)));
    assertMatchesIceberg(table, org.apache.iceberg.expressions.Expressions.notNull("v1"));
    assertMatchesIceberg(table, org.apache.iceberg.expressions.Expressions.isNull("id"));
    assertMatchesIceberg(
        table, org.apache.iceberg.expressions.Expressions.greaterThan("v1", 100.0));
    assertMatchesIceberg(
        table,
        org.apache.iceberg.expressions.Expressions.and(
            org.apache.iceberg.expressions.Expressions.notEqual("id", 10L),
            org.apache.iceberg.expressions.Expressions.lessThan("k", 4)));
  }

  /**
   * Compaction on the device: every row Iceberg's reader returns before, it returns after, from new
   * files written by libcudf with no delete files left; the files carry Iceberg's field ids and
   * their footers' metrics.
   */
  @Test
  public void testRewriteDataFiles() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "k", Types.IntegerType.get()),
            Types.NestedField.optional(3, "v1", Types.DoubleType.get()),
            Types.NestedField.required(4, "v2", Types.DoubleType.get()),
            Types.NestedField.required(5, "v3", Types.DoubleType.get()),
            Types.NestedField.optional(6, "v4", Types.DoubleType.get()));
    Table table = MorTables.create(temp.resolve("rewrite").toString(), schema, 2);
    List<DataFile> files = Lists.newArrayList();
    for (long f = 0; f < 3; f++) {
      long index = f;
      files.add(
          MorTables.writeDataFile(
              table,
              2000 + 500 * f,
              i -> {
                Record record = row(schema, index, i);
                if (i % 6 == 0) {
                  record.setField("v1", null);
                }
                if (i % 7 == 0) {
                  record.setField("v4", null);
                }
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deletePositions(
        table,
        files,
        ImmutableList.of(everyNth(2000, 5), randomPositions(2500, 0.2, 9), new long[0]));
    MorTables.deleteEqual(table, "id", ImmutableList.of(3L, 1_000_004L, 2_000_100L, 2_000_101L));

    table.refresh();
    List<String> before = rows(table);
    DeviceMergeOnReadScan.RewriteResult result;
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      result = device.rewriteDataFiles();
    }
    table.refresh();

    assertThat(result.dataFilesRemoved()).isEqualTo(3);
    assertThat(result.deleteFilesRemoved()).isEqualTo(3); // two position-delete files, one equality
    assertThat(result.rows()).isEqualTo(before.size());
    long recorded = 0;
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
        table.newScan().includeColumnStats().planFiles()) {
      for (org.apache.iceberg.FileScanTask task : tasks) {
        assertThat(task.deletes()).isEmpty();
        assertThat(task.file().location()).contains("gpu-rewrite-");
        assertThat(task.file().lowerBounds()).containsKey(1); // footer statistics, by field id
        recorded += task.file().recordCount();
      }
    }
    assertThat(recorded).isEqualTo(before.size());
    assertThat(rows(table)).containsExactlyElementsOf(before);
    assertMatchesIceberg(table);
  }

  /**
   * A v3 rewrite keeps every row's lineage: {@code _row_id} and {@code
   * _last_updated_sequence_number} read the same after it as before, though before they were
   * inherited from each file's first row id and sequence number and after they are stored.
   */
  @Test
  public void testRewriteDataFilesV3KeepsLineage() throws IOException {
    Table table = MorTables.create(temp.resolve("rewrite-v3").toString(), MorTables.SCHEMA, 3);
    List<DataFile> first = writeFiles(table, 0, 3000, 1200);
    List<DataFile> second = writeFiles(table, 2, 2200); // a later commit: other ids and sequence
    MorTables.deletePositions(
        table, first, ImmutableList.of(randomPositions(3000, 0.1, 4), everyNth(1200, 4)));
    MorTables.deletePositions(table, second, ImmutableList.of(everyNth(2200, 9)));
    MorTables.deleteEqual(table, "id", ImmutableList.of(11L, 1_000_013L, 2_000_017L));

    table.refresh();
    Map<Long, String> before = lineage(table);
    assertThat(before.values()).doesNotContainNull();
    DeviceMergeOnReadScan.RewriteResult result;
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      result = device.rewriteDataFiles();
    }
    table.refresh();

    assertThat(result.dataFilesRemoved()).isEqualTo(3);
    assertThat(result.rows()).isEqualTo(before.size());
    Map<Long, String> after = lineage(table);
    assertThat(after).isEqualTo(before);
    assertMatchesIceberg(table);
  }

  /**
   * A Z-order rewrite keeps every row and writes each new file in non-decreasing Iceberg Z-order of
   * (k, v1), as {@code ZOrderByteUtils} computes it on the CPU; v1 has nulls, which order as zero
   * bytes.
   */
  @Test
  public void testRewriteDataFilesZOrder() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "k", Types.IntegerType.get()),
            Types.NestedField.optional(3, "v1", Types.DoubleType.get()),
            Types.NestedField.required(4, "v2", Types.DoubleType.get()),
            Types.NestedField.required(5, "v3", Types.DoubleType.get()),
            Types.NestedField.required(6, "v4", Types.DoubleType.get()));
    Table table = MorTables.create(temp.resolve("zorder").toString(), schema, 2);
    List<DataFile> files = Lists.newArrayList();
    for (long f = 0; f < 3; f++) {
      long index = f;
      files.add(
          MorTables.writeDataFile(
              table,
              3000,
              i -> {
                Record record = row(schema, index, i);
                record.setField("k", (int) ((i * 7919 + index) % 101) - 50);
                record.setField("v1", i % 11 == 0 ? null : Math.sin(i + index) * 1e3);
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deletePositions(
        table, files, ImmutableList.of(everyNth(3000, 7), new long[0], everyNth(3000, 3)));
    MorTables.deleteEqual(table, "id", ImmutableList.of(10L, 1_000_020L));

    table.refresh();
    List<String> before = rows(table);
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      device.rewriteDataFilesZOrder("k", "v1");
    }
    table.refresh();
    assertThat(rows(table)).containsExactlyElementsOf(before);

    long checked = 0;
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = table.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask task : tasks) {
        byte[] previous = null;
        try (CloseableIterable<Record> records =
            org.apache.iceberg.parquet.Parquet.read(table.io().newInputFile(task.file().location()))
                .project(schema)
                .createReaderFunc(
                    fileSchema ->
                        org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(
                            schema, fileSchema))
                .build()) {
          for (Record record : records) {
            byte[] zvalue = zorder((Integer) record.getField("k"), (Double) record.getField("v1"));
            if (previous != null) {
              assertThat(java.util.Arrays.compareUnsigned(previous, zvalue))
                  .as("row %d of %s", checked, task.file().location())
                  .isLessThanOrEqualTo(0);
            }
            previous = zvalue;
            checked++;
          }
        }
      }
    }
    assertThat(checked).isEqualTo(before.size());
  }

  /**
   * A sorted rewrite keeps every row, writes each new file in the table's sort order -- descending
   * and nulls-last on v1, which has nulls -- as Iceberg's {@code SortOrderComparators} orders it,
   * and records that order's id on every new file.
   */
  @Test
  public void testRewriteDataFilesSorted() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "k", Types.IntegerType.get()),
            Types.NestedField.optional(3, "v1", Types.DoubleType.get()),
            Types.NestedField.required(4, "v2", Types.DoubleType.get()),
            Types.NestedField.required(5, "v3", Types.DoubleType.get()),
            Types.NestedField.required(6, "v4", Types.DoubleType.get()));
    Table table = MorTables.create(temp.resolve("sorted").toString(), schema, 3);
    List<DataFile> files = Lists.newArrayList();
    for (long f = 0; f < 3; f++) {
      long index = f;
      files.add(
          MorTables.writeDataFile(
              table,
              3000,
              i -> {
                Record record = row(schema, index, i);
                record.setField("k", (int) ((i * 7919 + index) % 13) - 6);
                record.setField("v1", i % 11 == 0 ? null : Math.sin(i + index) * 1e3);
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deletePositions(
        table, files, ImmutableList.of(everyNth(3000, 7), new long[0], everyNth(3000, 3)));
    MorTables.deleteEqual(table, "id", ImmutableList.of(10L, 1_000_020L));
    table.replaceSortOrder().asc("k").desc("v1", org.apache.iceberg.NullOrder.NULLS_LAST).commit();

    table.refresh();
    List<String> before = rows(table);
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      device.rewriteDataFilesSorted();
    }
    table.refresh();
    assertThat(rows(table)).containsExactlyElementsOf(before);

    java.util.Comparator<org.apache.iceberg.StructLike> order =
        org.apache.iceberg.SortOrderComparators.forSchema(schema, table.sortOrder());
    long checked = 0;
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = table.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask task : tasks) {
        assertThat(task.file().sortOrderId()).isEqualTo(table.sortOrder().orderId());
        Record previous = null;
        try (CloseableIterable<Record> records =
            org.apache.iceberg.parquet.Parquet.read(table.io().newInputFile(task.file().location()))
                .project(schema)
                .createReaderFunc(
                    fileSchema ->
                        org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(
                            schema, fileSchema))
                .build()) {
          for (Record record : records) {
            if (previous != null) {
              assertThat(order.compare(previous, record))
                  .as("row %d of %s", checked, task.file().location())
                  .isLessThanOrEqualTo(0);
            }
            previous = record.copy();
            checked++;
          }
        }
      }
    }
    assertThat(checked).isEqualTo(before.size());
  }

  /**
   * A sorted file group, as a planner with a filter or a delete threshold picks it: two of three
   * files are rewritten in the table's sort order and swapped by the caller's {@code RewriteFiles};
   * the rows are unchanged, the new files sorted and tagged with the order, and the third file is
   * left as it was.
   */
  @Test
  public void testRewriteGroupSorted() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "k", Types.IntegerType.get()),
            Types.NestedField.optional(3, "v1", Types.DoubleType.get()),
            Types.NestedField.required(4, "v2", Types.DoubleType.get()),
            Types.NestedField.required(5, "v3", Types.DoubleType.get()),
            Types.NestedField.required(6, "v4", Types.DoubleType.get()));
    Table table = MorTables.create(temp.resolve("group").toString(), schema, 3);
    List<DataFile> files = Lists.newArrayList();
    for (long f = 0; f < 3; f++) {
      long index = f;
      files.add(
          MorTables.writeDataFile(
              table,
              2000,
              i -> {
                Record record = row(schema, index, i);
                record.setField("k", (int) ((i * 7919 + index) % 13) - 6);
                record.setField("v1", i % 11 == 0 ? null : Math.cos(i + index) * 1e3);
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deletePositions(
        table, files, ImmutableList.of(everyNth(2000, 5), new long[0], everyNth(2000, 4)));
    table.replaceSortOrder().desc("k").asc("v1", org.apache.iceberg.NullOrder.NULLS_FIRST).commit();

    table.refresh();
    List<String> before = rows(table);
    List<org.apache.iceberg.FileScanTask> group = Lists.newArrayList();
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = table.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask task : tasks) {
        if (!task.file().location().equals(files.get(1).location())) {
          group.add(task);
        }
      }
    }
    assertThat(group).hasSize(2);
    List<DataFile> written;
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      written = device.rewriteGroupSorted(group, 128L << 20);
    }
    org.apache.iceberg.RewriteFiles swap =
        table.newRewrite().validateFromSnapshot(table.currentSnapshot().snapshotId());
    for (org.apache.iceberg.FileScanTask task : group) {
      swap.deleteFile(task.file());
      task.deletes().forEach(swap::deleteFile);
    }
    written.forEach(swap::addFile);
    swap.commit();
    table.refresh();
    assertThat(rows(table)).containsExactlyElementsOf(before);

    java.util.Comparator<org.apache.iceberg.StructLike> order =
        org.apache.iceberg.SortOrderComparators.forSchema(schema, table.sortOrder());
    java.util.Set<String> locations = new java.util.HashSet<>();
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = table.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask task : tasks) {
        locations.add(task.file().location());
      }
    }
    assertThat(locations).contains(files.get(1).location());
    for (DataFile file : written) {
      assertThat(file.sortOrderId()).isEqualTo(table.sortOrder().orderId());
      Record previous = null;
      try (CloseableIterable<Record> records =
          org.apache.iceberg.parquet.Parquet.read(table.io().newInputFile(file.location()))
              .project(schema)
              .createReaderFunc(
                  fileSchema ->
                      org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(
                          schema, fileSchema))
              .build()) {
        for (Record record : records) {
          if (previous != null) {
            assertThat(order.compare(previous, record)).isLessThanOrEqualTo(0);
          }
          previous = record.copy();
        }
      }
    }
  }

  private static byte[] zorder(int kValue, Double v1) {
    byte[] key =
        org.apache.iceberg.util.ZOrderByteUtils.intToOrderedBytes(
                kValue, java.nio.ByteBuffer.allocate(8))
            .array()
            .clone();
    byte[] value =
        v1 == null
            ? new byte[org.apache.iceberg.util.ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE]
            : org.apache.iceberg.util.ZOrderByteUtils.doubleToOrderedBytes(
                    v1, java.nio.ByteBuffer.allocate(8))
                .array()
                .clone();
    return org.apache.iceberg.util.ZOrderByteUtils.interleaveBits(
        new byte[][] {key, value}, 16, java.nio.ByteBuffer.allocate(16));
  }

  /** {@code id -> _row_id|_last_updated_sequence_number} of every live row. */
  private static Map<Long, String> lineage(Table table) throws IOException {
    Schema projection =
        org.apache.iceberg.types.TypeUtil.join(
            table.schema(),
            new Schema(
                org.apache.iceberg.MetadataColumns.ROW_ID,
                org.apache.iceberg.MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER));
    Map<Long, String> lineage = new TreeMap<>();
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(table).project(projection).build()) {
      for (Record record : records) {
        Object rowId = record.getField("_row_id");
        Object sequence = record.getField("_last_updated_sequence_number");
        lineage.put(
            (Long) record.getField("id"),
            rowId == null || sequence == null ? null : rowId + "|" + sequence);
      }
    }
    return lineage;
  }

  private static List<String> rows(Table table) throws IOException {
    List<String> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        // Only the table's columns: a read that applies position deletes also carries _pos.
        StringBuilder row = new StringBuilder();
        for (Types.NestedField field : table.schema().columns()) {
          row.append(record.getField(field.name())).append('|');
        }
        rows.add(row.toString());
      }
    }
    rows.sort(String::compareTo);
    return rows;
  }

  /**
   * Equality deletes become deletion vectors: the table reads the same rows, no equality-delete
   * file is left, each data file the deletes applied to has one deletion vector (its earlier one
   * merged in), and a file appended after the deletes keeps the row they would have matched.
   */
  @Test
  public void testConvertEqualityDeletesToDeletionVectors() throws IOException {
    Table table = equalityDeletesToConvert("eq-to-dv");
    List<String> before = rows(table);

    DeviceMergeOnReadScan.ConversionResult result;
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      result = device.convertEqualityDeletes();
    }
    table.refresh();

    // The CPU twin converts the same table to the same deletion vectors.
    Table twin = equalityDeletesToConvert("eq-to-dv-cpu");
    assertThat(CpuMergeOnReadScan.of(twin, 4).convertEqualityDeletes()).isEqualTo(result);
    twin.refresh();
    assertThat(rows(twin)).isEqualTo(before);

    assertThat(result.dataFiles()).isEqualTo(3);
    assertThat(result.equalityDeletesRemoved()).isEqualTo(2);
    assertThat(result.positionDeletesReplaced()).isEqualTo(1);
    assertThat(result.vectorsAdded()).isEqualTo(3);
    assertThat(rows(table)).isEqualTo(before);
    assertThat(result.deletedRows()).isEqualTo(7500 - before.size());
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = table.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask task : tasks) {
        assertThat(task.deletes()).hasSize(1);
        assertThat(org.apache.iceberg.util.ContentFileUtil.isDV(task.deletes().get(0))).isTrue();
      }
    }
    assertMatchesIceberg(table);
  }

  /**
   * A compaction that commits while a conversion runs removes a data file the conversion wrote a
   * deletion vector for: the conversion's commit fails rather than point a vector at a file that is
   * gone, on the device and on the CPU twin.
   */
  @Test
  public void testConversionFailsAfterConcurrentRewrite() {
    for (boolean onDevice : new boolean[] {true, false}) {
      Table table = equalityDeletesToConvert(onDevice ? "eq-to-dv-race" : "eq-to-dv-race-cpu");
      Table racing = rewriteBeforeRowDelta(table);
      if (onDevice) {
        assertThatThrownBy(
                () -> {
                  try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(racing)) {
                    device.convertEqualityDeletes();
                  }
                })
            .isInstanceOf(ValidationException.class)
            .hasMessageContaining("missing data files");
      } else {
        assertThatThrownBy(() -> CpuMergeOnReadScan.of(racing, 4).convertEqualityDeletes())
            .isInstanceOf(ValidationException.class)
            .hasMessageContaining("missing data files");
      }
      table.refresh();
      assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.REPLACE);
    }
  }

  /**
   * The table, except that its first {@code newRowDelta()} first commits a rewrite of a data file
   * that equality deletes apply to: a compaction landing between a conversion's plan and its
   * commit.
   */
  private static Table rewriteBeforeRowDelta(Table table) {
    AtomicBoolean rewritten = new AtomicBoolean();
    return (Table)
        Proxy.newProxyInstance(
            Table.class.getClassLoader(),
            new Class<?>[] {Table.class, HasTableOperations.class},
            (proxy, method, args) -> {
              if ("newRowDelta".equals(method.getName()) && rewritten.compareAndSet(false, true)) {
                DataFile victim = null;
                try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                  for (FileScanTask task : tasks) {
                    if (task.deletes().stream()
                        .anyMatch(d -> d.content() == FileContent.EQUALITY_DELETES)) {
                      victim = task.file();
                      break;
                    }
                  }
                }
                DataFile copy =
                    DataFiles.builder(table.spec())
                        .copy(victim)
                        .withPath(victim.location() + ".rewritten.parquet")
                        .build();
                table
                    .newRewrite()
                    .validateFromSnapshot(table.currentSnapshot().snapshotId())
                    .deleteFile(victim)
                    .addFile(copy)
                    .commit();
              }
              try {
                return method.invoke(table, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
  }

  /** Position deletes, two equality deletes, and a file appended between them: v3. */
  private Table equalityDeletesToConvert(String name) {
    Table table = MorTables.create(temp.resolve(name).toString(), MorTables.SCHEMA, 3);
    List<DataFile> files = writeFiles(table, 0, 4000, 3000);
    MorTables.deletePositions(
        table, files, ImmutableList.of(randomPositions(4000, 0.05, 5), new long[0]));
    MorTables.deleteEqual(table, "id", ImmutableList.of(1L, 2L, 3_999L, 1_000_002L, 9_000_000L));
    writeFiles(table, 0, 500); // ids 1 and 2 again, after the deletes
    MorTables.deleteEqual(table, "id", ImmutableList.of(3L, 400L, 1_000_400L));
    table.refresh();
    return table;
  }

  /**
   * Rows deleted by key as deletion vectors: the table reads its earlier rows minus the keys' rows,
   * only the files whose id bounds can hold a key get a vector (with their earlier deletes folded
   * in), no equality-delete file is added, and the table's own equality deletes stay.
   */
  @Test
  public void testDeleteKeysToDeletionVectors() throws IOException {
    Table table = equalityDeletesToConvert("delete-keys");
    List<String> before = rows(table);
    // 2 and 1_000_123 exist; 300 exists in file 0 and in the file appended after the first
    // deletes; 77_000_000 exists nowhere.
    List<Long> keys = ImmutableList.of(2L, 300L, 1_000_123L, 77_000_000L);
    long deleteFilesBefore = equalityDeleteFiles(table);

    DeviceMergeOnReadScan.ConversionResult result;
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      result = device.deleteKeys("id", keys);
    }
    table.refresh();

    List<String> expected = Lists.newArrayList();
    for (String row : before) {
      long id = Long.parseLong(row.substring(0, row.indexOf('|')));
      if (!keys.contains(id)) {
        expected.add(row);
      }
    }
    assertThat(rows(table)).isEqualTo(expected);
    // ids 0..3999 (file 0), 1_000_000.. (file 1), 0..499 (the late file): all three can hold a key.
    assertThat(result.dataFiles()).isEqualTo(3);
    assertThat(result.vectorsAdded()).isEqualTo(3);
    assertThat(result.equalityDeletesRemoved()).isZero();
    assertThat(equalityDeleteFiles(table)).isEqualTo(deleteFilesBefore);
    assertMatchesIceberg(table);

    // A key that no file's bounds can hold changes nothing.
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      assertThat(device.deleteKeys("id", ImmutableList.of(99_000_000L)).dataFiles()).isZero();
    }
  }

  private static long equalityDeleteFiles(Table table) throws IOException {
    java.util.Set<String> seen = new java.util.HashSet<>();
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = table.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask task : tasks) {
        for (org.apache.iceberg.DeleteFile delete : task.deletes()) {
          if (delete.content() == org.apache.iceberg.FileContent.EQUALITY_DELETES) {
            seen.add(delete.location());
          }
        }
      }
    }
    return seen.size();
  }

  /**
   * A v2 table: equality deletes become file-scoped position-delete files (v2 has no deletion
   * vectors), one per data file with its earlier position deletes folded in; then keys deleted the
   * same way. Both read as Iceberg's reader says.
   */
  @Test
  public void testConvertAndDeleteKeysOnV2() throws IOException {
    Table table = MorTables.create(temp.resolve("v2-convert").toString(), MorTables.SCHEMA, 2);
    List<DataFile> files = writeFiles(table, 0, 4000, 3000);
    MorTables.deletePositions(
        table, files, ImmutableList.of(randomPositions(4000, 0.05, 6), everyNth(3000, 7)));
    MorTables.deleteEqual(table, "id", ImmutableList.of(1L, 2L, 3_999L, 1_000_002L));
    table.refresh();
    List<String> before = rows(table);

    DeviceMergeOnReadScan.ConversionResult converted;
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      converted = device.convertEqualityDeletes();
    }
    table.refresh();
    assertThat(converted.dataFiles()).isEqualTo(2);
    assertThat(converted.equalityDeletesRemoved()).isEqualTo(1);
    assertThat(converted.positionDeletesReplaced()).isEqualTo(2);
    assertThat(converted.vectorsAdded()).isEqualTo(2);
    assertThat(rows(table)).isEqualTo(before);
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = table.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask task : tasks) {
        assertThat(task.deletes()).hasSize(1);
        org.apache.iceberg.DeleteFile delete = task.deletes().get(0);
        assertThat(delete.content()).isEqualTo(org.apache.iceberg.FileContent.POSITION_DELETES);
        assertThat(org.apache.iceberg.util.ContentFileUtil.isDV(delete)).isFalse();
        assertThat(
                String.valueOf(org.apache.iceberg.util.ContentFileUtil.referencedDataFile(delete)))
            .isEqualTo(task.file().location());
      }
    }

    List<Long> keys = ImmutableList.of(5L, 1_000_010L);
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      device.deleteKeys("id", keys);
    }
    table.refresh();
    List<String> expected = Lists.newArrayList();
    for (String row : before) {
      if (!keys.contains(Long.parseLong(row.substring(0, row.indexOf('|'))))) {
        expected.add(row);
      }
    }
    assertThat(rows(table)).isEqualTo(expected);
    assertMatchesIceberg(table);
  }

  /**
   * NDV statistics as Iceberg's compute_table_stats writes them: for each column the device's
   * smallest distinct hashes are exactly the smallest hashes of a DataSketches sketch fed the live
   * values' Iceberg serialization (as the Spark action feeds it), the estimate is exact below the
   * sketch's size and close above it, and a Puffin statistics file with the theta blobs and their
   * ndv property is set on the snapshot.
   */
  @Test
  public void testComputeTableStats() throws IOException {
    Table table = equalityDeletesToConvert("stats");
    String[] columns = {"id", "k", "v1"};
    Map<String, org.apache.datasketches.theta.UpdateSketch> reference = Maps.newHashMap();
    Map<String, java.util.Set<Object>> distinct = Maps.newHashMap();
    for (String column : columns) {
      reference.put(
          column,
          org.apache.datasketches.theta.UpdateSketch.builder()
              .setFamily(org.apache.datasketches.common.Family.ALPHA)
              .build());
      distinct.put(column, new java.util.HashSet<>());
    }
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        for (String column : columns) {
          Object value = record.getField(column);
          org.apache.iceberg.types.Type type = table.schema().findField(column).type();
          reference
              .get(column)
              .update(org.apache.iceberg.types.Conversions.toByteBuffer(type, value));
          distinct.get(column).add(value);
        }
      }
    }

    Map<String, Long> estimates;
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      estimates = device.computeTableStats(columns);
    }
    table.refresh();

    assertThat(estimates.get("k")).isEqualTo(distinct.get("k").size());
    for (String column : new String[] {"id", "v1"}) {
      long exact = distinct.get(column).size();
      assertThat((double) estimates.get(column))
          .isCloseTo(exact, org.assertj.core.data.Percentage.withPercentage(5));
    }
    assertThat(table.statisticsFiles()).hasSize(1);
    org.apache.iceberg.StatisticsFile statistics = table.statisticsFiles().get(0);
    assertThat(statistics.snapshotId()).isEqualTo(table.currentSnapshot().snapshotId());
    assertThat(statistics.blobMetadata()).hasSize(3);
    try (org.apache.iceberg.puffin.PuffinReader reader =
        org.apache.iceberg.puffin.Puffin.read(table.io().newInputFile(statistics.path())).build()) {
      for (org.apache.iceberg.util.Pair<org.apache.iceberg.puffin.BlobMetadata, java.nio.ByteBuffer>
          blob : reader.readAll(reader.fileMetadata().blobs())) {
        assertThat(blob.first().type())
            .isEqualTo(org.apache.iceberg.puffin.StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1);
        String column = table.schema().findField(blob.first().inputFields().get(0)).name();
        assertThat(blob.first().properties())
            .containsEntry("ndv", String.valueOf(estimates.get(column)));
        org.apache.datasketches.theta.Sketch ours =
            org.apache.datasketches.theta.CompactSketch.heapify(
                org.apache.datasketches.memory.Memory.wrap(
                    org.apache.iceberg.util.ByteBuffers.toByteArray(blob.second())));
        // The hash port: our hashes are the reference sketch's smallest ones.
        java.util.NavigableSet<Long> mine = new java.util.TreeSet<>();
        for (org.apache.datasketches.theta.HashIterator it = ours.iterator(); it.next(); ) {
          mine.add(it.get());
        }
        java.util.NavigableSet<Long> theirs = new java.util.TreeSet<>();
        for (org.apache.datasketches.theta.HashIterator it =
                reference.get(column).compact().iterator();
            it.next(); ) {
          theirs.add(it.get());
        }
        // DataSketches' own sketch keeps only the hashes below its theta, which may be below our
        // largest: compare up to the smaller of the two.
        assertThat(mine).isNotEmpty();
        long upTo = Math.min(mine.last(), theirs.last());
        assertThat(mine.headSet(upTo, true)).as(column).isEqualTo(theirs.headSet(upTo, true));
        assertThat(mine.headSet(upTo, true).size())
            .isGreaterThanOrEqualTo(Math.min(1000, distinct.get(column).size()));
      }
    }
  }

  /** An equality key of type INT, which the device probes as 32-bit keys. */
  @Test
  public void testIntEqualityKey() throws IOException {
    Table table = MorTables.create(temp.resolve("intkey").toString(), MorTables.SCHEMA, 2);
    writeFiles(table, 0, 3000, 1500);
    MorTables.deleteEqual(table, "k", ImmutableList.of(2, 5));
    assertMatchesIceberg(table);
  }

  // ---------------------------------------------------------------------------------------------

  private static void assertMatchesIceberg(Table table) throws IOException {
    assertMatchesIceberg(table, org.apache.iceberg.expressions.Expressions.alwaysTrue());
  }

  private static void assertMatchesIceberg(
      Table table, org.apache.iceberg.expressions.Expression filter) throws IOException {
    table.refresh();
    Map<Integer, double[]> expectedByK = new TreeMap<>();
    Map<Integer, double[]> expectedByBucket = new TreeMap<>();
    org.apache.iceberg.transforms.Transform<Long, Integer> bucket =
        org.apache.iceberg.transforms.Transforms.bucket(16);
    java.util.function.Function<Long, Integer> bucketOf =
        bucket.bind(org.apache.iceberg.types.Types.LongType.get());
    double[] expectedAll = new double[2];
    List<String> expectedRows = Lists.newArrayList();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).where(filter).build()) {
      for (Record record : records) {
        int kValue = (Integer) record.getField("k");
        double[] totals =
            expectedByK.computeIfAbsent(kValue, ignored -> new double[1 + SUMS.length]);
        totals[0] += 1;
        for (int c = 0; c < SUMS.length; c++) {
          Object value = record.getField(SUMS[c]);
          totals[1 + c] += value == null ? 0.0 : (Double) value;
        }
        expectedAll[0] += 1;
        Object v1 = record.getField("v1");
        expectedAll[1] += v1 == null ? 0.0 : (Double) v1;
        expectedRows.add(record.getField("id") + "/" + kValue + "/" + v1);
        if (record.getField("id") != null) {
          double[] byBucket =
              expectedByBucket.computeIfAbsent(
                  bucketOf.apply((Long) record.getField("id")), ignored -> new double[2]);
          byBucket[0] += 1;
          byBucket[1] += v1 == null ? 0.0 : (Double) v1;
        }
      }
    }

    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table, filter)) {
      assertScanMatches(device, expectedByK, expectedAll, expectedRows);
      // Again on the same scan, which reuses its device buffers and plans.
      assertScanMatches(device, expectedByK, expectedAll, expectedRows);
      if (table.schema().findField("id").isRequired()) {
        assertBucketsMatch(device, expectedByBucket);
      }
    }
    MergeOnReadScan cpu = CpuMergeOnReadScan.of(table, 4, filter);
    assertScanMatches(cpu, expectedByK, expectedAll, expectedRows);
    if (table.schema().findField("id").isRequired()) {
      assertBucketsMatch(cpu, expectedByBucket);
    }
  }

  /** bucket(16, id) as Iceberg's own Transforms.bucket computes it. */
  private static void assertBucketsMatch(MergeOnReadScan scan, Map<Integer, double[]> expected) {
    AggregateResult buckets = scan.aggregateByBucket("id", 16, "v1");
    assertThat(buckets.groups().keySet()).isEqualTo(expected.keySet());
    for (Map.Entry<Integer, double[]> entry : expected.entrySet()) {
      AggregateResult.Group group = buckets.groups().get(entry.getKey());
      assertThat(group.count())
          .as("rows in bucket %s", entry.getKey())
          .isEqualTo((long) entry.getValue()[0]);
      assertThat(group.sums()[0])
          .as("v1 in bucket %s", entry.getKey())
          .isCloseTo(entry.getValue()[1], org.assertj.core.data.Offset.offset(1e-6));
    }
  }

  private static void assertScanMatches(
      MergeOnReadScan scan,
      Map<Integer, double[]> expectedByK,
      double[] expectedAll,
      List<String> expectedRows) {
    AggregateResult grouped = scan.aggregate("k", SUMS);
    assertThat(grouped.groups().keySet()).isEqualTo(expectedByK.keySet());
    for (Map.Entry<Integer, double[]> entry : expectedByK.entrySet()) {
      AggregateResult.Group group = grouped.groups().get(entry.getKey());
      assertThat(group.count())
          .as("count of k=%s", entry.getKey())
          .isEqualTo((long) entry.getValue()[0]);
      for (int c = 0; c < SUMS.length; c++) {
        assertThat(group.sums()[c])
            .as("%s of k=%s", SUMS[c], entry.getKey())
            .isCloseTo(entry.getValue()[1 + c], org.assertj.core.data.Offset.offset(1e-6));
      }
    }

    AggregateResult total = scan.aggregate(null, "v1");
    if (expectedAll[0] == 0) {
      assertThat(total.groups()).isEmpty();
    } else {
      assertThat(total.groups().keySet()).containsExactly(0);
      assertThat(total.groups().get(0).count()).isEqualTo((long) expectedAll[0]);
      assertThat(total.groups().get(0).sums()[0])
          .isCloseTo(expectedAll[1], org.assertj.core.data.Offset.offset(1e-6));
    }

    List<String> rows = java.util.Collections.synchronizedList(Lists.newArrayList());
    long live =
        scan.materialize(
            ImmutableList.of("id", "k", "v1"),
            batch -> {
              for (int r = 0; r < batch.rows(); r++) {
                Object id = batch.isNull("id", r) ? null : batch.getLong("id", r);
                Object v1 = batch.isNull("v1", r) ? null : batch.getDouble("v1", r);
                rows.add(id + "/" + batch.getInt("k", r) + "/" + v1);
              }
            });
    assertThat(live).isEqualTo(expectedRows.size());
    String[] actual = rows.toArray(new String[0]);
    String[] expected = expectedRows.toArray(new String[0]);
    Arrays.sort(actual);
    Arrays.sort(expected);
    assertThat(actual).containsExactly(expected);
  }
}
