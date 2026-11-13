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
package org.apache.iceberg.tornadovm.bench;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.tornadovm.AggregateResult;
import org.apache.iceberg.tornadovm.CpuMergeOnReadScan;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;
import org.apache.iceberg.tornadovm.DeviceValue;
import org.apache.iceberg.tornadovm.GroupedAggregate;
import org.apache.iceberg.tornadovm.MergeOnReadScan;
import org.apache.iceberg.util.BucketUtil;

/**
 * Runs one engine and one query over benchmark tables and prints one CSV line a run.
 *
 * <pre>
 *   MorBench &lt;warehouse&gt; &lt;engine: gpu|cpu|generics&gt; &lt;query: q1|q2|q3&gt; &lt;runs&gt; &lt;table&gt;...
 * </pre>
 *
 * <ul>
 *   <li>q1 -- {@code SELECT count(*), sum(v1)};
 *   <li>q2 -- {@code SELECT k, count(*), sum(v1), sum(v2), sum(v3), sum(v4) GROUP BY k};
 *   <li>q3 -- {@code SELECT id, k, v1}, every live row copied into host arrays;
 *   <li>q4 -- {@code SELECT bucket(16, id), count(*), sum(v1) GROUP BY 1}, Iceberg's bucket
 *       partition transform.
 * </ul>
 *
 * <p>The first run of each table is a warm-up (TornadoVM's JIT compiles the kernels, the page cache
 * fills) and is reported but marked. The checksum lets runs of different engines be compared: the
 * live row count, then a sum over the result.
 */
public final class MorBench {

  private static final String[] SUMS = {"v1", "v2", "v3", "v4"};

  private MorBench() {}

  public static void main(String[] args) throws IOException {
    String warehouse = args[0];
    String engine = args[1];
    String query = args[2];
    int runs = Integer.parseInt(args[3]);
    int threads = Integer.getInteger("threads", Runtime.getRuntime().availableProcessors());
    System.out.println(
        "table,engine,query,run,warmup,millis,live_rows,checksum,breakdown,cpu_seconds");
    for (int t = 4; t < args.length; t++) {
      Table table = MorTables.load(warehouse + "/" + args[t]);
      // One scan object a table, as an engine keeps its operators: a device scan keeps its buffers
      // and compiled plan between runs, so only the first run of a table pays for them.
      Expression filter = filterOf(query);
      MergeOnReadScan scan =
          "gpu".equals(engine)
              ? DeviceMergeOnReadScan.of(table, filter)
              : "cpu".equals(engine) ? CpuMergeOnReadScan.of(table, threads, filter) : null;
      for (int run = 0; run < runs; run++) {
        if ("rewrite".equals(query) || "zorder".equals(query) || "sorted".equals(query)) {
          rewriteRun(table, (DeviceMergeOnReadScan) scan, run, args[t], threads, query);
          continue;
        }
        beforeRun();
        long cpuStart = PROCESS.getProcessCpuTime();
        long start = System.nanoTime();
        double[] answer;
        String breakdown = "";
        if (scan == null) {
          answer = generics(table, query);
        } else {
          answer = run(scan, query);
          breakdown = scan.metrics().toString();
        }
        double millis = (System.nanoTime() - start) / 1e6;
        // The JVM's CPU time over the run, every thread: what the query cost the host.
        double cpuSeconds = (PROCESS.getProcessCpuTime() - cpuStart) / 1e9;
        System.out.println(
            String.format(
                Locale.ROOT,
                "%s,%s,%s,%d,%s,%.1f,%d,%.6e,\"%s\",%.2f",
                args[t],
                engine,
                query,
                run,
                run == 0 ? "yes" : "no",
                millis,
                (long) answer[0],
                answer[1],
                breakdown,
                cpuSeconds));
      }
      if (scan instanceof AutoCloseable closeable) {
        try {
          closeable.close();
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
    }
    // Object-store clients (S3FileIO) start threads that are not daemons.
    System.exit(0);
  }

  private static final com.sun.management.OperatingSystemMXBean PROCESS =
      (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

  /**
   * Runs {@code -Dbench.beforeRun=<command>} before each timed run, outside the timing -- dropping
   * the page cache of the table's files, for cold reads -- after dropping cached object-store
   * copies if {@code -Dbench.coldObjectStore=true}.
   */
  private static void beforeRun() {
    // -Dbench.coldObjectStore=true: every run fetches object-store files again.
    if (Boolean.getBoolean("bench.coldObjectStore")) {
      DeviceMergeOnReadScan.clearObjectStoreCache();
    }
    String command = System.getProperty("bench.beforeRun");
    if (command == null) {
      return;
    }
    try {
      int status = new ProcessBuilder("sh", "-c", command).inheritIO().start().waitFor();
      if (status != 0) {
        throw new IllegalStateException("bench.beforeRun failed: " + status);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  /** The live row count and a checksum of the answer. */
  private static double[] run(MergeOnReadScan scan, String query) {
    return switch (query) {
      case "tpch1" -> {
        // TPC-H Q1: sums of quantity, price, discounted price and charge per (returnflag,
        // linestatus); the averages are these over the counts. Checksum: the four sums.
        GroupedAggregate result =
            ((DeviceMergeOnReadScan) scan)
                .aggregate(
                    ImmutableList.of("l_returnflag", "l_linestatus"),
                    ImmutableList.of(
                        DeviceValue.of("l_quantity"),
                        DeviceValue.of("l_extendedprice"),
                        DeviceValue.of("l_extendedprice").timesOneMinus("l_discount"),
                        DeviceValue.of("l_extendedprice")
                            .timesOneMinus("l_discount")
                            .timesOnePlus("l_tax"),
                        DeviceValue.of("l_discount")));
        double checksum = 0;
        for (double[] totals : result.groups().values()) {
          checksum += totals[0] + totals[1] + totals[2] + totals[3];
        }
        yield new double[] {result.rows(), checksum};
      }
      case "tpch6" -> {
        GroupedAggregate result =
            ((DeviceMergeOnReadScan) scan)
                .aggregate(
                    ImmutableList.of(),
                    ImmutableList.of(DeviceValue.of("l_extendedprice").times("l_discount")));
        double revenue = 0;
        for (double[] totals : result.groups().values()) {
          revenue += totals[0];
        }
        yield new double[] {result.rows(), revenue};
      }
      case "q1" -> {
        AggregateResult result = scan.aggregate(null, "v1");
        yield new double[] {result.rows(), sum(result)};
      }
      case "q2" -> {
        AggregateResult result = scan.aggregate("k", SUMS);
        yield new double[] {result.rows(), sum(result) + result.groups().size()};
      }
      case "q4" -> {
        AggregateResult result = scan.aggregateByBucket("id", 16, "v1");
        yield new double[] {result.rows(), sum(result) + result.groups().size()};
      }
      case "q5" -> {
        AggregateResult result = scan.aggregate("k", "v1");
        yield new double[] {result.rows(), sum(result) + result.groups().size()};
      }
      case "q3" -> {
        LongAdder ids = new LongAdder();
        DoubleAdder values = new DoubleAdder();
        long rows =
            scan.materialize(
                ImmutableList.of("id", "k", "v1"),
                batch -> {
                  int id = batch.column("id");
                  int kColumn = batch.column("k");
                  int v1 = batch.column("v1");
                  long idSum = 0;
                  double v1Sum = 0;
                  for (int r = 0; r < batch.rows(); r++) {
                    idSum += batch.getLong(id, r) + batch.getInt(kColumn, r);
                    v1Sum += batch.getDouble(v1, r);
                  }
                  ids.add(idSum);
                  values.add(v1Sum);
                });
        yield new double[] {rows, ids.sum() + values.sum()};
      }
      default -> throw new IllegalArgumentException("Unknown query " + query);
    };
  }

  /**
   * One timed compaction: rewrite on the device, check the rewritten table with an independent CPU
   * scan (Q2's answer must not change), then roll the table back to the snapshot it had and delete
   * the files the rewrite wrote, so every run starts from the same merge-on-read table.
   */
  private static void rewriteRun(
      Table table, DeviceMergeOnReadScan scan, int run, String name, int threads, String query) {
    table.refresh();
    if ("sorted".equals(query) && table.sortOrder().isUnsorted()) {
      table.replaceSortOrder().asc("k").asc("v1").commit();
    }
    long snapshot = table.currentSnapshot().snapshotId();
    long cpuStart = PROCESS.getProcessCpuTime();
    long start = System.nanoTime();
    // zorder: the same compaction, each file written in Iceberg's Z-order of (k, v1); sorted: in
    // the table's sort order, (k, v1) ascending, set before the timed runs when the table has none.
    DeviceMergeOnReadScan.RewriteResult result =
        switch (query) {
          case "zorder" -> scan.rewriteDataFilesZOrder("k", "v1");
          case "sorted" -> scan.rewriteDataFilesSorted();
          default -> scan.rewriteDataFiles();
        };
    double millis = (System.nanoTime() - start) / 1e6;
    double cpuSeconds = (PROCESS.getProcessCpuTime() - cpuStart) / 1e9;
    table.refresh();
    double[] check = run(CpuMergeOnReadScan.of(table, threads), "q2");
    List<String> added = Lists.newArrayList();
    for (org.apache.iceberg.DataFile file : table.currentSnapshot().addedDataFiles(table.io())) {
      added.add(file.location());
    }
    // -Diceberg.tornadovm.keep-rewrite=true keeps the rewrite, for checking it with other readers.
    if (!Boolean.getBoolean("iceberg.tornadovm.keep-rewrite")) {
      table.manageSnapshots().rollbackTo(snapshot).commit();
      added.forEach(table.io()::deleteFile);
    }
    System.out.println(
        String.format(
            Locale.ROOT,
            "%s,gpu,%s,%d,%s,%.1f,%d,%.6e,\"%s; %d data + %d delete files -> %d files\",%.2f",
            name,
            query,
            run,
            run == 0 ? "yes" : "no",
            millis,
            (long) check[0],
            check[1],
            scan.metrics(),
            result.dataFilesRemoved(),
            result.deleteFilesRemoved(),
            result.dataFilesAdded(),
            cpuSeconds));
  }

  /** Q5's filter: ids are ascending file by file, so the id bound prunes three files in four. */
  static Expression filterOf(String query) {
    if ("tpch1".equals(query)) {
      return Expressions.lessThanOrEqual("l_shipdate", "1998-09-02");
    }
    if ("tpch6".equals(query)) {
      return Expressions.and(
          Expressions.and(
              Expressions.greaterThanOrEqual("l_shipdate", "1994-01-01"),
              Expressions.lessThan("l_shipdate", "1995-01-01")),
          Expressions.and(
              Expressions.and(
                  Expressions.greaterThanOrEqual("l_discount", 0.05),
                  Expressions.lessThanOrEqual("l_discount", 0.07)),
              Expressions.lessThan("l_quantity", 24.0)));
    }
    if ("q5".equals(query)) {
      return Expressions.and(
          Expressions.lessThan("id", 25_000_000L), Expressions.greaterThan("v2", 0.5));
    }
    return Expressions.alwaysTrue();
  }

  private static double sum(AggregateResult result) {
    double total = 0;
    for (AggregateResult.Group group : result.groups().values()) {
      for (double value : group.sums()) {
        total += value;
      }
    }
    return total;
  }

  /** Iceberg's own row-at-a-time merge-on-read reader, one thread: the reference answer. */
  private static double[] generics(Table table, String query) throws IOException {
    if (query.startsWith("tpch")) {
      return genericsTpch(table, query);
    }
    List<String> columns =
        "q3".equals(query)
            ? ImmutableList.of("id", "k", "v1")
            : "q1".equals(query)
                ? ImmutableList.of("v1")
                : "q4".equals(query)
                    ? ImmutableList.of("id", "v1")
                    : ImmutableList.of("id", "k", "v1", "v2", "v3", "v4");
    long rows = 0;
    double checksum = 0;
    Set<Integer> groups = Sets.newHashSet();
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(table).select(columns).where(filterOf(query)).build()) {
      for (Record record : records) {
        rows++;
        switch (query) {
          case "q1" -> checksum += (Double) record.getField("v1");
          case "q4" -> {
            long id = (Long) record.getField("id");
            groups.add((BucketUtil.hash(id) & Integer.MAX_VALUE) % 16);
            checksum += (Double) record.getField("v1");
          }
          case "q5" -> {
            groups.add((Integer) record.getField("k"));
            checksum += (Double) record.getField("v1");
          }
          case "q2" -> {
            groups.add((Integer) record.getField("k"));
            for (String sum : SUMS) {
              checksum += (Double) record.getField(sum);
            }
          }
          default ->
              checksum +=
                  (Long) record.getField("id")
                      + (Integer) record.getField("k")
                      + (Double) record.getField("v1");
        }
      }
    }
    return new double[] {rows, checksum + groups.size()};
  }

  /** TPC-H Q1 and Q6 row by row with Iceberg's own reader: the oracle. */
  private static double[] genericsTpch(Table table, String query) throws IOException {
    long rows = 0;
    double checksum = 0;
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(table)
            .select("l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_shipdate")
            .where(filterOf(query))
            .build()) {
      for (Record record : records) {
        rows++;
        double price = (Double) record.getField("l_extendedprice");
        double discount = (Double) record.getField("l_discount");
        if ("tpch6".equals(query)) {
          checksum += price * discount;
        } else {
          checksum +=
              (Double) record.getField("l_quantity")
                  + price
                  + price * (1 - discount)
                  + price * (1 - discount) * (1 + (Double) record.getField("l_tax"));
        }
      }
    }
    return new double[] {rows, checksum};
  }
}
