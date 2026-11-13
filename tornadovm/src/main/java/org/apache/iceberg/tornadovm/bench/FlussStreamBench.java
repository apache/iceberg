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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.tornadovm.AggregateResult;
import org.apache.iceberg.tornadovm.CpuMergeOnReadScan;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;
import org.apache.iceberg.tornadovm.MergeOnReadScan;

/**
 * Watches Iceberg tables that Apache Fluss keeps tiering primary-key changes into (equality deletes
 * every freshness round), and maintains some of them: every query interval the grouped aggregate
 * runs on the GPU over each table; every maintenance interval a table whose name ends in {@code
 * _gpu} has its equality deletes converted on the GPU, one ending in {@code _cpu} on the CPU twin,
 * and one ending in {@code _none} is left alone. A conversion that meets a concurrent tiering
 * commit fails validation and is retried at the next interval. When the stream has stopped and
 * every table is idle, the answers of all tables are compared.
 *
 * <pre>
 *   FlussStreamBench &lt;namespace dir&gt; &lt;minutes&gt; &lt;query s&gt; &lt;maintenance s&gt; &lt;table&gt;...
 * </pre>
 */
public final class FlussStreamBench {
  private FlussStreamBench() {}

  public static void main(String[] args) throws Exception {
    String namespace = args[0];
    long endAt = System.nanoTime() + (long) (Double.parseDouble(args[1]) * 60e9);
    long queryNanos = (long) (Double.parseDouble(args[2]) * 1e9);
    long maintainNanos = (long) (Double.parseDouble(args[3]) * 1e9);
    int threads = Integer.getInteger("threads", Runtime.getRuntime().availableProcessors());
    List<String> names = List.of(args).subList(4, args.length);
    List<Table> tables = Lists.newArrayList();
    List<DeviceMergeOnReadScan> scans = Lists.newArrayList();
    for (String name : names) {
      Table table = MorTables.load(namespace + "/" + name);
      tables.add(table);
      scans.add(DeviceMergeOnReadScan.of(table));
    }
    System.out.println(
        "t_s,table,event,ms,live_rows,checksum,data_files,equality_delete_files,position_delete_files,snapshot,detail");
    long start = System.nanoTime();
    long nextMaintain = start + maintainNanos;
    try {
      while (System.nanoTime() < endAt) {
        long round = System.nanoTime();
        for (int i = 0; i < tables.size(); i++) {
          query(start, names.get(i), tables.get(i), scans.get(i));
        }
        if (System.nanoTime() >= nextMaintain) {
          for (int i = 0; i < tables.size(); i++) {
            maintain(start, names.get(i), tables.get(i), scans.get(i), threads);
          }
          nextMaintain += maintainNanos;
        }
        long sleep = queryNanos - (System.nanoTime() - round);
        if (sleep > 0) {
          Thread.sleep(sleep / 1_000_000);
        }
      }
      // The stream has stopped: wait for tiering to go idle, convert once more, then compare.
      for (int i = 0; i < tables.size(); i++) {
        waitIdle(tables.get(i));
        maintain(start, names.get(i), tables.get(i), scans.get(i), threads);
      }
      System.out.printf(
          Locale.ROOT,
          "summary,answers_equal=%s%n",
          allEqual(start, names, tables, scans) ? "yes" : "no");
    } finally {
      scans.forEach(DeviceMergeOnReadScan::close);
    }
  }

  private static boolean allEqual(
      long start, List<String> names, List<Table> tables, List<DeviceMergeOnReadScan> scans) {
    double[] first = null;
    boolean equal = true;
    for (int i = 0; i < tables.size(); i++) {
      double[] answer = query(start, names.get(i), tables.get(i), scans.get(i));
      if (first == null) {
        first = answer;
      } else if (answer[0] != first[0]
          || Math.abs(answer[1] - first[1]) > 1e-6 * Math.abs(first[1])) {
        equal = false;
      }
    }
    return equal;
  }

  private static double[] query(long start, String name, Table table, MergeOnReadScan scan) {
    table.refresh();
    long began = System.nanoTime();
    double[] answer = q2(scan);
    print(start, name, "query", (System.nanoTime() - began) / 1e6, answer, table, "");
    return answer;
  }

  private static void maintain(
      long start, String name, Table table, DeviceMergeOnReadScan device, int threads) {
    boolean onGpu = name.endsWith("_gpu");
    if (!onGpu && !name.endsWith("_cpu")) {
      return;
    }
    table.refresh();
    long began = System.nanoTime();
    String detail;
    String event = "convert";
    try {
      DeviceMergeOnReadScan.ConversionResult result =
          onGpu
              ? device.convertEqualityDeletes()
              : CpuMergeOnReadScan.of(table, threads).convertEqualityDeletes();
      detail =
          result.equalityDeletesRemoved() + " eq files -> " + result.dataFiles() + " data files";
    } catch (ValidationException | CommitFailedException e) {
      event = "convert-failed";
      detail = e.getClass().getSimpleName();
    }
    table.refresh();
    print(
        start, name, event, (System.nanoTime() - began) / 1e6, new double[] {0, 0}, table, detail);
  }

  private static void waitIdle(Table table) throws InterruptedException {
    long last = -1;
    for (int quiet = 0; quiet < 3; ) {
      table.refresh();
      Snapshot current = table.currentSnapshot();
      long id = current == null ? 0 : current.snapshotId();
      quiet = id == last ? quiet + 1 : 0;
      last = id;
      Thread.sleep(30_000);
    }
  }

  private static void print(
      long start,
      String name,
      String event,
      double ms,
      double[] answer,
      Table table,
      String detail) {
    Snapshot current = table.currentSnapshot();
    Map<String, String> summary = current == null ? Map.of() : current.summary();
    System.out.printf(
        Locale.ROOT,
        "%.1f,%s,%s,%.1f,%d,%.6e,%s,%s,%s,%d,\"%s\"%n",
        (System.nanoTime() - start) / 1e9,
        name,
        event,
        ms,
        (long) answer[0],
        answer[1],
        summary.getOrDefault("total-data-files", ""),
        summary.getOrDefault("total-equality-deletes", "")
            + "/"
            + summary.getOrDefault("total-delete-files", ""),
        summary.getOrDefault("total-position-deletes", ""),
        current == null ? 0 : current.snapshotId(),
        detail);
  }

  private static double[] q2(MergeOnReadScan scan) {
    AggregateResult result = scan.aggregate("k", "v1", "v2", "v3", "v4");
    double sum = 0;
    for (AggregateResult.Group group : result.groups().values()) {
      for (double value : group.sums()) {
        sum += value;
      }
    }
    return new double[] {result.rows(), sum};
  }
}
