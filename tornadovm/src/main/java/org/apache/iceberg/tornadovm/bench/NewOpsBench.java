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

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.tornadovm.CpuMergeOnReadScan;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;

/**
 * The maintenance operations beyond conversion, timed on the device, each run checked and rolled
 * back so that every run starts from the same table.
 *
 * <pre>
 *   NewOpsBench &lt;warehouse&gt; deletekeys &lt;runs&gt; &lt;base table&gt; &lt;key count&gt;...
 *   NewOpsBench &lt;warehouse&gt; convertv2 &lt;runs&gt; &lt;v2 table&gt;...
 * </pre>
 *
 * <p>{@code deletekeys} works on a v3 twin of the base table ({@code <base>_v3keys}, its data
 * files, nothing copied): each run deletes that many distinct existing ids and checks that exactly
 * that many rows went. {@code convertv2} converts a v2 table's equality deletes into file-scoped
 * position-delete files and checks the live rows did not change. One CSV line a run: operation,
 * table, keys, run, warm-up, ms, rows deleted, delete files written, host CPU-s, phases.
 */
public final class NewOpsBench {
  private NewOpsBench() {}

  private static final com.sun.management.OperatingSystemMXBean PROCESS =
      (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

  public static void main(String[] args) throws Exception {
    String warehouse = args[0];
    String operation = args[1];
    int runs = Integer.parseInt(args[2]);
    int threads = Integer.getInteger("checkThreads", 32);
    System.out.println(
        "operation,table,keys,run,warmup,ms,rows_deleted,files_written,cpu_s,phases");
    if ("deletekeys".equals(operation)) {
      Table twin = twin(warehouse, args[3]);
      long rows = live(twin, threads);
      long snapshot = twin.currentSnapshot().snapshotId();
      try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(twin)) {
        for (int k = 4; k < args.length; k++) {
          int count = Integer.parseInt(args[k]);
          for (int run = 0; run < runs; run++) {
            Random random = new Random(count * 31L + run);
            Set<Long> keys = Sets.newLinkedHashSet();
            while (keys.size() < count) {
              keys.add((long) (random.nextDouble() * rows));
            }
            long cpu = PROCESS.getProcessCpuTime();
            long start = System.nanoTime();
            DeviceMergeOnReadScan.ConversionResult result = device.deleteKeys("id", keys);
            double millis = (System.nanoTime() - start) / 1e6;
            double cpuSeconds = (PROCESS.getProcessCpuTime() - cpu) / 1e9;
            twin.refresh();
            long after = live(twin, threads);
            if (after != rows - count) {
              throw new IllegalStateException(
                  args[3] + ": " + count + " keys deleted " + (rows - after) + " rows");
            }
            print(
                "deletekeys",
                args[3],
                count,
                run,
                millis,
                rows - after,
                result,
                cpuSeconds,
                device);
            rollBack(twin, snapshot);
          }
        }
      }
    } else if ("convertv2".equals(operation)) {
      for (int t = 3; t < args.length; t++) {
        Table table = MorTables.load(warehouse + "/" + args[t]);
        if (MorTables.formatVersion(table) != 2) {
          throw new IllegalArgumentException(args[t] + " is not a v2 table");
        }
        long rows = live(table, threads);
        long snapshot = table.currentSnapshot().snapshotId();
        try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
          for (int run = 0; run < runs; run++) {
            long cpu = PROCESS.getProcessCpuTime();
            long start = System.nanoTime();
            DeviceMergeOnReadScan.ConversionResult result = device.convertEqualityDeletes();
            double millis = (System.nanoTime() - start) / 1e6;
            double cpuSeconds = (PROCESS.getProcessCpuTime() - cpu) / 1e9;
            table.refresh();
            if (live(table, threads) != rows) {
              throw new IllegalStateException(args[t] + ": the conversion changed the table");
            }
            print(
                "convertv2",
                args[t],
                0,
                run,
                millis,
                result.deletedRows(),
                result,
                cpuSeconds,
                device);
            rollBack(table, snapshot);
          }
        }
      }
    } else {
      throw new IllegalArgumentException("Unknown operation " + operation);
    }
    System.exit(0);
  }

  private static void print(
      String operation,
      String table,
      int keys,
      int run,
      double millis,
      long deleted,
      DeviceMergeOnReadScan.ConversionResult result,
      double cpuSeconds,
      DeviceMergeOnReadScan device) {
    System.out.printf(
        Locale.ROOT,
        "%s,%s,%d,%d,%s,%.1f,%d,%d,%.2f,\"%s\"%n",
        operation,
        table,
        keys,
        run,
        run == 0 ? "yes" : "no",
        millis,
        deleted,
        result.vectorsAdded(),
        cpuSeconds,
        device.metrics().phases());
  }

  private static long live(Table table, int threads) {
    return CpuMergeOnReadScan.of(table, threads).aggregate(null, "v1").rows();
  }

  /** A v3 table over the base table's data files, made once. */
  private static Table twin(String warehouse, String name) throws Exception {
    String location = warehouse + "/" + name + "_v3keys";
    if (new File(location, "metadata").isDirectory()) {
      return MorTables.load(location);
    }
    Table source = MorTables.load(warehouse + "/" + name);
    List<DataFile> data = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = source.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        if (!task.deletes().isEmpty()) {
          throw new IllegalArgumentException(name + " has deletes; use a table without");
        }
        data.add(task.file());
      }
    }
    Table twin = MorTables.create(location, source.schema(), 3);
    MorTables.append(twin, data);
    return twin;
  }

  /** Back to the snapshot before the run; the delete files the run wrote are deleted. */
  private static void rollBack(Table table, long snapshot) {
    table.refresh();
    List<String> written = Lists.newArrayList();
    for (DeleteFile file : table.currentSnapshot().addedDeleteFiles(table.io())) {
      written.add(file.location());
    }
    table.manageSnapshots().rollbackTo(snapshot).commit();
    written.forEach(table.io()::deleteFile);
  }
}
