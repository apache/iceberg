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
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.tornadovm.AggregateResult;
import org.apache.iceberg.tornadovm.CpuMergeOnReadScan;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;
import org.apache.iceberg.tornadovm.MergeOnReadScan;

/**
 * Equality deletes into deletion vectors, timed: the GPU conversion against the same algorithm on
 * every core.
 *
 * <pre>
 *   ConvertBench &lt;warehouse&gt; &lt;engine: gpu|cpu&gt; &lt;runs&gt; &lt;table&gt;...
 * </pre>
 *
 * <p>Each named table is a v2 table of {@link GenerateTables} with equality deletes ({@code eq10k},
 * {@code eq1m}, {@code eq10m}). Its v3 twin, named with {@code _v3} appended, holds the same data
 * files and the same equality-delete files, added in a later commit, so they apply exactly as they
 * do in the v2 table; nothing is copied. A run converts the twin, checks with a CPU scan that the
 * live rows and their sums did not change, times a scan before and after, then rolls the twin back
 * and deletes the deletion vectors it wrote, so every run starts from the same equality deletes.
 *
 * <p>Output, one CSV line a run: table, engine, run, warm-up, conversion ms, data files, equality
 * files removed, vectors, deleted rows, scan ms with equality deletes, scan ms with vectors.
 */
public final class ConvertBench {
  private ConvertBench() {}

  private static final com.sun.management.OperatingSystemMXBean PROCESS =
      (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public static void main(String[] args) throws Exception {
    String warehouse = args[0];
    String engine = args[1];
    int runs = Integer.parseInt(args[2]);
    int threads = Integer.getInteger("threads", Runtime.getRuntime().availableProcessors());
    // The scans that check a conversion run in the GPU JVM, whose heap stays under 32 GB: on all of
    // a 128-thread host their buffers outgrow it at 500M rows. The timed CPU conversion keeps all.
    int checkThreads = Math.min(threads, Integer.getInteger("checkThreads", 32));
    System.out.println(
        "table,engine,run,warmup,convert_ms,data_files,eq_removed,vectors,deleted_rows,"
            + "scan_eq_ms,scan_dv_ms,cpu_seconds,phases");
    for (int t = 3; t < args.length; t++) {
      String name = args[t];
      Table twin = twin(warehouse, name);
      long snapshot = twin.currentSnapshot().snapshotId();
      double[] expected = q2(CpuMergeOnReadScan.of(twin, checkThreads));
      double[] source =
          q2(CpuMergeOnReadScan.of(MorTables.load(warehouse + "/" + name), checkThreads));
      if (source[0] != expected[0]) {
        throw new IllegalStateException(
            name + "_v3 has " + expected[0] + " live rows, the v2 table " + source[0]);
      }
      try (DeviceMergeOnReadScan device =
          "gpu".equals(engine) ? DeviceMergeOnReadScan.of(twin) : null) {
        for (int run = 0; run < runs; run++) {
          twin.refresh();
          MergeOnReadScan reader = device != null ? device : CpuMergeOnReadScan.of(twin, threads);
          long scanStart = System.nanoTime();
          q2(reader);
          double scanEq = (System.nanoTime() - scanStart) / 1e6;

          long cpuStart = PROCESS.getProcessCpuTime();
          long start = System.nanoTime();
          DeviceMergeOnReadScan.ConversionResult result =
              device != null
                  ? device.convertEqualityDeletes()
                  : CpuMergeOnReadScan.of(twin, threads).convertEqualityDeletes();
          double millis = (System.nanoTime() - start) / 1e6;
          double cpuSeconds = (PROCESS.getProcessCpuTime() - cpuStart) / 1e9;

          twin.refresh();
          double[] after = q2(CpuMergeOnReadScan.of(twin, checkThreads));
          if (after[0] != expected[0]
              || Math.abs(after[1] - expected[1]) > 1e-6 * Math.abs(expected[1])) {
            throw new IllegalStateException(
                name + ": the conversion changed the table: " + after[0] + " vs " + expected[0]);
          }
          try (DeviceMergeOnReadScan fresh =
              device != null ? DeviceMergeOnReadScan.of(twin) : null) {
            MergeOnReadScan dvReader = fresh != null ? fresh : CpuMergeOnReadScan.of(twin, threads);
            q2(dvReader); // a fresh scan's first read compiles and caches; time the second
            long dvStart = System.nanoTime();
            q2(dvReader);
            double scanDv = (System.nanoTime() - dvStart) / 1e6;
            System.out.printf(
                Locale.ROOT,
                "%s,%s,%d,%s,%.1f,%d,%d,%d,%d,%.1f,%.1f,%.2f,\"%s\"%n",
                name,
                engine,
                run,
                run == 0 ? "yes" : "no",
                millis,
                result.dataFiles(),
                result.equalityDeletesRemoved(),
                result.vectorsAdded(),
                result.deletedRows(),
                scanEq,
                scanDv,
                cpuSeconds,
                device != null ? device.metrics().phases() : "");
          }
          rollBack(twin, snapshot);
        }
      }
    }
    // Object-store clients (S3FileIO) start threads that are not daemons.
    System.exit(0);
  }

  /** Q2 of {@link MorBench}: live rows, and the sums of v1 over the groups of k. */
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

  /** The v3 twin of a v2 table: its data files, then its equality-delete files, in two commits. */
  private static Table twin(String warehouse, String name) throws Exception {
    String location = warehouse + "/" + name + "_v3";
    if (new File(location, "metadata").isDirectory()) {
      return MorTables.load(location);
    }
    Table source = MorTables.load(warehouse + "/" + name);
    List<DataFile> data = Lists.newArrayList();
    Set<DeleteFile> equality = Sets.newLinkedHashSet();
    try (CloseableIterable<FileScanTask> tasks = source.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        data.add(task.file());
        for (DeleteFile delete : task.deletes()) {
          if (delete.content() != FileContent.EQUALITY_DELETES) {
            throw new IllegalArgumentException(name + " has deletes other than equality deletes");
          }
          equality.add(delete);
        }
      }
    }
    Table twin = MorTables.create(location, source.schema(), 3);
    MorTables.append(twin, data);
    RowDelta delta = twin.newRowDelta();
    equality.forEach(delta::addDeletes);
    delta.commit();
    twin.refresh();
    return twin;
  }

  /** Back to the snapshot with equality deletes; the vectors the conversion wrote are deleted. */
  private static void rollBack(Table twin, long snapshot) {
    twin.refresh();
    List<String> written = Lists.newArrayList();
    for (DeleteFile file : twin.currentSnapshot().addedDeleteFiles(twin.io())) {
      written.add(file.location());
    }
    twin.manageSnapshots().rollbackTo(snapshot).commit();
    Sets.newHashSet(written).forEach(twin.io()::deleteFile);
  }
}
