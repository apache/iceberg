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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.tornadovm.AggregateResult;
import org.apache.iceberg.tornadovm.CpuMergeOnReadScan;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;
import org.apache.iceberg.tornadovm.MergeOnReadScan;

/**
 * A change-data-capture stream into an Iceberg table, as Flink's upsert sink writes it, with and
 * without equality deletes converted into deletion vectors after each commit.
 *
 * <p>Both tables start from the data files of a {@link GenerateTables} table (v3, nothing copied).
 * Each round is one {@code RowDelta}: a new data file with {@code upserts} rows whose ids already
 * exist (new values), and an equality-delete file on {@code id} for the same ids. The deletes drop
 * every older version and not the new one, which has the deletes' sequence number. Table {@code eq}
 * keeps its equality deletes; table {@code dv} has them converted on the GPU after every round.
 * After each round both are read with Q2 on the GPU and on all CPU cores, and every answer must be
 * the same in both tables.
 *
 * <pre>
 *   CdcBench &lt;warehouse&gt; &lt;base table&gt; &lt;rounds&gt; &lt;upserts a round&gt;
 * </pre>
 *
 * <p>One CSV line a round and table: round, table, live rows, equality-delete files, convert ms,
 * GPU Q2 ms, CPU Q2 ms, GPU host CPU-s, CPU host CPU-s.
 */
public final class CdcBench {
  private CdcBench() {}

  private static final com.sun.management.OperatingSystemMXBean PROCESS =
      (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

  public static void main(String[] args) throws Exception {
    String warehouse = args[0];
    Table base = MorTables.load(warehouse + "/" + args[1]);
    int rounds = Integer.parseInt(args[2]);
    int upserts = Integer.parseInt(args[3]);
    int threads = Integer.getInteger("threads", Runtime.getRuntime().availableProcessors());
    List<DataFile> data = Lists.newArrayList();
    long rows = 0;
    try (CloseableIterable<FileScanTask> tasks = base.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        data.add(task.file());
        rows += task.file().recordCount();
      }
    }
    Table eq = fresh(warehouse + "/" + args[1] + "_cdc_eq", base, data);
    Table dv = fresh(warehouse + "/" + args[1] + "_cdc_dv", base, data);
    // Keys deleted as deletion vectors before the new rows are appended: no equality delete ever.
    Table dk = fresh(warehouse + "/" + args[1] + "_cdc_dk", base, data);
    System.out.println(
        "round,table,live_rows,equality_delete_files,convert_ms,gpu_q2_ms,cpu_q2_ms,gpu_cpu_s,cpu_cpu_s");
    try (DeviceMergeOnReadScan eqGpu = DeviceMergeOnReadScan.of(eq);
        DeviceMergeOnReadScan dvGpu = DeviceMergeOnReadScan.of(dv);
        DeviceMergeOnReadScan dkGpu = DeviceMergeOnReadScan.of(dk)) {
      // A warm-up read of each: kernels compiled, plans built.
      q2(eqGpu);
      q2(dvGpu);
      q2(dkGpu);
      for (int round = 1; round <= rounds; round++) {
        Random random = new Random(round);
        // Distinct ids: an upsert commit holds one new version a key.
        Set<Object> distinct = Sets.newLinkedHashSet();
        while (distinct.size() < upserts) {
          distinct.add((long) (random.nextDouble() * rows));
        }
        List<Object> ids = Lists.newArrayList(distinct);
        upsert(eq, ids, round);
        upsert(dv, ids, round);
        dv.refresh();
        long start = System.nanoTime();
        dvGpu.convertEqualityDeletes();
        double convertMillis = (System.nanoTime() - start) / 1e6;
        start = System.nanoTime();
        dkGpu.deleteKeys("id", ids);
        double deleteMillis = (System.nanoTime() - start) / 1e6;
        append(dk, ids, round);
        double[] eqResult = report(round, "eq", eq, eqGpu, threads, 0);
        double[] dvResult = report(round, "dv", dv, dvGpu, threads, convertMillis);
        double[] dkResult = report(round, "dk", dk, dkGpu, threads, deleteMillis);
        if (eqResult[0] != dkResult[0]
            || Math.abs(eqResult[1] - dkResult[1]) > 1e-6 * Math.abs(eqResult[1])) {
          throw new IllegalStateException(
              "round "
                  + round
                  + ": keys deleted as vectors read differently: "
                  + eqResult[0]
                  + " vs "
                  + dkResult[0]);
        }
        if (eqResult[0] != dvResult[0]
            || Math.abs(eqResult[1] - dvResult[1]) > 1e-6 * Math.abs(eqResult[1])) {
          throw new IllegalStateException(
              "round "
                  + round
                  + ": the converted table reads differently: "
                  + eqResult[0]
                  + " vs "
                  + dvResult[0]);
        }
      }
    }
    System.exit(0);
  }

  /** A v3 table holding the base table's data files, made anew. */
  private static Table fresh(String location, Table base, List<DataFile> data) throws Exception {
    File dir = new File(location);
    if (dir.exists()) {
      try (Stream<Path> paths = Files.walk(dir.toPath())) {
        paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
    }
    Table table = MorTables.create(location, base.schema(), 3);
    MorTables.append(table, data);
    return table;
  }

  /** One upsert commit: new versions of the ids, and equality deletes of their old versions. */
  private static void upsert(Table table, List<Object> ids, int round) {
    table.refresh();
    DataFile file = newVersions(table, ids, round);
    DeleteFile deletes = MorTables.writeEqualityDeletes(table, "id", ids, null);
    table.newRowDelta().addRows(file).addDeletes(deletes).commit();
  }

  /** The new versions alone, appended after their old versions were deleted by key. */
  private static void append(Table table, List<Object> ids, int round) {
    table.refresh();
    MorTables.append(table, List.of(newVersions(table, ids, round)));
  }

  private static DataFile newVersions(Table table, List<Object> ids, int round) {
    return MorTables.writeDataFile(
        table,
        ids.size(),
        i -> {
          long id = (Long) ids.get((int) i);
          GenericRecord row = GenericRecord.create(table.schema());
          row.set(0, id);
          row.set(1, (int) (id % 1000));
          row.set(2, round + i * 1e-9);
          row.set(3, (double) -round);
          row.set(4, (double) (i % 3));
          row.set(5, 1.0);
          return row;
        });
  }

  /** Reads the table with Q2 on the GPU and on the CPU and prints the round's line. */
  private static double[] report(
      int round, String name, Table table, DeviceMergeOnReadScan gpu, int threads, double convert) {
    table.refresh();
    long cpuStart = PROCESS.getProcessCpuTime();
    long start = System.nanoTime();
    double[] onGpu = q2(gpu);
    double gpuMillis = (System.nanoTime() - start) / 1e6;
    double gpuCpu = (PROCESS.getProcessCpuTime() - cpuStart) / 1e9;
    cpuStart = PROCESS.getProcessCpuTime();
    start = System.nanoTime();
    double[] onCpu = q2(CpuMergeOnReadScan.of(table, threads));
    double cpuMillis = (System.nanoTime() - start) / 1e6;
    double cpuCpu = (PROCESS.getProcessCpuTime() - cpuStart) / 1e9;
    if (onGpu[0] != onCpu[0] || Math.abs(onGpu[1] - onCpu[1]) > 1e-6 * Math.abs(onCpu[1])) {
      throw new IllegalStateException(name + " round " + round + ": GPU and CPU answers differ");
    }
    long equalityFiles = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      Set<String> seen = Sets.newHashSet();
      for (FileScanTask task : tasks) {
        for (DeleteFile delete : task.deletes()) {
          if (delete.content() == org.apache.iceberg.FileContent.EQUALITY_DELETES
              && seen.add(delete.location())) {
            equalityFiles++;
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    System.out.printf(
        Locale.ROOT,
        "%d,%s,%d,%d,%.1f,%.1f,%.1f,%.2f,%.2f%n",
        round,
        name,
        (long) onGpu[0],
        equalityFiles,
        convert,
        gpuMillis,
        cpuMillis,
        gpuCpu,
        cpuCpu);
    return onGpu;
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
