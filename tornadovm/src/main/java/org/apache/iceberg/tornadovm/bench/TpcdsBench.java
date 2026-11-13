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

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Locale;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.tornadovm.CpuMergeOnReadScan;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;
import org.apache.iceberg.tornadovm.DeviceValue;
import org.apache.iceberg.tornadovm.GroupedAggregate;
import org.apache.iceberg.types.Types;

/**
 * TPC-DS store_sales operations on the tables of {@code tpcds_partitioned_setup.py}, each run
 * checked against the table's answer before it and, for maintenance, rolled back afterwards.
 *
 * <pre>
 *   TpcdsBench &lt;warehouse&gt; agg &lt;runs&gt; &lt;table&gt;...       sales by store on the GPU
 *   TpcdsBench &lt;warehouse&gt; convert &lt;runs&gt; &lt;table&gt;...   equality deletes to vectors, GPU and CPU
 *   TpcdsBench &lt;warehouse&gt; sorted &lt;runs&gt; &lt;table&gt;...    compaction in the table's sort order
 * </pre>
 *
 * <p>The answer is {@code SELECT ss_store_sk, count(*), sum(ss_quantity), sum(ss_net_paid),
 * sum(ss_ext_sales_price) GROUP BY ss_store_sk}: its live rows and the sum of its sums. One CSV
 * line a run: table, engine, operation, run, warm-up, ms, live rows, checksum, host CPU-s, detail.
 */
public final class TpcdsBench {
  private TpcdsBench() {}

  private static final com.sun.management.OperatingSystemMXBean PROCESS =
      (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

  public static void main(String[] args) throws Exception {
    String warehouse = args[0];
    String operation = args[1];
    int runs = Integer.parseInt(args[2]);
    int threads = Integer.getInteger("threads", Runtime.getRuntime().availableProcessors());
    System.out.println("table,engine,operation,run,warmup,ms,live_rows,checksum,cpu_s,detail");
    for (int t = 3; t < args.length; t++) {
      Table table = MorTables.load(warehouse + "/" + args[t]);
      try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
        double[] answer = sales(table, device);
        for (String engine : "convert".equals(operation) ? List.of("gpu", "cpu") : List.of("gpu")) {
          for (int run = 0; run < runs; run++) {
            table.refresh();
            long snapshot = table.currentSnapshot().snapshotId();
            long cpu = PROCESS.getProcessCpuTime();
            long start = System.nanoTime();
            String operationDetail =
                switch (operation) {
                  case "agg" -> {
                    answer = sales(table, device);
                    yield device.metrics().toString();
                  }
                  case "convert" -> {
                    DeviceMergeOnReadScan.ConversionResult result =
                        "gpu".equals(engine)
                            ? device.convertEqualityDeletes()
                            : CpuMergeOnReadScan.of(table, threads).convertEqualityDeletes();
                    yield result.equalityDeletesRemoved()
                        + " equality delete files -> "
                        + result.vectorsAdded()
                        + " vectors";
                  }
                  case "sorted" -> {
                    DeviceMergeOnReadScan.RewriteResult result = device.rewriteDataFilesSorted();
                    yield result.dataFilesRemoved()
                        + " data + "
                        + result.deleteFilesRemoved()
                        + " delete files -> "
                        + result.dataFilesAdded();
                  }
                  default -> throw new IllegalArgumentException("Unknown operation " + operation);
                };
            double millis = (System.nanoTime() - start) / 1e6;
            double cpuSeconds = (PROCESS.getProcessCpuTime() - cpu) / 1e9;
            String detail = operationDetail;
            if (!"agg".equals(operation)) {
              table.refresh();
              detail += String.format(Locale.ROOT, ", %.2f GB written", writtenBytes(table) / 1e9);
              double[] after = sales(table, device);
              if (after[0] != answer[0] || Math.abs(after[1] - answer[1]) > 1e-6 * answer[1]) {
                throw new IllegalStateException(
                    args[t] + ": " + operation + " changed the answer: " + after[0]);
              }
              rollBack(table, snapshot);
            }
            System.out.printf(
                Locale.ROOT,
                "%s,%s,%s,%d,%s,%.1f,%d,%.6e,%.2f,\"%s\"%n",
                args[t],
                engine,
                operation,
                run,
                run == 0 ? "yes" : "no",
                millis,
                (long) answer[0],
                answer[1],
                cpuSeconds,
                detail);
          }
        }
      }
    }
    System.exit(0);
  }

  private static final List<String> SUMS =
      ImmutableList.of("ss_quantity", "ss_net_paid", "ss_ext_sales_price");

  private static double[] sales(Table table, DeviceMergeOnReadScan device) {
    GroupedAggregate result =
        device.aggregate(
            ImmutableList.of("ss_store_sk"),
            ImmutableList.of(
                DeviceValue.of(SUMS.get(0)),
                DeviceValue.of(SUMS.get(1)),
                DeviceValue.of(SUMS.get(2))));
    // A decimal sums as its unscaled value: scaled here, to compare with Spark's sums.
    double[] unit = new double[SUMS.size()];
    for (int v = 0; v < unit.length; v++) {
      org.apache.iceberg.types.Type type = table.schema().findType(SUMS.get(v));
      unit[v] = type instanceof Types.DecimalType decimal ? Math.pow(10, -decimal.scale()) : 1;
    }
    double checksum = 0;
    for (double[] totals : result.groups().values()) {
      for (int v = 0; v < unit.length; v++) {
        checksum += totals[v] * unit[v];
      }
    }
    return new double[] {result.rows(), checksum};
  }

  /** The bytes of the files the current snapshot added. */
  private static long writtenBytes(Table table) {
    long bytes = 0;
    for (DataFile file : table.currentSnapshot().addedDataFiles(table.io())) {
      bytes += file.fileSizeInBytes();
    }
    for (DeleteFile file : table.currentSnapshot().addedDeleteFiles(table.io())) {
      bytes += file.fileSizeInBytes();
    }
    return bytes;
  }

  /** Back to the snapshot before the run; the files the run wrote are deleted. */
  private static void rollBack(Table table, long snapshot) {
    table.refresh();
    List<String> written = Lists.newArrayList();
    for (DataFile file : table.currentSnapshot().addedDataFiles(table.io())) {
      written.add(file.location());
    }
    for (DeleteFile file : table.currentSnapshot().addedDeleteFiles(table.io())) {
      written.add(file.location());
    }
    table.manageSnapshots().rollbackTo(snapshot).commit();
    written.forEach(table.io()::deleteFile);
  }
}
