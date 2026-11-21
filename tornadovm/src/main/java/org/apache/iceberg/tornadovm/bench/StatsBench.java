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
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;

/**
 * Iceberg's compute_table_stats on the device, timed.
 *
 * <pre>
 *   StatsBench &lt;warehouse&gt; &lt;columns,comma-separated&gt; &lt;runs&gt; &lt;table&gt;...
 * </pre>
 *
 * <p>One CSV line a run: table, run, warm-up, ms, host CPU-s, each column's NDV estimate, phases.
 */
public final class StatsBench {
  private StatsBench() {}

  private static final com.sun.management.OperatingSystemMXBean PROCESS =
      (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

  public static void main(String[] args) throws Exception {
    String[] columns = args[1].split(",");
    int runs = Integer.parseInt(args[2]);
    System.out.println("table,run,warmup,ms,cpu_seconds,ndv,phases");
    for (int t = 3; t < args.length; t++) {
      Table table = MorTables.load(args[0] + "/" + args[t]);
      try (DeviceMergeOnReadScan scan = DeviceMergeOnReadScan.of(table)) {
        for (int run = 0; run < runs; run++) {
          long cpu = PROCESS.getProcessCpuTime();
          long start = System.nanoTime();
          Map<String, Long> ndv = scan.computeTableStats(columns);
          System.out.printf(
              Locale.ROOT,
              "%s,%d,%s,%.1f,%.2f,\"%s\",\"%s\"%n",
              args[t],
              run,
              run == 0 ? "yes" : "no",
              (System.nanoTime() - start) / 1e6,
              (PROCESS.getProcessCpuTime() - cpu) / 1e9,
              ndv,
              scan.metrics().phases());
        }
      }
    }
    System.exit(0);
  }
}
