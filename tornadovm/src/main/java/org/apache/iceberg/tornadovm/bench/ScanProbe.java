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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;

/**
 * How the device reads a projection of a table: as one scan in big batches, and as concurrent
 * one-file requests -- what Spark's tasks send the GPU reader. Times are ms, best of the runs.
 *
 * <pre>run.sh gpu with MAIN=org.apache.iceberg.tornadovm.bench.ScanProbe: table col,col runs</pre>
 */
public final class ScanProbe {
  private ScanProbe() {}

  public static void main(String[] args) throws Exception {
    Table table = new HadoopTables().load(args[0]);
    List<String> columns = List.of(args[1].split(","));
    int runs = Integer.parseInt(args[2]);
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
      planned.forEach(tasks::add);
    }
    ExecutorService spark = Executors.newFixedThreadPool(32);
    try (DeviceMergeOnReadScan scan = DeviceMergeOnReadScan.of(table)) {
      double whole = Double.MAX_VALUE;
      double perFile = Double.MAX_VALUE;
      long rows = 0;
      for (int run = 0; run < runs; run++) {
        AtomicLong live = new AtomicLong();
        long start = System.nanoTime();
        scan.materialize(columns, batch -> live.addAndGet(batch.rows()));
        whole = Math.min(whole, (System.nanoTime() - start) / 1e6);
        rows = live.get();

        AtomicLong live2 = new AtomicLong();
        start = System.nanoTime();
        List<CompletableFuture<Void>> requests = Lists.newArrayList();
        for (FileScanTask task : tasks) {
          requests.add(
              CompletableFuture.runAsync(
                  () ->
                      scan.materialize(
                          List.of(task), columns, batch -> live2.addAndGet(batch.rows())),
                  spark));
        }
        requests.forEach(CompletableFuture::join);
        perFile = Math.min(perFile, (System.nanoTime() - start) / 1e6);
        if (live2.get() != rows) {
          throw new IllegalStateException(live2.get() + " rows per file, " + rows + " whole");
        }
      }
      System.out.printf(
          "%s %s: %d files, %d live rows; whole scan %.1f ms, %d one-file requests %.1f ms%n",
          table.name(), columns, tasks.size(), rows, whole, tasks.size(), perFile);
    } finally {
      spark.shutdown();
    }
  }
}
