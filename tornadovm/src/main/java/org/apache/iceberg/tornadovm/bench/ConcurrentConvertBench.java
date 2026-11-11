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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.tornadovm.AggregateResult;
import org.apache.iceberg.tornadovm.CpuMergeOnReadScan;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;
import org.apache.iceberg.tornadovm.MergeOnReadScan;

/**
 * Converts equality deletes while another writer keeps committing equality deletes to the same
 * table, and checks that no conversion changes the table's answer.
 *
 * <p>Two fresh v3 copies of a v2 table with equality deletes are made: {@code TABLE_race}, which
 * the conversions run on, and {@code TABLE_oracle}, which is never converted. A writer thread
 * commits the same equality deletes ({@code keysPerCommit} random ids, every {@code intervalMs}) to
 * both. Meanwhile the main thread converts {@code TABLE_race} again and again; a conversion that
 * meets a concurrent delete must fail its commit, and is counted. After the writer stops, the two
 * tables must give the same grouped aggregate. One CSV line a conversion attempt, then a summary.
 *
 * <pre>
 *   ConcurrentConvertBench &lt;warehouse&gt; &lt;gpu|cpu&gt; &lt;attempts&gt; &lt;table&gt;
 *   -DkeysPerCommit=1000 -DintervalMs=500
 * </pre>
 */
public final class ConcurrentConvertBench {
  private ConcurrentConvertBench() {}

  public static void main(String[] args) throws Exception {
    String warehouse = args[0];
    String engine = args[1];
    int attempts = Integer.parseInt(args[2]);
    String name = args[3];
    int keysPerCommit = Integer.getInteger("keysPerCommit", 1000);
    long intervalMs = Long.getLong("intervalMs", 500);
    int threads = Integer.getInteger("threads", Runtime.getRuntime().availableProcessors());

    Table race = copy(warehouse, name, "race");
    Table oracle = copy(warehouse, name, "oracle");
    long totalRows = 0;
    try (CloseableIterable<FileScanTask> tasks = race.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        totalRows += task.file().recordCount();
      }
    }

    AtomicBoolean stop = new AtomicBoolean();
    AtomicInteger writerCommits = new AtomicInteger();
    long rows = totalRows;
    Thread writer =
        new Thread(
            () -> {
              Table raceWriter = MorTables.load(race.location());
              Table oracleWriter = MorTables.load(oracle.location());
              SplittableRandom random = new SplittableRandom(42);
              while (!stop.get()) {
                List<Object> keys = randomIds(keysPerCommit, rows, random);
                oracleWriter.refresh();
                MorTables.deleteEqual(oracleWriter, "id", keys);
                raceWriter.refresh();
                MorTables.deleteEqual(raceWriter, "id", keys);
                writerCommits.incrementAndGet();
                try {
                  Thread.sleep(intervalMs);
                } catch (InterruptedException e) {
                  return;
                }
              }
            },
            "writer");

    System.out.println("table,engine,attempt,outcome,ms,detail");
    int committed = 0;
    int failed = 0;
    writer.start();
    try (DeviceMergeOnReadScan device =
        "gpu".equals(engine) ? DeviceMergeOnReadScan.of(race) : null) {
      for (int attempt = 0; attempt < attempts; attempt++) {
        race.refresh();
        long start = System.nanoTime();
        String outcome;
        String detail;
        try {
          DeviceMergeOnReadScan.ConversionResult result =
              device != null
                  ? device.convertEqualityDeletes()
                  : CpuMergeOnReadScan.of(race, threads).convertEqualityDeletes();
          outcome = "committed";
          detail =
              result.equalityDeletesRemoved()
                  + " eq files -> "
                  + result.vectorsAdded()
                  + " vectors";
          committed++;
        } catch (ValidationException | CommitFailedException e) {
          outcome = "failed";
          detail = e.getClass().getSimpleName() + ": " + firstLine(e.getMessage());
          failed++;
        }
        System.out.printf(
            Locale.ROOT,
            "%s,%s,%d,%s,%.1f,\"%s\"%n",
            name,
            engine,
            attempt,
            outcome,
            (System.nanoTime() - start) / 1e6,
            detail.replace('"', '\''));
      }
    } finally {
      stop.set(true);
      writer.join();
    }

    race.refresh();
    oracle.refresh();
    double[] converted = q2(CpuMergeOnReadScan.of(race, threads));
    double[] expected = q2(CpuMergeOnReadScan.of(oracle, threads));
    boolean equal =
        converted[0] == expected[0]
            && Math.abs(converted[1] - expected[1]) <= 1e-6 * Math.abs(expected[1]);
    System.out.printf(
        Locale.ROOT,
        "summary,%s,%s,attempts=%d committed=%d failed=%d writer_commits=%d live_rows=%d oracle_rows=%d"
            + " answers_equal=%s%n",
        name,
        engine,
        attempts,
        committed,
        failed,
        writerCommits.get(),
        (long) converted[0],
        (long) expected[0],
        equal ? "yes" : "no");
    System.exit(equal ? 0 : 1);
  }

  /** A fresh v3 table over the data files and equality deletes of a v2 benchmark table. */
  private static Table copy(String warehouse, String name, String suffix) throws IOException {
    Table source = MorTables.load(warehouse + "/" + name);
    List<DataFile> data = Lists.newArrayList();
    Set<DeleteFile> equality = Sets.newLinkedHashSet();
    try (CloseableIterable<FileScanTask> tasks = source.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        data.add(task.file());
        for (DeleteFile delete : task.deletes()) {
          if (delete.content() == FileContent.EQUALITY_DELETES) {
            equality.add(delete);
          }
        }
      }
    }
    String location = warehouse + "/" + name + "_" + suffix + "_" + System.currentTimeMillis();
    Table table = MorTables.create(location, source.schema(), 3);
    MorTables.append(table, data);
    RowDelta delta = table.newRowDelta();
    equality.forEach(delta::addDeletes);
    delta.commit();
    table.refresh();
    return table;
  }

  private static List<Object> randomIds(int count, long totalRows, SplittableRandom random) {
    long[] ids = new long[count];
    for (int i = 0; i < count; i++) {
      ids[i] = random.nextLong(totalRows);
    }
    Arrays.sort(ids);
    List<Object> keys = Lists.newArrayListWithCapacity(count);
    for (long id : ids) {
      keys.add(id);
    }
    return keys;
  }

  /** Q2 of {@link MorBench}: live rows, and the sums over the groups of k. */
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

  private static String firstLine(String message) {
    if (message == null) {
      return "";
    }
    int newline = message.indexOf('\n');
    String line = newline < 0 ? message : message.substring(0, newline);
    return line.length() > 160 ? line.substring(0, 160) : line;
  }
}
