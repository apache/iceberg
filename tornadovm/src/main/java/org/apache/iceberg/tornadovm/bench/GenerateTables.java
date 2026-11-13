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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.LongStream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Writes the benchmark tables: one set of data files, shared by every table, and per table the
 * deletes of one scenario.
 *
 * <pre>
 *   GenerateTables &lt;warehouse&gt; &lt;files&gt; &lt;rowsPerFile&gt; [threads]
 * </pre>
 *
 * <p>Rows are {@code id = file * rowsPerFile + i} (unique), {@code k} in [0, 1000), and four
 * uniformly random doubles, all required. Tables, all unpartitioned:
 *
 * <ul>
 *   <li>{@code none} -- no deletes (v2);
 *   <li>{@code pos01}, {@code pos05}, {@code pos20} -- that percentage of every file deleted by
 *       position, one position-delete file per data file (v2);
 *   <li>{@code dv05} -- 5% by deletion vectors (v3);
 *   <li>{@code eq10k}, {@code eq1m}, {@code eq10m} -- equality deletes on {@code id}, that many
 *       random ids, applying to every data file (v2);
 *   <li>{@code mixed} -- {@code pos05} and {@code eq1m} together (v2).
 * </ul>
 */
public final class GenerateTables {

  private GenerateTables() {}

  public static void main(String[] args) throws Exception {
    String warehouse = new File(args[0]).getAbsolutePath();
    int files = Integer.parseInt(args[1]);
    long rowsPerFile = Long.parseLong(args[2]);
    int threads = args.length > 3 ? Integer.parseInt(args[3]) : 16;

    long start = System.nanoTime();
    Table base = MorTables.create(warehouse + "/none", MorTables.SCHEMA, 2);
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    List<Future<DataFile>> futures = Lists.newArrayList();
    for (int f = 0; f < files; f++) {
      long fileIndex = f;
      futures.add(
          pool.submit(
              () -> {
                SplittableRandom random = new SplittableRandom(1000 + fileIndex);
                GenericRecord template = GenericRecord.create(MorTables.SCHEMA);
                return MorTables.writeDataFile(
                    base,
                    rowsPerFile,
                    i -> {
                      Record row = template.copy();
                      row.set(0, fileIndex * rowsPerFile + i);
                      row.set(1, random.nextInt(1000));
                      row.set(2, random.nextDouble());
                      row.set(3, random.nextDouble());
                      row.set(4, random.nextDouble());
                      row.set(5, random.nextDouble());
                      return row;
                    });
              }));
    }
    List<DataFile> data = Lists.newArrayList();
    for (Future<DataFile> future : futures) {
      data.add(future.get());
    }
    pool.shutdown();
    MorTables.append(base, data);
    log("data: %d files x %d rows in %.1f s", files, rowsPerFile, seconds(start));

    for (int percent : new int[] {1, 5, 20}) {
      Table table = withData(warehouse, String.format(Locale.ROOT, "pos%02d", percent), 2, data);
      MorTables.deletePositions(table, data, positions(data, percent / 100.0, percent));
      log("pos%02d done at %.1f s", percent, seconds(start));
    }

    Table dv = withData(warehouse, "dv05", 3, data);
    MorTables.deletePositions(dv, data, positions(data, 0.05, 5));
    log("dv05 done at %.1f s", seconds(start));

    long totalRows = files * rowsPerFile;
    for (long keys : new long[] {10_000, 1_000_000, 10_000_000}) {
      String name = keys >= 1_000_000 ? "eq" + keys / 1_000_000 + "m" : "eq" + keys / 1000 + "k";
      Table table = withData(warehouse, name, 2, data);
      MorTables.deleteEqual(table, "id", randomIds(keys, totalRows, keys));
      log("%s done at %.1f s", name, seconds(start));
    }

    Table mixed = withData(warehouse, "mixed", 2, data);
    MorTables.deletePositions(mixed, data, positions(data, 0.05, 5));
    MorTables.deleteEqual(mixed, "id", randomIds(1_000_000, totalRows, 1_000_000));
    log("mixed done at %.1f s", seconds(start));
  }

  private static Table withData(String warehouse, String name, int version, List<DataFile> data) {
    Table table = MorTables.create(warehouse + "/" + name, MorTables.SCHEMA, version);
    MorTables.append(table, data);
    return table;
  }

  private static List<long[]> positions(List<DataFile> data, double fraction, long seed) {
    List<long[]> positions = Lists.newArrayList();
    for (int f = 0; f < data.size(); f++) {
      SplittableRandom random = new SplittableRandom(seed * 7919 + f);
      positions.add(
          LongStream.range(0, data.get(f).recordCount())
              .filter(i -> random.nextDouble() < fraction)
              .toArray());
    }
    return positions;
  }

  private static List<Object> randomIds(long count, long totalRows, long seed) {
    SplittableRandom random = new SplittableRandom(seed);
    long[] ids = new long[(int) count];
    for (int i = 0; i < count; i++) {
      ids[i] = random.nextLong(totalRows);
    }
    Arrays.sort(ids);
    List<Object> keys = Lists.newArrayListWithCapacity(ids.length);
    for (long id : ids) {
      keys.add(id);
    }
    return keys;
  }

  private static double seconds(long start) {
    return (System.nanoTime() - start) / 1e9;
  }

  private static void log(String format, Object... args) {
    System.out.println(String.format(Locale.ROOT, format, args));
  }
}
