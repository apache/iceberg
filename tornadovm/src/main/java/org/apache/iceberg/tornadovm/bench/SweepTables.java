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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.SplittableRandom;
import java.util.stream.LongStream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Adds the points of a delete-fraction sweep next to the tables of {@link GenerateTables}, over the
 * same data files as its {@code none} table: position deletes of 0.1% and 50% of every file ({@code
 * pos001}, {@code pos50}), deletion vectors of 1%, 20% and 50% ({@code dv01}, {@code dv20}, {@code
 * dv50}), and equality deletes of 1K and 30M keys ({@code eq1k}, {@code eq30m}).
 *
 * <pre>
 *   SweepTables &lt;warehouse&gt;
 * </pre>
 */
public final class SweepTables {
  private SweepTables() {}

  public static void main(String[] args) throws IOException {
    String warehouse = new File(args[0]).getAbsolutePath();
    Table base = MorTables.load(warehouse + "/none");
    List<DataFile> data = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = base.newScan().planFiles()) {
      tasks.forEach(task -> data.add(task.file()));
    }
    data.sort((a, b) -> a.location().compareTo(b.location()));
    long totalRows = data.stream().mapToLong(DataFile::recordCount).sum();

    for (Object[] spec :
        new Object[][] {
          {"pos001", 2, 0.001},
          {"pos50", 2, 0.50},
          {"dv01", 3, 0.01},
          {"dv20", 3, 0.20},
          {"dv50", 3, 0.50}
        }) {
      String name = (String) spec[0];
      Table table = withData(warehouse, name, (Integer) spec[1], data);
      MorTables.deletePositions(table, data, positions(data, (Double) spec[2], name.hashCode()));
      log("%s done", name);
    }
    for (Object[] spec : new Object[][] {{"eq1k", 1_000L}, {"eq30m", 30_000_000L}}) {
      String name = (String) spec[0];
      long keys = (Long) spec[1];
      Table table = withData(warehouse, name, 2, data);
      MorTables.deleteEqual(table, "id", randomIds(keys, totalRows, keys));
      log("%s done", name);
    }
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

  private static void log(String format, Object... args) {
    System.out.println(String.format(Locale.ROOT, format, args));
  }
}
