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
import java.util.SplittableRandom;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Adds table {@code eq2c1m} to a warehouse {@link GenerateTables} wrote: its data, and 1M equality
 * deletes over (id, k) -- half of them pairs that exist, half with a k the row does not have, which
 * must delete nothing. The rows' k values are recovered by replaying the generator's random
 * streams.
 *
 * <pre>AddCompositeTable &lt;warehouse&gt; &lt;files&gt; &lt;rows a file&gt;</pre>
 */
public final class AddCompositeTable {

  private AddCompositeTable() {}

  public static void main(String[] args) {
    String warehouse = new File(args[0]).getAbsolutePath();
    int files = Integer.parseInt(args[1]);
    long rowsPerFile = Long.parseLong(args[2]);
    long start = System.nanoTime();
    Table base = MorTables.load(warehouse + "/none");
    List<DataFile> data = Lists.newArrayList(base.currentSnapshot().addedDataFiles(base.io()));

    int pairs = 1_000_000;
    SplittableRandom pick = new SplittableRandom(77);
    long[] ids = new long[pairs];
    for (int j = 0; j < pairs; j++) {
      ids[j] = pick.nextLong(files * rowsPerFile);
    }
    Arrays.sort(ids);
    List<Object[]> keys = Lists.newArrayListWithCapacity(pairs);
    int next = 0;
    for (int f = 0; f < files && next < pairs; f++) {
      // The generator's stream for file f: k, then v1..v4, a row.
      SplittableRandom random = new SplittableRandom(1000 + f);
      long first = f * rowsPerFile;
      for (long i = 0; i < rowsPerFile && next < pairs; i++) {
        int kValue = random.nextInt(1000);
        random.nextDouble();
        random.nextDouble();
        random.nextDouble();
        random.nextDouble();
        while (next < pairs && ids[next] == first + i) {
          boolean exists = (next & 1) == 0;
          keys.add(
              new Object[] {ids[next], exists ? kValue : (kValue + 1 + pick.nextInt(998)) % 1000});
          next++;
        }
      }
    }

    Table table = MorTables.create(warehouse + "/eq2c1m", MorTables.SCHEMA, 2);
    MorTables.append(table, data);
    MorTables.deleteEqualRows(table, ImmutableList.of("id", "k"), keys);
    System.out.printf(
        "eq2c1m: %d pairs on %d files in %.1f s%n",
        keys.size(), data.size(), (System.nanoTime() - start) / 1e9);
  }
}
