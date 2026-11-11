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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

/**
 * Equality deletes of every key {@code k} of an integer column with {@code k % modulus ==
 * remainder}, over the column's range by its files' bounds, written as {@code commits} delete files
 * in as many commits: the deletes a change-data-capture writer (Flink, say) leaves, the same rows a
 * merge-on-read {@code DELETE ... WHERE k % modulus = remainder} removes with position deletes.
 *
 * <pre>
 *   AddKeyEqualityDeletes &lt;table location&gt; &lt;column&gt; &lt;modulus&gt; &lt;remainder&gt; &lt;commits&gt;
 * </pre>
 */
public final class AddKeyEqualityDeletes {
  private AddKeyEqualityDeletes() {}

  public static void main(String[] args) {
    Table table = MorTables.load(args[0]);
    String column = args[1];
    long modulus = Long.parseLong(args[2]);
    long remainder = Long.parseLong(args[3]);
    int commits = Integer.parseInt(args[4]);
    Types.NestedField field = table.schema().findField(column);
    boolean intKey = field.type().typeId() == org.apache.iceberg.types.Type.TypeID.INTEGER;
    long low = Long.MAX_VALUE;
    long high = Long.MIN_VALUE;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().includeColumnStats().planFiles()) {
      for (FileScanTask task : tasks) {
        ByteBuffer min = task.file().lowerBounds().get(field.fieldId());
        ByteBuffer max = task.file().upperBounds().get(field.fieldId());
        low = Math.min(low, ((Number) Conversions.fromByteBuffer(field.type(), min)).longValue());
        high = Math.max(high, ((Number) Conversions.fromByteBuffer(field.type(), max)).longValue());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    long first = low + Math.floorMod(remainder - low, modulus);
    long count = first > high ? 0 : (high - first) / modulus + 1;
    long written = 0;
    for (int c = 0; c < commits; c++) {
      // Commit c deletes a contiguous slice of the keys, as successive change batches would.
      long from = count * c / commits;
      long to = count * (c + 1) / commits;
      List<Object> keys = Lists.newArrayList();
      for (long i = from; i < to; i++) {
        long key = first + i * modulus;
        keys.add(intKey ? (Object) (int) key : (Object) key);
      }
      if (!keys.isEmpty()) {
        MorTables.deleteEqual(table, column, keys);
        written += keys.size();
      }
    }
    System.out.printf(
        "%s: %d equality-deleted keys of %s (%% %d = %d, range %d..%d) in %d commits%n",
        args[0], written, column, modulus, remainder, low, high, commits);
  }
}
