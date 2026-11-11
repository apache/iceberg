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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;

/**
 * Equality deletes of given keys in a table partitioned by one field, one delete file a partition
 * in one commit, as a change-data-capture writer (Flink, say) writes them at a checkpoint. The keys
 * come as CSV lines {@code partition value,key} (an empty partition value for the null partition),
 * from every file of a directory, as {@code tpcds_partitioned_setup.py} writes them.
 *
 * <pre>
 *   AddPartitionedEqualityDeletes &lt;table location&gt; &lt;key column&gt; &lt;CSV directory&gt;
 * </pre>
 */
public final class AddPartitionedEqualityDeletes {
  private AddPartitionedEqualityDeletes() {}

  public static void main(String[] args) throws IOException {
    Table table = MorTables.load(args[0]);
    String column = args[1];
    Types.NestedField field = table.schema().findField(column);
    boolean intKey = field.type().typeId() == org.apache.iceberg.types.Type.TypeID.INTEGER;
    Map<Long, List<Object>> keys = Maps.newHashMap();
    List<Path> csvs;
    try (Stream<Path> files = Files.list(Paths.get(args[2]))) {
      csvs = files.filter(p -> p.getFileName().toString().endsWith(".csv")).toList();
    }
    long total = 0;
    for (Path csv : csvs) {
      for (String line : Files.readAllLines(csv)) {
        int comma = line.indexOf(',');
        Long partition = comma == 0 ? null : Long.valueOf(line.substring(0, comma));
        long key = Long.parseLong(line.substring(comma + 1));
        keys.computeIfAbsent(partition, p -> Lists.newArrayList())
            .add(intKey ? (Object) (int) key : (Object) key);
        total++;
      }
    }
    Map<Long, StructLike> partitions = Maps.newHashMap();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        Object value = task.file().partition().get(0, Object.class);
        partitions.putIfAbsent(
            value == null ? null : ((Number) value).longValue(), task.file().partition());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    RowDelta delta = table.newRowDelta();
    int written = 0;
    for (Map.Entry<Long, List<Object>> entry : keys.entrySet()) {
      StructLike partition = partitions.get(entry.getKey());
      if (partition == null) {
        throw new IllegalArgumentException("No data file in partition " + entry.getKey());
      }
      DeleteFile file = MorTables.writeEqualityDeletes(table, column, entry.getValue(), partition);
      delta.addDeletes(file);
      written++;
    }
    delta.commit();
    System.out.printf(
        "%s: %d keys of %s in %d equality delete files, one commit%n",
        args[0], total, column, written);
    System.exit(0);
  }
}
