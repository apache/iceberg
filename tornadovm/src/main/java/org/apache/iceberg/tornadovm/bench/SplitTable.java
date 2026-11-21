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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * Splits a table into {@code n} tables over the same files, for one process a GPU: part {@code i}
 * holds every {@code n}-th data file starting at {@code i} (by path), with the delete files that
 * apply to them. Nothing is copied; the parts' answers add up to the table's.
 *
 * <pre>
 *   SplitTable &lt;warehouse&gt; &lt;n&gt; &lt;table&gt;...     creates &lt;table&gt;_part0 ... &lt;table&gt;_part{n-1}
 * </pre>
 */
public final class SplitTable {
  private SplitTable() {}

  public static void main(String[] args) throws Exception {
    String warehouse = args[0];
    int parts = Integer.parseInt(args[1]);
    for (int t = 2; t < args.length; t++) {
      Table source = MorTables.load(warehouse + "/" + args[t]);
      List<FileScanTask> tasks = Lists.newArrayList();
      try (CloseableIterable<FileScanTask> planned = source.newScan().planFiles()) {
        planned.forEach(tasks::add);
      }
      tasks.sort(Comparator.comparing(task -> task.file().location()));
      for (int part = 0; part < parts; part++) {
        String location = warehouse + "/" + args[t] + "_part" + part;
        if (new File(location, "metadata").isDirectory()) {
          System.out.println(location + " exists");
          continue;
        }
        List<DataFile> data = Lists.newArrayList();
        Map<String, DeleteFile> deletes = new LinkedHashMap<>();
        for (int f = part; f < tasks.size(); f += parts) {
          data.add(tasks.get(f).file());
          tasks.get(f).deletes().forEach(delete -> deletes.putIfAbsent(delete.location(), delete));
        }
        Table copy =
            MorTables.create(location, source.schema(), MorTables.formatVersion(source), Map.of());
        MorTables.append(copy, data);
        if (!deletes.isEmpty()) {
          RowDelta delta = copy.newRowDelta();
          deletes.values().forEach(delta::addDeletes);
          delta.commit();
        }
        Set<String> kinds = Sets.newTreeSet();
        deletes.values().forEach(delete -> kinds.add(delete.content().name()));
        System.out.printf(
            "%s: %d data files, %d delete files %s%n",
            location, data.size(), deletes.size(), kinds);
      }
    }
    System.exit(0);
  }
}
