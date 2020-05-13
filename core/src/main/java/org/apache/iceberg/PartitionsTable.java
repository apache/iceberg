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

package org.apache.iceberg;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;

/**
 * A {@link Table} implementation that exposes a table's partitions as rows.
 */
public class PartitionsTable extends BaseMetadataTable {

  private final TableOperations ops;
  private final Table table;
  private final Schema schema;

  public PartitionsTable(TableOperations ops, Table table) {
    this.ops = ops;
    this.table = table;
    this.schema = new Schema(
        Types.NestedField.required(1, "partition", table.spec().partitionType()),
        Types.NestedField.required(2, "record_count", Types.LongType.get()),
        Types.NestedField.required(3, "file_count", Types.IntegerType.get())
    );
  }

  @Override
  Table table() {
    return table;
  }

  @Override
  String metadataTableName() {
    return "partitions";
  }

  @Override
  public TableScan newScan() {
    return new PartitionsScan();
  }

  @Override
  public Schema schema() {
    return schema;
  }

  private DataTask task(TableScan scan) {
    return StaticDataTask.of(
        ops.current().file(),
        partitions(table, scan.snapshot().snapshotId()),
        PartitionsTable::convertPartition);
  }

  private static StaticDataTask.Row convertPartition(Partition partition) {
    return StaticDataTask.Row.of(partition.key, partition.recordCount, partition.fileCount);
  }

  private static Iterable<Partition> partitions(Table table, Long snapshotId) {
    PartitionSet partitions = new PartitionSet();
    TableScan scan = table.newScan();

    if (snapshotId != null) {
      scan = scan.useSnapshot(snapshotId);
    }

    for (FileScanTask task : scan.planFiles()) {
      partitions.get(task.file().partition()).update(task.file());
    }

    return partitions.all();
  }

  private class PartitionsScan extends StaticTableScan {
    PartitionsScan() {
      super(ops, table, schema, PartitionsTable.this::task);
    }
  }

  static class PartitionSet {
    private final Map<StructLikeWrapper, Partition> partitions = Maps.newHashMap();
    private final StructLikeWrapper reused = StructLikeWrapper.wrap(null);

    Partition get(StructLike key) {
      Partition partition = partitions.get(reused.set(key));
      if (partition == null) {
        partition = new Partition(key);
        partitions.put(StructLikeWrapper.wrap(key), partition);
      }
      return partition;
    }

    Iterable<Partition> all() {
      return partitions.values();
    }
  }

  static class Partition {
    private final StructLike key;
    private long recordCount;
    private int fileCount;

    Partition(StructLike key) {
      this.key = key;
      this.recordCount = 0;
      this.fileCount = 0;
    }

    void update(DataFile file) {
      this.recordCount += file.recordCount();
      this.fileCount += 1;
    }
  }
}
