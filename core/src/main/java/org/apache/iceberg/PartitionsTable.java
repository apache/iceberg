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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.StructLikeMap;

/** A {@link Table} implementation that exposes a table's partitions as rows. */
public class PartitionsTable extends BaseMetadataTable {

  private final Schema schema;

  PartitionsTable(Table table) {
    this(table, table.name() + ".partitions");
  }

  PartitionsTable(Table table, String name) {
    super(table, name);

    this.schema =
        new Schema(
            Types.NestedField.required(1, "partition", Partitioning.partitionType(table)),
            Types.NestedField.required(4, "spec_id", Types.IntegerType.get()),
            Types.NestedField.required(
                2, "record_count", Types.LongType.get(), "Count of records in data files"),
            Types.NestedField.required(
                3, "file_count", Types.IntegerType.get(), "Count of data files"));
  }

  @Override
  public TableScan newScan() {
    return new PartitionsScan(table());
  }

  @Override
  public Schema schema() {
    if (table().spec().fields().size() < 1) {
      return schema.select("record_count", "file_count");
    }
    return schema;
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.PARTITIONS;
  }

  private DataTask task(StaticTableScan scan) {
    Iterable<Partition> partitions = partitions(table(), scan);
    if (table().spec().fields().size() < 1) {
      // the table is unpartitioned, partitions contains only the root partition
      return StaticDataTask.of(
          io().newInputFile(table().operations().current().metadataFileLocation()),
          schema(),
          scan.schema(),
          partitions,
          root -> StaticDataTask.Row.of(root.dataRecordCount, root.dataFileCount));
    } else {
      return StaticDataTask.of(
          io().newInputFile(table().operations().current().metadataFileLocation()),
          schema(),
          scan.schema(),
          partitions,
          PartitionsTable::convertPartition);
    }
  }

  private static StaticDataTask.Row convertPartition(Partition partition) {
    return StaticDataTask.Row.of(
        partition.partitionData,
        partition.specId,
        partition.dataRecordCount,
        partition.dataFileCount);
  }

  private static Iterable<Partition> partitions(Table table, StaticTableScan scan) {
    Types.StructType partitionType = Partitioning.partitionType(table);
    PartitionMap partitions = new PartitionMap(partitionType);

    CloseableIterable<DataFile> datafiles = planDataFiles(scan);
    for (DataFile dataFile : datafiles) {
      StructLike partition =
          PartitionUtil.coercePartition(
              partitionType, table.specs().get(dataFile.specId()), dataFile.partition());
      partitions.get(partition).update(dataFile);
    }

    return partitions.all();
  }

  @VisibleForTesting
  static CloseableIterable<DataFile> planDataFiles(StaticTableScan scan) {
    Table table = scan.table();
    Snapshot snapshot = scan.snapshot();

    CloseableIterable<ManifestFile> dataManifests =
        CloseableIterable.withNoopClose(snapshot.dataManifests(table.io()));

    LoadingCache<Integer, ManifestEvaluator> evalCache =
        Caffeine.newBuilder()
            .build(
                specId -> {
                  PartitionSpec spec = table.specs().get(specId);
                  PartitionSpec transformedSpec = transformSpec(scan.tableSchema(), spec);
                  return ManifestEvaluator.forRowFilter(
                      scan.filter(), transformedSpec, scan.isCaseSensitive());
                });

    CloseableIterable<ManifestFile> filteredManifests =
        CloseableIterable.filter(
            dataManifests, manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest));

    Iterable<CloseableIterable<DataFile>> tasks =
        CloseableIterable.transform(
            filteredManifests,
            manifest ->
                ManifestFiles.read(manifest, table.io(), table.specs())
                    .caseSensitive(scan.isCaseSensitive())
                    .select(BaseScan.SCAN_COLUMNS)); // don't select stats columns

    return new ParallelIterable<>(tasks, scan.planExecutor());
  }

  private class PartitionsScan extends StaticTableScan {
    PartitionsScan(Table table) {
      super(
          table,
          PartitionsTable.this.schema(),
          MetadataTableType.PARTITIONS,
          PartitionsTable.this::task);
    }
  }

  static class PartitionMap {
    private final StructLikeMap<Partition> partitions;
    private final Types.StructType keyType;

    PartitionMap(Types.StructType type) {
      this.partitions = StructLikeMap.create(type);
      this.keyType = type;
    }

    Partition get(StructLike key) {
      Partition partition = partitions.get(key);
      if (partition == null) {
        partition = new Partition(key, keyType);
        partitions.put(key, partition);
      }
      return partition;
    }

    Iterable<Partition> all() {
      return partitions.values();
    }
  }

  static class Partition {
    private final PartitionData partitionData;
    private int specId;
    private long dataRecordCount;
    private int dataFileCount;

    Partition(StructLike key, Types.StructType keyType) {
      this.partitionData = toPartitionData(key, keyType);
      this.specId = 0;
      this.dataRecordCount = 0;
      this.dataFileCount = 0;
    }

    void update(DataFile file) {
      this.dataRecordCount += file.recordCount();
      this.dataFileCount += 1;
      this.specId = file.specId();
    }

    /** Needed because StructProjection is not serializable */
    private PartitionData toPartitionData(StructLike key, Types.StructType keyType) {
      PartitionData data = new PartitionData(keyType);
      for (int i = 0; i < keyType.fields().size(); i++) {
        Object val = key.get(i, keyType.fields().get(i).type().typeId().javaClass());
        if (val != null) {
          data.set(i, val);
        }
      }
      return data;
    }
  }
}
