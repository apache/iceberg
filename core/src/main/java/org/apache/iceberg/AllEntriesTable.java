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

import org.apache.iceberg.ManifestEntriesTable.ManifestReadTask;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.StructType;

/**
 * A {@link Table} implementation that exposes a table's manifest entries as rows, for both delete and data files.
 * <p>
 * WARNING: this table exposes internal details, like files that have been deleted. For a table of the live data files,
 * use {@link DataFilesTable}.
 */
public class AllEntriesTable extends BaseMetadataTable {

  AllEntriesTable(TableOperations ops, Table table) {
    this(ops, table, table.name() + ".all_entries");
  }

  AllEntriesTable(TableOperations ops, Table table, String name) {
    super(ops, table, name);
  }

  @Override
  public TableScan newScan() {
    return new Scan(operations(), table(), schema());
  }

  @Override
  public Schema schema() {
    StructType partitionType = Partitioning.partitionType(table());
    Schema schema = ManifestEntry.getSchema(partitionType);
    if (partitionType.fields().size() < 1) {
      // avoid returning an empty struct, which is not always supported. instead, drop the partition field (id 102)
      return TypeUtil.selectNot(schema, Sets.newHashSet(102));
    } else {
      return schema;
    }
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.ALL_ENTRIES;
  }

  private static class Scan extends BaseAllMetadataTableScan {

    Scan(TableOperations ops, Table table, Schema schema) {
      super(ops, table, schema, MetadataTableType.ALL_ENTRIES);
    }

    private Scan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
      super(ops, table, schema, MetadataTableType.ALL_ENTRIES, context);
    }

    @Override
    protected TableScan newRefinedScan(TableOperations ops, Table table, Schema schema,
                                       TableScanContext context) {
      return new Scan(ops, table, schema, context);
    }

    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
      CloseableIterable<ManifestFile> manifests =
          reachableManifests(snapshot -> snapshot.allManifests(tableOps().io()));

      String schemaString = SchemaParser.toJson(schema());
      String specString = PartitionSpecParser.toJson(PartitionSpec.unpartitioned());
      Expression filter = shouldIgnoreResiduals() ? Expressions.alwaysTrue() : filter();
      ResidualEvaluator residuals = ResidualEvaluator.unpartitioned(filter);

      return CloseableIterable.transform(manifests, manifest ->
          new ManifestReadTask(table(), manifest, schema(), schemaString, specString, residuals));
    }
  }
}
