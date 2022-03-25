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

import java.io.IOException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ParallelIterable;

/**
 * A {@link Table} implementation that exposes a table's valid data files as rows.
 * <p>
 * A valid data file is one that is readable from any snapshot currently tracked by the table.
 * <p>
 * This table may return duplicate rows.
 */
public class AllDataFilesTable extends BaseFilesTable {

  AllDataFilesTable(TableOperations ops, Table table) {
    this(ops, table, table.name() + ".all_data_files");
  }

  AllDataFilesTable(TableOperations ops, Table table, String name) {
    super(ops, table, name);
  }

  @Override
  public TableScan newScan() {
    return new AllDataFilesTableScan(operations(), table(), schema());
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.ALL_DATA_FILES;
  }

  public static class AllDataFilesTableScan extends BaseFilesTableScan {

    AllDataFilesTableScan(TableOperations ops, Table table, Schema fileSchema) {
      super(ops, table, fileSchema, MetadataTableType.ALL_DATA_FILES);
    }

    private AllDataFilesTableScan(TableOperations ops, Table table, Schema schema, Schema fileSchema,
                                  TableScanContext context) {
      super(ops, table, schema, fileSchema, context, MetadataTableType.ALL_DATA_FILES);
    }

    @Override
    public boolean allScan() {
      return true;
    }

    @Override
    protected TableScan newRefinedScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
      return new AllDataFilesTableScan(ops, table, schema, fileSchema(), context);
    }

    @Override
    public TableScan useSnapshot(long scanSnapshotId) {
      throw new UnsupportedOperationException("Cannot select snapshot: all_data_files is for all snapshots");
    }

    @Override
    public TableScan asOfTime(long timestampMillis) {
      throw new UnsupportedOperationException("Cannot select snapshot: all_data_files is for all snapshots");
    }

    @Override
    protected CloseableIterable<ManifestFile> manifests() {
      try (CloseableIterable<ManifestFile> iterable = new ParallelIterable<>(
          Iterables.transform(table().snapshots(),
              snapshot -> (Iterable<ManifestFile>) () -> snapshot.dataManifests().iterator()),
          context().planExecutor())) {
        return CloseableIterable.withNoopClose(Sets.newHashSet(iterable));
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to close parallel iterable");
      }
    }
  }
}
