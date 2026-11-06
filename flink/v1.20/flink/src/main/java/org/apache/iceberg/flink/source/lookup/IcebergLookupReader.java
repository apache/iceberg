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
package org.apache.iceberg.flink.source.lookup;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.FileScanTaskReader;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.SnapshotUtil;

/** Reads all rows of an Iceberg table to build a lookup cache. */
@Internal
class IcebergLookupReader {

  static final long CURRENT_SNAPSHOT = -1L;

  private final Table table;
  private final Schema projectedSchema;
  private final List<Expression> baseFilters;
  private final Expression filter;
  private final boolean caseSensitive;
  private final String nameMapping;

  IcebergLookupReader(
      Table table,
      Schema projectedSchema,
      List<Expression> baseFilters,
      boolean caseSensitive,
      @Nullable String nameMapping) {
    Preconditions.checkNotNull(baseFilters, "Base filters should not be null");

    this.table = table;
    this.projectedSchema = projectedSchema;
    this.baseFilters = baseFilters;
    this.caseSensitive = caseSensitive;
    this.nameMapping = nameMapping;

    Expression combinedFilter = Expressions.alwaysTrue();
    for (Expression baseFilter : this.baseFilters) {
      combinedFilter = Expressions.and(combinedFilter, baseFilter);
    }

    this.filter = combinedFilter;
  }

  CloseableIterable<RowData> read(long snapshotId) {
    Schema tableSchema =
        snapshotId == CURRENT_SNAPSHOT ? table.schema() : SnapshotUtil.schemaFor(table, snapshotId);

    RowDataFileScanTaskReader fileReader =
        new RowDataFileScanTaskReader(
            tableSchema, projectedSchema, nameMapping, caseSensitive, baseFilters);

    TableScan scan =
        table.newScan().caseSensitive(caseSensitive).project(projectedSchema).filter(filter);
    if (snapshotId != CURRENT_SNAPSHOT) {
      scan = scan.useSnapshot(snapshotId);
    }

    return new LookupIterable(scan.planTasks(), fileReader);
  }

  private class LookupIterable extends CloseableGroup implements CloseableIterable<RowData> {
    private final CloseableIterable<CombinedScanTask> tasks;
    private final FileScanTaskReader<RowData> fileReader;

    private LookupIterable(
        CloseableIterable<CombinedScanTask> tasks, FileScanTaskReader<RowData> fileReader) {
      this.tasks = tasks;
      this.fileReader = fileReader;
    }

    @Override
    public CloseableIterator<RowData> iterator() {
      CloseableIterator<RowData> rows =
          CloseableIterable.concat(Iterables.transform(tasks, this::taskRows)).iterator();
      addCloseable(rows);
      return rows;
    }

    @Override
    public void close() throws IOException {
      tasks.close();
      super.close();
    }

    private CloseableIterable<RowData> taskRows(CombinedScanTask task) {
      DataIterator<RowData> rows =
          new DataIterator<>(fileReader, task, table.io(), table.encryption());
      return CloseableIterable.combine(() -> rows, rows);
    }
  }
}
