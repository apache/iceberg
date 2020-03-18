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

package org.apache.iceberg.hive.legacy;

import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DataTableScan;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.TableScanContext;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link DataTableScan} which uses Hive table and partition metadata to read tables.
 * This scan does not provide any time travel, snapshot isolation, incremental computation benefits.
 */
public class LegacyHiveTableScan extends DataTableScan {
  private static final Logger LOG = LoggerFactory.getLogger(LegacyHiveTableScan.class);

  protected LegacyHiveTableScan(TableOperations ops, Table table) {
    super(ops, table);
  }

  protected LegacyHiveTableScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    super(ops, table, schema, context);
  }

  @Override
  @SuppressWarnings("checkstyle:HiddenField")
  protected TableScan newRefinedScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    return new LegacyHiveTableScan(ops, table, schema, context);
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    LOG.info("Scanning table {} with filter {}", table().toString(), filter());

    Listeners.notifyAll(
        new ScanEvent(table().toString(), -1, filter(), schema()));

    LegacyHiveTableOperations hiveOps = (LegacyHiveTableOperations) tableOps();
    PartitionSpec spec = hiveOps.current().spec();
    String schemaString = SchemaParser.toJson(spec.schema());
    String specString = PartitionSpecParser.toJson(spec);
    ResidualEvaluator residuals = ResidualEvaluator.of(spec, filter(), isCaseSensitive());

    Iterable<Iterable<FileScanTask>> tasks = Iterables.transform(
        hiveOps.getFilesByFilter(filter()),
        fileIterable ->
            Iterables.transform(
                fileIterable,
                file -> new BaseFileScanTask(file, new DeleteFile[0], schemaString, specString, residuals)));

    return new ParallelIterable<>(tasks, ThreadPools.getWorkerPool());
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles(TableOperations ops, Snapshot snapshot,
      Expression rowFilter, boolean ignoreResiduals,
      boolean caseSensitive, boolean colStats) {
    throw new IllegalStateException("Control flow should never reach here");
  }
}
