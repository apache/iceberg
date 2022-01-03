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

package org.apache.iceberg.flink.sink;

import java.util.Map;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergRewriteFilesCommitter extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<RewriteResult, Void>, BoundedOneInput {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(IcebergRewriteFilesCommitter.class);

  private final TableLoader tableLoader;

  private transient Table table;
  private transient TableOperations ops;

  private final Map<Long, RewriteResult.Builder> rewriteResults = Maps.newLinkedHashMap();

  IcebergRewriteFilesCommitter(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  @Override
  public void open() throws Exception {
    this.tableLoader.open();
    this.table = tableLoader.loadTable();
    this.ops = ((HasTableOperations) table).operations();
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    long checkpointId = context.getCheckpointId();
    LOG.info("Start to commit rewrite results, table: {}, checkpointId: {}", table, checkpointId);

    commitRewriteResults();
  }

  @Override
  public void processElement(StreamRecord<RewriteResult> record) throws Exception {
    RewriteResult result = record.getValue();

    long snapshotId = result.startingSnapshotId();
    RewriteResult.Builder collector = rewriteResults.getOrDefault(snapshotId, RewriteResult.builder(snapshotId));

    collector.partitions(result.partitions())
        .addAddedDataFiles(result.addedDataFiles())
        .addRewrittenDataFiles(result.rewrittenDataFiles());

    rewriteResults.putIfAbsent(snapshotId, collector);
  }

  private void commitRewriteResults() {
    // Refresh the table to get the committed snapshot of rewrite results.
    table.refresh();

    for (RewriteResult.Builder builder : rewriteResults.values()) {
      commitRewriteResult(builder.build());
    }
    rewriteResults.clear();
  }

  private void commitRewriteResult(RewriteResult result) {
    LOG.info("Committing rewrite file groups of table {}: {}.", table, result);

    long start = System.currentTimeMillis();
    try {
      long sequenceNumber = table.snapshot(result.startingSnapshotId()).sequenceNumber();
      RewriteFiles rewriteFiles = table.newRewrite()
          .validateFromSnapshot(result.startingSnapshotId())
          .rewriteFiles(result.rewrittenDataFiles(), result.addedDataFiles(), sequenceNumber);
      rewriteFiles.commit();
      LOG.info("Committed rewrite file groups in {} ms.", System.currentTimeMillis() - start);
    } catch (Exception e) {
      LOG.error("Cannot commit rewrite file groups, attempting to clean up written files.", e);

      Tasks.foreach(Iterables.transform(result.addedDataFiles(), f -> f.path().toString()))
          .noRetry()
          .suppressFailureWhenFinished()
          .onFailure((location, exc) -> LOG.warn("Failed to delete: {}", location, exc))
          .run(ops.io()::deleteFile);
    }
  }

  @Override
  public void endInput() throws Exception {
    commitRewriteResults();
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (tableLoader != null) {
      tableLoader.close();
    }
  }
}
