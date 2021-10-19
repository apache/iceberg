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

import org.apache.flink.runtime.state.StateInitializationContext;
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
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
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

  IcebergRewriteFilesCommitter(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    // Open the table loader and load the table.
    this.tableLoader.open();
    this.table = tableLoader.loadTable();
    this.ops = ((HasTableOperations) table).operations();
  }

  @Override
  public void processElement(StreamRecord<RewriteResult> record) throws Exception {
    commit(record.getValue());  // TODO async
  }

  private void commit(RewriteResult result) {
    try {
      RewriteFiles rewriteFiles = table.newRewrite()
          .validateFromSnapshot(result.startingSnapshotId())
          .rewriteFiles(Sets.newHashSet(result.deletedDataFiles()), Sets.newHashSet(result.addedDataFiles()));
      rewriteFiles.commit();
    } catch (Exception e) {
      LOG.error("Cannot commit file group {} and attempting to clean up written files", result, e);

      Tasks.foreach(Iterables.transform(result.addedDataFiles(), f -> f.path().toString()))
          .noRetry()
          .suppressFailureWhenFinished()
          .onFailure((location, exc) -> LOG.warn("Failed to delete: {}", location, exc))
          .run(ops.io()::deleteFile);
    }
  }

  @Override
  public void endInput() throws Exception {

  }
}
