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

package org.apache.iceberg.flink.sink.compact;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.filesystem.stream.TaskTracker;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.compact.SmallFilesMessage.CommonControllerMessage;
import org.apache.iceberg.flink.sink.compact.SmallFilesMessage.CompactCommitInfo;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactFileCommitter extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<CommonControllerMessage, Void>, BoundedOneInput {
  private static final Logger LOG = LoggerFactory.getLogger(CompactFileCommitter.class);

  private final TableLoader tableLoader;
  private transient Table table;
  private transient TaskTracker taskTracker;

  private final List<DataFile> deletedDataFiles = Lists.newArrayList();
  private final List<DataFile> addedDataFiles = Lists.newArrayList();

  public CompactFileCommitter(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    // Open the table loader and load the table.
    this.tableLoader.open();
    this.table = tableLoader.loadTable();
  }

  @Override
  public void processElement(StreamRecord<CommonControllerMessage> element) throws Exception {
    CommonControllerMessage value = element.getValue();
    if (value instanceof CompactCommitInfo) {
      CompactCommitInfo message = (CompactCommitInfo) value;
      if (taskTracker == null) {
        taskTracker = new TaskTracker(message.getNumberOfTasks());
      }
      LOG.info(
          "Receive a CompactCommitInfo (checkpoint {}, total delete {} files: {}, total add {} files: {})",
          message.getCheckpointId(),
          message.getCurrentDataFiles().size(),
          Joiner.on(", ").join(message.getCurrentDataFiles().stream()
              .map(s -> s.content() + "=>" + s.fileSizeInBytes() / 1024 / 1024 + "M")
              .collect(Collectors.toList())),
          message.getDataFiles().size(),
          Joiner.on(", ").join(message.getDataFiles().stream()
              .map(s -> s.content() + "=>" + s.fileSizeInBytes() / 1024 / 1024 + "M")
              .collect(Collectors.toList())));

      this.deletedDataFiles.addAll(message.getCurrentDataFiles());
      this.addedDataFiles.addAll(message.getDataFiles());

      boolean needCommit = taskTracker.add(message.getCheckpointId(), message.getTaskId());
      if (needCommit) {
        LOG.info(
            "Need to commit: checkpoint {}, total delete {} files: {}, total add {} files: {}]",
            message.getCheckpointId(),
            this.deletedDataFiles.size(),
            Joiner.on(", ").join(this.deletedDataFiles.stream()
                .map(s -> s.content() + "=>" + s.fileSizeInBytes() / 1024 / 1024 + "M").collect(Collectors.toList())),
            this.addedDataFiles.size(),
            Joiner.on(", ").join(this.addedDataFiles.stream()
                .map(s -> s.content() + "=>" + s.fileSizeInBytes() / 1024 / 1024 + "M").collect(Collectors.toList())));
        if (!this.addedDataFiles.isEmpty()) {
          replaceDataFiles(this.deletedDataFiles, this.addedDataFiles, message.getStartingSnapshotId());
          LOG.info("Commit compaction information successfully, checkpoint {} ", message.getCheckpointId());
        } else {
          LOG.warn("addedDataFiles is null, abort commit transaction");
        }
        this.deletedDataFiles.clear();
        this.addedDataFiles.clear();
      }
    }
  }

  private void replaceDataFiles(
      Iterable<DataFile> currentDataFiles, Iterable<DataFile> dataFiles,
      long startingSnapshotId) {
    try {
      RewriteFiles rewriteFiles = table.newRewrite();
      rewriteFiles.validateFromSnapshot(startingSnapshotId)
          .rewriteFiles(Sets.newHashSet(currentDataFiles), Sets.newHashSet(dataFiles));
      rewriteFiles.commit();
      table.updateProperties()
          .set(TableProperties.WRITE_FLINK_COMPACT_LAST_REWRITE_MS, Long.toString(System.currentTimeMillis()))
          .commit();
      LOG.info("Compaction transaction has been committed successfully, total delete files: {}, total add files: {}",
          currentDataFiles, dataFiles);
    } catch (Exception e) {
      Tasks.foreach(Iterables.transform(dataFiles, f -> f.path().toString()))
          .noRetry()
          .suppressFailureWhenFinished()
          .onFailure((location, exc) -> LOG.warn("Failed to delete: {}", location, exc))
          .run(table.io()::deleteFile);
      throw e;
    }
  }

  /**
   * It is notified that no more data will arrive on the input.
   */
  @Override
  public void endInput() throws Exception {
  }

  @Override
  public void dispose() throws Exception {
    if (tableLoader != null) {
      tableLoader.close();
    }
  }
}
