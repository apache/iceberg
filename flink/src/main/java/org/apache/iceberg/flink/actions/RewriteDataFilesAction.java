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

package org.apache.iceberg.flink.actions;

import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BaseRewriteDataFilesAction;
import org.apache.iceberg.flink.source.RowDataRewriter;
import org.apache.iceberg.io.FileIO;

public class RewriteDataFilesAction extends BaseRewriteDataFilesAction<RewriteDataFilesAction> {

  private StreamExecutionEnvironment env;

  public RewriteDataFilesAction(StreamExecutionEnvironment env, Table table) {
    super(table);
    this.env = env;
  }

  @Override
  protected FileIO fileIO() {
    return table().io();
  }

  @Override
  protected List<DataFile> rewriteDataForTasks(List<CombinedScanTask> combinedScanTasks) {
    env.setParallelism(combinedScanTasks.size());
    DataStream<CombinedScanTask> dataStream = env.fromCollection(combinedScanTasks);
    RowDataRewriter rowDataRewriter = new RowDataRewriter(table(), caseSensitive(), fileIO(), encryptionManager());
    List<DataFile> addedDataFiles = rowDataRewriter.rewriteDataForTasks(dataStream);
    return addedDataFiles;
  }

  @Override
  protected RewriteDataFilesAction self() {
    return this;
  }
}
