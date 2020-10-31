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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BaseRewriteDataFilesAction;
import org.apache.iceberg.flink.source.RowDataRewriter;
import org.apache.iceberg.io.FileIO;

public class RewriteDataFilesAction extends BaseRewriteDataFilesAction<RewriteDataFilesAction> {

  private ExecutionEnvironment env;

  public RewriteDataFilesAction(ExecutionEnvironment env, Table table) {
    super(table);
    this.env = env;
  }

  @Override
  protected FileIO setFileIO() {
    return table().io();
  }

  @Override
  protected List<DataFile> rewriteDataForTasks(List<CombinedScanTask> combinedScanTasks) {
    DataSet<CombinedScanTask> dataSet = env.fromCollection(combinedScanTasks).setParallelism(combinedScanTasks.size());
    RowDataRewriter rowDataRewriter = new RowDataRewriter(table(), caseSensitive(), fileIO(), encryptionManager());
    List<DataFile> addedDataFiles = null;
    try {
      addedDataFiles = rowDataRewriter.rewriteDataForTasks(dataSet);
    } catch (Exception e) {
      throw new RuntimeException("Rewrite Data File error.", e);
    }
    return addedDataFiles;
  }

  @Override
  protected RewriteDataFilesAction self() {
    return this;
  }
}
