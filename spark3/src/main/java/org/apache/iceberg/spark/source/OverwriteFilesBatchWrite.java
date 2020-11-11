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

package org.apache.iceberg.spark.source;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import static org.apache.iceberg.IsolationLevel.SERIALIZABLE;
import static org.apache.iceberg.IsolationLevel.SNAPSHOT;

class OverwriteFilesBatchWrite extends BaseBatchWrite {

  private final SparkBatchScan scan;
  private final IsolationLevel isolationLevel;

  OverwriteFilesBatchWrite(Table table, Broadcast<FileIO> io, Broadcast<EncryptionManager> encryption,
                           CaseInsensitiveStringMap options, String applicationId, String wapId,
                           Schema writeSchema, StructType dsSchema, SparkBatchScan scan,
                           IsolationLevel isolationLevel) {
    super(table, io, encryption, options, applicationId, wapId, writeSchema, dsSchema);
    this.scan = scan;
    this.isolationLevel = isolationLevel;
  }

  private List<DataFile> overwrittenFiles() {
    return scan.files().stream().map(FileScanTask::file).collect(Collectors.toList());
  }

  private Expression conflictDetectionFilter() {
    List<Expression> scanFilterExpressions = scan.filterExpressions();

    Expression filter = Expressions.alwaysTrue();

    if (scanFilterExpressions == null) {
      return filter;
    }

    for (Expression expr : scanFilterExpressions) {
      filter = Expressions.and(filter, expr);
    }

    return filter;
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    OverwriteFiles overwriteFiles = table().newOverwrite();

    List<DataFile> overwrittenFiles = overwrittenFiles();
    int numOverwrittenFiles = overwrittenFiles.size();
    for (DataFile overwrittenFile : overwrittenFiles) {
      overwriteFiles.deleteFile(overwrittenFile);
    }

    int numAddedFiles = 0;
    for (DataFile file : files(messages)) {
      numAddedFiles += 1;
      overwriteFiles.addFile(file);
    }

    if (isolationLevel == SERIALIZABLE) {
      Long scanSnapshotId = scan.snapshotId();
      if (scanSnapshotId != null) {
        overwriteFiles.validateFromSnapshot(scanSnapshotId);
      }

      Expression conflictDetectionFilter = conflictDetectionFilter();
      overwriteFiles.validateNoConflictingAppends(conflictDetectionFilter);

      String commitMsg = String.format(
          "overwrite of %d data files with %d new data files, scanSnapshotId: %d, conflictDetectionFilter: %s",
          numOverwrittenFiles, numAddedFiles, scanSnapshotId, conflictDetectionFilter);
      commitOperation(overwriteFiles, commitMsg);
    } else if (isolationLevel == SNAPSHOT) {
      String commitMsg = String.format(
          "overwrite of %d data files with %d new data files",
          numOverwrittenFiles, numAddedFiles);
      commitOperation(overwriteFiles, commitMsg);
    } else {
      throw new IllegalArgumentException("Unsupported isolation level: " + isolationLevel);
    }
  }
}
