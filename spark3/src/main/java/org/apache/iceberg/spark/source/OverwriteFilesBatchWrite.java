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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.FileIO;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.write.SupportsWriteFileFilter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class OverwriteFilesBatchWrite extends BaseBatchWrite implements SupportsWriteFileFilter {

  private List<DataFile> overwrittenFiles;
  private final Long readSnapshotId;
  private final Expression conflictDetectionFilter;

  OverwriteFilesBatchWrite(Table table, Broadcast<FileIO> io, Broadcast<EncryptionManager> encryption,
                           CaseInsensitiveStringMap options, String appId, String wapId,
                           Schema writeSchema, StructType dsSchema, List<DataFile> overwrittenFiles) {
    this(table, io, encryption, options, appId, wapId, writeSchema, dsSchema, overwrittenFiles, null, null);
  }

  OverwriteFilesBatchWrite(Table table, Broadcast<FileIO> io, Broadcast<EncryptionManager> encryption,
                           CaseInsensitiveStringMap options, String applicationId, String wapId,
                           Schema writeSchema, StructType dsSchema, List<DataFile> overwrittenFiles,
                           Long readSnapshotId, Expression conflictDetectionFilter) {
    super(table, io, encryption, options, applicationId, wapId, writeSchema, dsSchema);
    this.overwrittenFiles = overwrittenFiles;
    this.readSnapshotId = readSnapshotId;
    this.conflictDetectionFilter = conflictDetectionFilter;
  }

  @Override
  public void filterFiles(Set<String> locations) {
    overwrittenFiles = overwrittenFiles.stream()
        .filter(file -> locations.contains(file.path().toString()))
        .collect(Collectors.toList());
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    OverwriteFiles overwriteFiles = table().newOverwrite();

    int numOverwrittenFiles = overwrittenFiles.size();
    for (DataFile overwrittenFile : overwrittenFiles) {
      overwriteFiles.deleteFile(overwrittenFile);
    }

    int numAddedFiles = 0;
    for (DataFile file : files(messages)) {
      numAddedFiles += 1;
      overwriteFiles.addFile(file);
    }

    if (readSnapshotId != null) {
      overwriteFiles.validateFromSnapshot(readSnapshotId);
    }

    if (conflictDetectionFilter != null) {
      overwriteFiles.validateNoConflictingAppends(conflictDetectionFilter);
    }

    String commitMsg = String.format(
        "overwrite of %d data files with %d new data files, readSnapshotId: %d, conflictDetectionFilter: %s",
        numOverwrittenFiles, numAddedFiles, readSnapshotId, conflictDetectionFilter);
    commitOperation(overwriteFiles, commitMsg);
  }
}
