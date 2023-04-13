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
package org.apache.iceberg.actions;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Container class representing a set of position delete files to be rewritten by a RewriteAction
 * and the new files which have been written by the action.
 */
public class RewritePositionDeleteGroup {
  private final RewritePositionDeleteFiles.PositionDeleteGroupInfo info;
  private final List<PositionDeletesScanTask> positionDeletesScanTasks;

  private Set<DeleteFile> addedDeleteFiles = Collections.emptySet();

  public RewritePositionDeleteGroup(
      RewritePositionDeleteFiles.PositionDeleteGroupInfo info,
      List<PositionDeletesScanTask> fileScanTasks) {
    this.info = info;
    this.positionDeletesScanTasks = fileScanTasks;
  }

  public RewritePositionDeleteFiles.PositionDeleteGroupInfo info() {
    return info;
  }

  public List<PositionDeletesScanTask> scans() {
    return positionDeletesScanTasks;
  }

  public void setOutputFiles(Set<DeleteFile> files) {
    addedDeleteFiles = files;
  }

  public Set<DeleteFile> rewrittenDeleteFiles() {
    return scans().stream().map(PositionDeletesScanTask::file).collect(Collectors.toSet());
  }

  public Set<DeleteFile> addedDeleteFiles() {
    return addedDeleteFiles;
  }

  public RewritePositionDeleteFiles.PositionDeleteGroupRewriteResult asResult() {
    Preconditions.checkState(
        addedDeleteFiles != null, "Cannot get result, Group was never rewritten");
    long addedBytes = addedDeleteFiles.stream().mapToLong(DeleteFile::fileSizeInBytes).sum();

    return ImmutablePositionDeleteGroupRewriteResult.builder()
        .info(info)
        .addedDeleteFilesCount(addedDeleteFiles.size())
        .rewrittenDeleteFilesCount(positionDeletesScanTasks.size())
        .rewrittenBytesCount(rewrittenBytes())
        .addedBytesCount(addedBytes())
        .build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("info", info)
        .add("numRewrittenPositionDeleteFiles", positionDeletesScanTasks.size())
        .add(
            "numAddedPositionDeleteFiles",
            addedDeleteFiles == null
                ? "Rewrite Incomplete"
                : Integer.toString(addedDeleteFiles.size()))
        .add("numRewrittenBytes", rewrittenBytes())
        .toString();
  }

  public long rewrittenBytes() {
    return positionDeletesScanTasks.stream().mapToLong(PositionDeletesScanTask::length).sum();
  }

  public long addedBytes() {
    return addedDeleteFiles.stream().mapToLong(DeleteFile::fileSizeInBytes).sum();
  }

  public int numDeleteFiles() {
    return positionDeletesScanTasks.size();
  }
}
