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
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.actions.RewritePositionDeleteFiles.FileGroupInfo;
import org.apache.iceberg.actions.RewritePositionDeleteFiles.FileGroupRewriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Container class representing a set of position delete files to be rewritten by a {@link
 * RewritePositionDeleteFiles} and the new files which have been written by the action.
 */
public class RewritePositionDeletesGroup {
  private final FileGroupInfo info;
  private final List<PositionDeletesScanTask> tasks;

  private Set<DeleteFile> addedDeleteFiles = Collections.emptySet();

  public RewritePositionDeletesGroup(FileGroupInfo info, List<PositionDeletesScanTask> tasks) {
    this.info = info;
    this.tasks = tasks;
  }

  public FileGroupInfo info() {
    return info;
  }

  public List<PositionDeletesScanTask> tasks() {
    return tasks;
  }

  public void setOutputFiles(Set<DeleteFile> files) {
    addedDeleteFiles = files;
  }

  public Set<DeleteFile> rewrittenDeleteFiles() {
    return tasks().stream().map(PositionDeletesScanTask::file).collect(Collectors.toSet());
  }

  public Set<DeleteFile> addedDeleteFiles() {
    return addedDeleteFiles;
  }

  public FileGroupRewriteResult asResult() {
    Preconditions.checkState(
        addedDeleteFiles != null, "Cannot get result, Group was never rewritten");

    return ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult.builder()
        .info(info)
        .addedDeleteFilesCount(addedDeleteFiles.size())
        .rewrittenDeleteFilesCount(tasks.size())
        .rewrittenBytesCount(rewrittenBytes())
        .addedBytesCount(addedBytes())
        .build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("info", info)
        .add("numRewrittenPositionDeleteFiles", tasks.size())
        .add(
            "numAddedPositionDeleteFiles",
            addedDeleteFiles == null
                ? "Rewrite Incomplete"
                : Integer.toString(addedDeleteFiles.size()))
        .add("numAddedBytes", addedBytes())
        .add("numRewrittenBytes", rewrittenBytes())
        .toString();
  }

  public long rewrittenBytes() {
    return tasks.stream().mapToLong(PositionDeletesScanTask::length).sum();
  }

  public long addedBytes() {
    return addedDeleteFiles.stream().mapToLong(DeleteFile::fileSizeInBytes).sum();
  }

  public int numRewrittenDeleteFiles() {
    return tasks.size();
  }

  public static Comparator<RewritePositionDeletesGroup> comparator(RewriteJobOrder order) {
    switch (order) {
      case BYTES_ASC:
        return Comparator.comparing(RewritePositionDeletesGroup::rewrittenBytes);
      case BYTES_DESC:
        return Comparator.comparing(
            RewritePositionDeletesGroup::rewrittenBytes, Comparator.reverseOrder());
      case FILES_ASC:
        return Comparator.comparing(RewritePositionDeletesGroup::numRewrittenDeleteFiles);
      case FILES_DESC:
        return Comparator.comparing(
            RewritePositionDeletesGroup::numRewrittenDeleteFiles, Comparator.reverseOrder());
      default:
        return (fileGroupOne, fileGroupTwo) -> 0;
    }
  }
}
