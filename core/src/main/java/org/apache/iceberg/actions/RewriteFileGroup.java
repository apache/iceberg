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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Container class representing a set of files to be rewritten by a RewriteAction and the new files
 * which have been written by the action.
 */
public class RewriteFileGroup {
  private final FileGroupInfo info;
  private final List<FileScanTask> fileScanTasks;

  private Set<DataFile> addedFiles = Collections.emptySet();

  public RewriteFileGroup(FileGroupInfo info, List<FileScanTask> fileScanTasks) {
    this.info = info;
    this.fileScanTasks = fileScanTasks;
  }

  public FileGroupInfo info() {
    return info;
  }

  public List<FileScanTask> fileScans() {
    return fileScanTasks;
  }

  public void setOutputFiles(Set<DataFile> files) {
    addedFiles = files;
  }

  public Set<DataFile> rewrittenFiles() {
    return fileScans().stream().map(FileScanTask::file).collect(Collectors.toSet());
  }

  public Set<DataFile> addedFiles() {
    return addedFiles;
  }

  public RewriteDataFiles.FileGroupRewriteResult asResult() {
    Preconditions.checkState(addedFiles != null, "Cannot get result, Group was never rewritten");
    return ImmutableRewriteDataFiles.FileGroupRewriteResult.builder()
        .info(info)
        .addedDataFilesCount(addedFiles.size())
        .rewrittenDataFilesCount(fileScanTasks.size())
        .rewrittenBytesCount(sizeInBytes())
        .build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("info", info)
        .add("numRewrittenFiles", fileScanTasks.size())
        .add(
            "numAddedFiles",
            addedFiles == null ? "Rewrite Incomplete" : Integer.toString(addedFiles.size()))
        .add("numRewrittenBytes", sizeInBytes())
        .toString();
  }

  public long sizeInBytes() {
    return fileScanTasks.stream().mapToLong(FileScanTask::length).sum();
  }

  public int numFilesWithDeletes() {
    return fileScanTasks.stream().mapToInt(FileScanTask::filesCount).sum();
  }

  public int numFiles() {
    return fileScanTasks.size();
  }

  public int numDeletes() {
    return numFilesWithDeletes() - numFiles();
  }

  public static Comparator<RewriteFileGroup> comparator(RewriteJobOrder rewriteJobOrder) {
    switch (rewriteJobOrder) {
      case BYTES_ASC:
        return Comparator.comparing(RewriteFileGroup::sizeInBytes);
      case BYTES_DESC:
        return Comparator.comparing(RewriteFileGroup::sizeInBytes, Comparator.reverseOrder());
      case FILES_ASC:
        return Comparator.comparing(RewriteFileGroup::numFiles);
      case FILES_DESC:
        return Comparator.comparing(RewriteFileGroup::numFiles, Comparator.reverseOrder());
      case DELETES_DESC:
        return Comparator.comparing(RewriteFileGroup::numFilesWithDeletes, Comparator.reverseOrder());
      default:
        return (fileGroupOne, fileGroupTwo) -> 0;
    }
  }
}
