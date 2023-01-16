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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
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
    return new BaseFileGroupRewriteResult(info, addedFiles.size(), fileScanTasks.size());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("info", info)
        .add("numRewrittenFiles", fileScanTasks.size())
        .add(
            "numAddedFiles",
            addedFiles == null ? "Rewrite Incomplete" : Integer.toString(addedFiles.size()))
        .toString();
  }

  public long sizeInBytes() {
    return fileScanTasks.stream().mapToLong(FileScanTask::length).sum();
  }

  public int numFiles() {
    return fileScanTasks.size();
  }
}
