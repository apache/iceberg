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
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Container class representing a set of files to be rewritten by a RewriteAction and the new files which have been
 * written by the action.
 */
public class RewriteFileGroup {
  private final RewriteDataFiles.FileGroupInfo info;
  private final List<FileScanTask> fileScanTasks;
  private final int numInputFiles;

  private Set<DataFile> outputFiles = Collections.emptySet();
  private int numOutputFiles;

  public RewriteFileGroup(RewriteDataFiles.FileGroupInfo info, List<FileScanTask> fileScanTasks) {
    this.info = info;
    this.fileScanTasks = fileScanTasks;
    this.numInputFiles = fileScanTasks.size();
  }

  public int numInputFiles() {
    return numInputFiles;
  }

  public StructLike partition() {
    return info.partition();
  }

  public Integer globalIndex() {
    return info.globalIndex();
  }

  public Integer partitionIndex() {
    return info.partitionIndex();
  }

  public RewriteDataFiles.FileGroupInfo info() {
    return info;
  }

  public List<FileScanTask> fileScans() {
    return fileScanTasks;
  }

  public void outputFiles(Set<DataFile> files) {
    numOutputFiles = files.size();
    outputFiles = files;
  }

  public List<DataFile> rewrittenFiles() {
    return fileScans().stream().map(FileScanTask::file).collect(Collectors.toList());
  }

  public Set<DataFile> addedFiles() {
    return outputFiles;
  }

  public RewriteDataFiles.FileGroupRewriteResult asResult() {
    Preconditions.checkState(outputFiles != null, "Cannot get result, Group was never rewritten");
    return new BaseRewriteDataFilesFileGroupRewriteResult(numOutputFiles, numInputFiles);
  }
}
