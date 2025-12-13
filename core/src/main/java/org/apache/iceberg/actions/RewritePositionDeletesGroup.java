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
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.ScanTaskUtil;

/**
 * Container class representing a set of position delete files to be rewritten by a {@link
 * RewritePositionDeleteFiles} and the new files which have been written by the action.
 */
public class RewritePositionDeletesGroup
    extends RewriteGroupBase<FileGroupInfo, PositionDeletesScanTask, DeleteFile> {
  private final long maxRewrittenDataSequenceNumber;

  private DeleteFileSet addedDeleteFiles = DeleteFileSet.create();

  public RewritePositionDeletesGroup(
      FileGroupInfo info,
      List<PositionDeletesScanTask> tasks,
      long writeMaxFileSize,
      long inputSplitSize,
      int expectedOutputFiles) {
    super(info, tasks, writeMaxFileSize, inputSplitSize, expectedOutputFiles);
    Preconditions.checkArgument(!tasks.isEmpty(), "Tasks must not be empty");
    this.maxRewrittenDataSequenceNumber =
        tasks.stream().mapToLong(t -> t.file().dataSequenceNumber()).max().getAsLong();
  }

  public void setOutputFiles(Set<DeleteFile> files) {
    addedDeleteFiles = DeleteFileSet.of(files);
  }

  public long maxRewrittenDataSequenceNumber() {
    return maxRewrittenDataSequenceNumber;
  }

  public Set<DeleteFile> rewrittenDeleteFiles() {
    return fileScanTasks().stream()
        .map(PositionDeletesScanTask::file)
        .collect(Collectors.toCollection(DeleteFileSet::create));
  }

  public Set<DeleteFile> addedDeleteFiles() {
    return addedDeleteFiles;
  }

  public FileGroupRewriteResult asResult() {
    Preconditions.checkState(
        addedDeleteFiles != null, "Cannot get result, Group was never rewritten");

    return ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult.builder()
        .info(info())
        .addedDeleteFilesCount(addedDeleteFiles.size())
        .rewrittenDeleteFilesCount(inputFileNum())
        .rewrittenBytesCount(inputFilesSizeInBytes())
        .addedBytesCount(addedBytes())
        .build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("info", info())
        .add("numRewrittenPositionDeleteFiles", fileScanTasks().size())
        .add(
            "numAddedPositionDeleteFiles",
            addedDeleteFiles == null
                ? "Rewrite Incomplete"
                : Integer.toString(addedDeleteFiles.size()))
        .add("numAddedBytes", addedBytes())
        .add("numRewrittenBytes", inputFilesSizeInBytes())
        .add("maxOutputFileSize", maxOutputFileSize())
        .add("inputSplitSize", inputSplitSize())
        .add("expectedOutputFiles", expectedOutputFiles())
        .toString();
  }

  public long addedBytes() {
    return addedDeleteFiles.stream().mapToLong(ScanTaskUtil::contentSizeInBytes).sum();
  }

  public static Comparator<RewritePositionDeletesGroup> comparator(RewriteJobOrder order) {
    switch (order) {
      case BYTES_ASC:
        return Comparator.comparing(RewritePositionDeletesGroup::inputFilesSizeInBytes);
      case BYTES_DESC:
        return Comparator.comparing(
            RewritePositionDeletesGroup::inputFilesSizeInBytes, Comparator.reverseOrder());
      case FILES_ASC:
        return Comparator.comparing(RewritePositionDeletesGroup::inputFileNum);
      case FILES_DESC:
        return Comparator.comparing(
            RewritePositionDeletesGroup::inputFileNum, Comparator.reverseOrder());
      default:
        return (unused, unused2) -> 0;
    }
  }
}
