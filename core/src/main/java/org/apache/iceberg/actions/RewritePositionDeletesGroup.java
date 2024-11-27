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

/**
 * Container class representing a set of position delete files to be rewritten by a {@link
 * RewritePositionDeleteFiles} and the new files which have been written by the action.
 */
public class RewritePositionDeletesGroup
    extends FileRewriteGroup<FileGroupInfo, PositionDeletesScanTask, DeleteFile> {
  private final long maxRewrittenDataSequenceNumber;

  private DeleteFileSet addedDeleteFiles = DeleteFileSet.create();

  /**
   * @deprecated since 1.8.0, will be removed in 1.9.0.
   */
  @Deprecated
  public RewritePositionDeletesGroup(FileGroupInfo info, List<PositionDeletesScanTask> tasks) {
    this(info, tasks, 0L, 0);
  }

  public RewritePositionDeletesGroup(
      FileGroupInfo info,
      List<PositionDeletesScanTask> tasks,
      long splitSize,
      int expectedOutputFiles) {
    super(info, tasks, splitSize, expectedOutputFiles);
    Preconditions.checkArgument(!tasks.isEmpty(), "Tasks must not be empty");
    this.maxRewrittenDataSequenceNumber =
        tasks.stream().mapToLong(t -> t.file().dataSequenceNumber()).max().getAsLong();
  }

  /**
   * @deprecated since 1.8.0, will be removed in 1.9.0. Use {@link #fileScans()} instead.
   */
  @Deprecated
  public List<PositionDeletesScanTask> tasks() {
    return fileScans();
  }

  public void setOutputFiles(Set<DeleteFile> files) {
    addedDeleteFiles = DeleteFileSet.of(files);
  }

  public long maxRewrittenDataSequenceNumber() {
    return maxRewrittenDataSequenceNumber;
  }

  public Set<DeleteFile> rewrittenDeleteFiles() {
    return fileScans().stream()
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
        .rewrittenDeleteFilesCount(fileScans().size())
        .rewrittenBytesCount(rewrittenBytes())
        .addedBytesCount(addedBytes())
        .build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("info", info())
        .add("numRewrittenPositionDeleteFiles", fileScans().size())
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
    return fileScans().stream().mapToLong(PositionDeletesScanTask::length).sum();
  }

  public long addedBytes() {
    return addedDeleteFiles.stream().mapToLong(DeleteFile::fileSizeInBytes).sum();
  }

  /**
   * @deprecated since 1.8.0, will be removed in 1.9.0. Use {@link #numInputFiles()} instead.
   */
  @Deprecated
  public int numRewrittenDeleteFiles() {
    return fileScans().size();
  }

  /**
   * @deprecated since 1.8.0, will be removed in 1.9.0. Use {@link
   *     FileRewriteGroup#taskComparator(RewriteJobOrder)} instead.
   */
  @Deprecated
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
        return (unused, unused2) -> 0;
    }
  }
}
