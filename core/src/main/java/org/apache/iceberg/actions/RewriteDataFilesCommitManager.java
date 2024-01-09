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

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Functionality used by RewriteDataFile Actions from different platforms to handle commits. */
public class RewriteDataFilesCommitManager {
  private static final Logger LOG = LoggerFactory.getLogger(RewriteDataFilesCommitManager.class);

  private final Table table;
  private final long startingSnapshotId;
  private final boolean useStartingSequenceNumber;
  private final Map<String, String> extraCommitSummary;

  // constructor used for testing
  public RewriteDataFilesCommitManager(Table table) {
    this(table, table.currentSnapshot().snapshotId());
  }

  public RewriteDataFilesCommitManager(Table table, long startingSnapshotId) {
    this(table, startingSnapshotId, RewriteDataFiles.USE_STARTING_SEQUENCE_NUMBER_DEFAULT);
  }

  public RewriteDataFilesCommitManager(
      Table table, long startingSnapshotId, boolean useStartingSequenceNumber) {
    this(table, startingSnapshotId, useStartingSequenceNumber, ImmutableMap.of());
  }

  public RewriteDataFilesCommitManager(
      Table table,
      long startingSnapshotId,
      boolean useStartingSequenceNumber,
      Map<String, String> extraCommitSummary) {
    this.table = table;
    this.startingSnapshotId = startingSnapshotId;
    this.useStartingSequenceNumber = useStartingSequenceNumber;
    this.extraCommitSummary = extraCommitSummary;
  }

  /**
   * Perform a commit operation on the table adding and removing files as required for this set of
   * file groups
   *
   * @param fileGroups fileSets to commit
   */
  public void commitFileGroups(Set<RewriteFileGroup> fileGroups) {
    Set<DataFile> rewrittenDataFiles = Sets.newHashSet();
    Set<DataFile> addedDataFiles = Sets.newHashSet();
    for (RewriteFileGroup group : fileGroups) {
      rewrittenDataFiles.addAll(group.rewrittenFiles());
      addedDataFiles.addAll(group.addedFiles());
    }

    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshotId);
    if (useStartingSequenceNumber) {
      long sequenceNumber = table.snapshot(startingSnapshotId).sequenceNumber();
      rewrite.rewriteFiles(rewrittenDataFiles, addedDataFiles, sequenceNumber);
    } else {
      rewrite.rewriteFiles(rewrittenDataFiles, addedDataFiles);
    }
    if (!extraCommitSummary.isEmpty()) {
      extraCommitSummary.forEach(rewrite::set);
    }
    rewrite.commit();
  }

  /**
   * Clean up a specified file set by removing any files created for that operation, should not
   * throw any exceptions
   *
   * @param fileGroup group of files which has already been rewritten
   */
  public void abortFileGroup(RewriteFileGroup fileGroup) {
    Preconditions.checkState(
        fileGroup.addedFiles() != null, "Cannot abort a fileGroup that was not rewritten");

    Tasks.foreach(fileGroup.addedFiles())
        .noRetry()
        .suppressFailureWhenFinished()
        .onFailure((dataFile, exc) -> LOG.warn("Failed to delete: {}", dataFile.path(), exc))
        .run(dataFile -> table.io().deleteFile(dataFile.path().toString()));
  }

  public void commitOrClean(Set<RewriteFileGroup> rewriteGroups) {
    try {
      commitFileGroups(rewriteGroups);
    } catch (CommitStateUnknownException e) {
      LOG.error(
          "Commit state unknown for {}, cannot clean up files because they may have been committed successfully.",
          rewriteGroups,
          e);
      throw e;
    } catch (Exception e) {
      LOG.error("Cannot commit groups {}, attempting to clean up written files", rewriteGroups, e);
      rewriteGroups.forEach(this::abortFileGroup);
      throw e;
    }
  }

  /**
   * An async service which allows for committing multiple file groups as their rewrites complete.
   * The service also allows for partial-progress since commits can fail. Once the service has been
   * closed no new file groups should not be offered.
   *
   * @param rewritesPerCommit number of file groups to include in a commit
   * @return the service for handling commits
   */
  public CommitService service(int rewritesPerCommit) {
    return new CommitService(rewritesPerCommit);
  }

  public class CommitService extends BaseCommitService<RewriteFileGroup> {

    CommitService(int rewritesPerCommit) {
      super(table, rewritesPerCommit);
    }

    @Override
    protected void commitOrClean(Set<RewriteFileGroup> batch) {
      RewriteDataFilesCommitManager.this.commitOrClean(batch);
    }

    @Override
    protected void abortFileGroup(RewriteFileGroup group) {
      RewriteDataFilesCommitManager.this.abortFileGroup(group);
    }
  }
}
