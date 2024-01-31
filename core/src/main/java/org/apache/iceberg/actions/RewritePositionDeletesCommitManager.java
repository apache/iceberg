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
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functionality used by {@link RewritePositionDeleteFiles} from different platforms to handle
 * commits.
 */
public class RewritePositionDeletesCommitManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(RewritePositionDeletesCommitManager.class);

  private final Table table;
  private final long startingSnapshotId;
  private final Map<String, String> snapshotProperties;

  public RewritePositionDeletesCommitManager(Table table) {
    this(table, ImmutableMap.of());
  }

  public RewritePositionDeletesCommitManager(Table table, Map<String, String> snapshotProperties) {
    this.table = table;
    this.startingSnapshotId = table.currentSnapshot().snapshotId();
    this.snapshotProperties = snapshotProperties;
  }

  /**
   * Perform a commit operation on the table adding and removing files as required for this set of
   * file groups.
   *
   * @param fileGroups file groups to commit
   */
  public void commit(Set<RewritePositionDeletesGroup> fileGroups) {
    RewriteFiles rewriteFiles = table.newRewrite().validateFromSnapshot(startingSnapshotId);

    for (RewritePositionDeletesGroup group : fileGroups) {
      for (DeleteFile file : group.rewrittenDeleteFiles()) {
        rewriteFiles.deleteFile(file);
      }

      for (DeleteFile file : group.addedDeleteFiles()) {
        rewriteFiles.addFile(file, group.maxRewrittenDataSequenceNumber());
      }
    }

    snapshotProperties.forEach(rewriteFiles::set);

    rewriteFiles.commit();
  }

  /**
   * Clean up a specified file set by removing any files created for that operation, should not
   * throw any exceptions.
   *
   * @param fileGroup group of files which has already been rewritten
   */
  public void abort(RewritePositionDeletesGroup fileGroup) {
    Preconditions.checkState(
        fileGroup.addedDeleteFiles() != null, "Cannot abort a fileGroup that was not rewritten");

    Iterable<String> filePaths =
        Iterables.transform(fileGroup.addedDeleteFiles(), f -> f.path().toString());
    CatalogUtil.deleteFiles(table.io(), filePaths, "position delete", true);
  }

  public void commitOrClean(Set<RewritePositionDeletesGroup> rewriteGroups) {
    try {
      commit(rewriteGroups);
    } catch (CommitStateUnknownException e) {
      LOG.error(
          "Commit state unknown for {}, cannot clean up files because they may have been committed successfully.",
          rewriteGroups,
          e);
      throw e;
    } catch (Exception e) {
      LOG.error("Cannot commit groups {}, attempting to clean up written files", rewriteGroups, e);
      rewriteGroups.forEach(this::abort);
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

  public class CommitService extends BaseCommitService<RewritePositionDeletesGroup> {

    CommitService(int rewritesPerCommit) {
      super(table, rewritesPerCommit);
    }

    @Override
    protected void commitOrClean(Set<RewritePositionDeletesGroup> batch) {
      RewritePositionDeletesCommitManager.this.commitOrClean(batch);
    }

    @Override
    protected void abortFileGroup(RewritePositionDeletesGroup group) {
      RewritePositionDeletesCommitManager.this.abort(group);
    }
  }
}
