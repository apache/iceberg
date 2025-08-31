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
package org.apache.iceberg.util;

import java.util.function.Predicate;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.DuplicateWAPCommitException;

public class WapUtil {

  private WapUtil() {}

  public static String stagedWapId(Snapshot snapshot) {
    return snapshot.summary() != null
        ? snapshot.summary().get(SnapshotSummary.STAGED_WAP_ID_PROP)
        : null;
  }

  public static String publishedWapId(Snapshot snapshot) {
    return snapshot.summary() != null
        ? snapshot.summary().get(SnapshotSummary.PUBLISHED_WAP_ID_PROP)
        : null;
  }

  /**
   * Sets Write-Audit-Publish (WAP) properties on the given {@link SnapshotUpdate} operation. This
   * method is intended to be used by write operations that support WAP, ensuring that staged
   * snapshot is tagged with wapId and wap branches are tag with branch name
   *
   * @param operation the {@link SnapshotUpdate} operation to update with WAP properties
   * @param wapEnabled true if WAP is enabled for this operation
   * @param wapId the WAP ID for staging the commit, or null if not applicable
   * @param branch the branch name for WAP commit, or null if not applicable
   * @param isWapBranch a predicate to determine if a branch is a WAP branch
   */
  public static void setWapProperties(
      SnapshotUpdate<?> operation,
      boolean wapEnabled,
      String wapId,
      String branch,
      Predicate<String> isWapBranch) {
    if (wapEnabled) {
      if (wapId != null) {
        operation.set(SnapshotSummary.STAGED_WAP_ID_PROP, wapId);
        operation.stageOnly();
      } else if (branch != null && isWapBranch != null && isWapBranch.test(branch)) {
        operation.set(SnapshotSummary.WAP_BRANCH_PROP, branch);
      }
    }
  }

  /**
   * Check if a given staged snapshot's associated wap-id was already published. Does not fail for
   * non-WAP workflows.
   *
   * @param current the current {@link TableMetadata metadata} for the target table
   * @param wapSnapshotId a snapshot id which could have been staged and is associated with a wap id
   * @return the WAP ID that will be published, if the snapshot has one
   */
  public static String validateWapPublish(TableMetadata current, long wapSnapshotId) {
    Snapshot cherryPickSnapshot = current.snapshot(wapSnapshotId);
    String wapId = stagedWapId(cherryPickSnapshot);
    if (wapId != null && !wapId.isEmpty()) {
      if (WapUtil.isWapIdPublished(current, wapId)) {
        throw new DuplicateWAPCommitException(wapId);
      }
    }

    return wapId;
  }

  private static boolean isWapIdPublished(TableMetadata current, String wapId) {
    for (long ancestorId : SnapshotUtil.ancestorIds(current.currentSnapshot(), current::snapshot)) {
      Snapshot snapshot = current.snapshot(ancestorId);
      if (wapId.equals(stagedWapId(snapshot)) || wapId.equals(publishedWapId(snapshot))) {
        return true;
      }
    }
    return false;
  }
}
