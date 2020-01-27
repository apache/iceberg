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

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CherrypickAncestorCommitException;
import org.apache.iceberg.exceptions.DuplicateWAPCommitException;

public class WapUtil {

  private WapUtil() {
  }

  public static String stagedWapId(Snapshot snapshot) {
    return snapshot.summary() != null ? snapshot.summary().getOrDefault("wap.id", null) : null;
  }

  /**
   * Check if a given staged snapshot's associated wap-id was already published. Does not fail for non-WAP workflows
   * @param table a {@link Table}
   * @param wapSnapshotId a snapshot id which could have been staged and is associated with a wap id
   */
  public static void validateWapPublish(Table table, Long wapSnapshotId) {
    Preconditions.checkArgument(table instanceof HasTableOperations,
        "Cannot validate WAP publish on a table that doesn't expose its TableOperations");
    TableMetadata baseMeta = ((HasTableOperations) table).operations().current();
    Snapshot cherryPickSnapshot = ((HasTableOperations) table).operations()
        .current().snapshot(wapSnapshotId);
    String wapId = stagedWapId(cherryPickSnapshot);
    if (isWapWorkflow(baseMeta, wapSnapshotId)) {
      if (WapUtil.isWapIdPublished(table, wapId)) {
        throw new DuplicateWAPCommitException(wapId);
      }
    }
  }

  /**
   * Ensure that the given snapshot picked to be committed is not already an ancestor. This check applies to non-WAP
   * snapshots too. Also check if the picked snapshot wasn't cherrypicked earlier.
   * @param table table a {@link Table}
   * @param snapshotId a snapshot id picked for cherrypick operation
   */
  public static void validateNonAncestor(Table table, Long snapshotId) {
    if (SnapshotUtil.isCurrentAncestor(table, snapshotId)) {
      throw new CherrypickAncestorCommitException(snapshotId);
    }
    Long ancestorId = lookupAncestorBySourceSnapshot(table, snapshotId);
    if (ancestorId != null) {
      throw new CherrypickAncestorCommitException(snapshotId, ancestorId);
    }
  }

  private static Long lookupAncestorBySourceSnapshot(Table table, Long snapshotId) {
    Long ancestorId = null;
    String snapshotIdStr = String.valueOf(snapshotId);
    for (Long publishedSnapshotId : SnapshotUtil.currentAncestors(table)) {
      Map<String, String> summary = table.snapshot(publishedSnapshotId).summary();
      if (summary != null && snapshotIdStr.equals(summary.get(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP))) {
        ancestorId = publishedSnapshotId;
        break;
      }
    }
    return ancestorId;
  }

  private static boolean isWapWorkflow(TableMetadata base, Long snapshotId) {
    String wapId = stagedWapId(base.snapshot(snapshotId));
    return wapId != null && !wapId.isEmpty();
  }

  private static boolean isWapIdPublished(Table table, String wapId) {
    boolean isPublished = false;
    for (Long publishedSnapshotId : SnapshotUtil.currentAncestors(table)) {
      Map<String, String> summary = table.snapshot(publishedSnapshotId).summary();
      if (summary != null && wapId.equals(summary.get(SnapshotSummary.PUBLISHED_WAP_ID_PROP))) {
        isPublished = true;
        break;
      }
    }
    return isPublished;
  }
}
