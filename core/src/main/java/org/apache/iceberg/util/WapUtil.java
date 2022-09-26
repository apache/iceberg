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

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
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
