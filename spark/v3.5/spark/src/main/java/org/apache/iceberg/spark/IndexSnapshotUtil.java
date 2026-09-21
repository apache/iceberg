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
package org.apache.iceberg.spark;

import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;

/**
 * Shared helper for reasoning about which data files a secondary index snapshot does or doesn't
 * cover, relative to a table's current state.
 *
 * <p>Used on both the read side (deciding which files a stale SCALAR index can still prune, in
 * {@code SparkScanBuilder}) and the write side (deciding which files an incremental index rebuild
 * needs to newly index, in {@code BuildScalarIndexProcedure}) -- kept in one place because a
 * divergent bugfix between two copies of this logic would be a correctness risk, not just
 * inconsistent behavior.
 */
public class IndexSnapshotUtil {

  private IndexSnapshotUtil() {}

  /**
   * Data file paths added to {@code table} strictly after {@code sourceSnapshotId} up to and
   * including {@code currentSnapshotId}.
   *
   * <p>Requires every snapshot in that range to be a pure append -- throws {@link
   * IllegalStateException} otherwise (for example, if a compaction/rewrite ran in between).
   * Silently omitting such a snapshot's files here would be a correctness risk, not just a missed
   * optimization: a row physically moved by a rewrite into a file this method fails to report
   * could end up covered by neither an index's existing entries nor its "added since" set, and
   * never be indexed or scanned again. Callers should let the exception propagate and fall back
   * to their safe default (no pruning, or a full rebuild) rather than catch it here.
   *
   * @throws IllegalArgumentException if {@code sourceSnapshotId} is not an ancestor of {@code
   *     currentSnapshotId}
   * @throws IllegalStateException if any snapshot between them is not an append
   */
  public static Set<String> addedFilePathsSince(
      Table table, long sourceSnapshotId, long currentSnapshotId) {
    Preconditions.checkArgument(
        SnapshotUtil.isAncestorOf(table, currentSnapshotId, sourceSnapshotId),
        "Source snapshot %s is not an ancestor of the current snapshot %s",
        sourceSnapshotId,
        currentSnapshotId);

    Set<String> paths = Sets.newHashSet();
    for (Snapshot snapshot :
        SnapshotUtil.ancestorsBetween(currentSnapshotId, sourceSnapshotId, table::snapshot)) {
      Preconditions.checkState(
          DataOperations.APPEND.equals(snapshot.operation()),
          "Cannot safely determine files added since snapshot %s: snapshot %s in between is a "
              + "'%s', not an append",
          sourceSnapshotId,
          snapshot.snapshotId(),
          snapshot.operation());
      for (DataFile file : snapshot.addedDataFiles(table.io())) {
        paths.add(file.location());
      }
    }
    return paths;
  }
}
