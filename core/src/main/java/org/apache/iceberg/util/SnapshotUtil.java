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

import com.google.common.collect.Lists;
import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;  
import org.apache.iceberg.TableMetadata;

public class SnapshotUtil {
  private SnapshotUtil() {
  }

  /**
   * Return the snapshot IDs for the ancestors of the current table state.
   * <p>
   * Ancestor IDs are ordered by commit time, descending. The first ID is the current snapshot, followed by its parent,
   * and so on.
   *
   * @param table a {@link Table}
   * @return a set of snapshot IDs of the known ancestor snapshots, including the current ID
   */
  public static List<Long> currentAncestors(Table table) {
    return ancestorIds(table.currentSnapshot(), table::snapshot);
  }

  public static List<Long> ancestorIds(Snapshot snapshot, Function<Long, Snapshot> lookup) {
    List<Long> ancestorIds = Lists.newArrayList();
    Snapshot current = snapshot;
    while (current != null) {
      ancestorIds.add(current.snapshotId());
      if (current.parentId() != null) {
        current = lookup.apply(current.parentId());
      } else {
        current = null;
      }
    }
    return ancestorIds;
  }

  public static boolean isCurrentAncestor(TableMetadata tableMeta, long snapshotId) {
    List<Long> currentAncestors = SnapshotUtil.ancestorIds(tableMeta.currentSnapshot(), tableMeta::snapshot);
    return currentAncestors.contains(snapshotId);
  }

  /**
   * Return the latest snapshot whose timestamp is before the provided timestamp.
   * @param tableMeta TableMetadata representing the table state on which the snapshot is being looked up
   * @param timestampMillis lookup snapshots before this timestamp
   * @return
   */
  public static Snapshot findLatestSnapshotOlderThan(TableMetadata tableMeta, long timestampMillis) {
    long snapshotTimestamp = 0;
    Snapshot result = null;
    for (Snapshot snapshot : tableMeta.snapshots()) {
      if (snapshot.timestampMillis() < timestampMillis &&
          snapshot.timestampMillis() > snapshotTimestamp) {
        result = snapshot;
        snapshotTimestamp = snapshot.timestampMillis();
      }
    }
    return result;
  }
}
