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

import com.google.common.collect.Sets;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

public class SnapshotUtil {
  private SnapshotUtil() {
  }

  /**
   * Return the set of snapshot IDs for the known ancestors of the current table state.
   *
   * @param table a {@link Table}
   * @return a set of snapshot IDs of the known ancestor snapshots, including the current ID
   */
  public static Set<Long> currentAncestors(Table table) {
    return ancestorIds(table.currentSnapshot(), table::snapshot);
  }

  public static Set<Long> ancestorIds(Snapshot snapshot, Function<Long, Snapshot> lookup) {
    Set<Long> ancestorIds = Sets.newHashSet();
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
}
