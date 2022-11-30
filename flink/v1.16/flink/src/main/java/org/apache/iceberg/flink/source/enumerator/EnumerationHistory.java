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
package org.apache.iceberg.flink.source.enumerator;

import java.util.Arrays;
import javax.annotation.concurrent.ThreadSafe;

/** This enumeration history is used for split discovery throttling. */
@ThreadSafe
class EnumerationHistory {

  private final int[] enumerationSplitCountHistory;
  // int (2B) should be enough without overflow for enumeration history
  private int count;

  EnumerationHistory(int maxHistorySize) {
    this.enumerationSplitCountHistory = new int[maxHistorySize];
  }

  /** Add the split count from the last enumeration result. */
  synchronized void add(int splitCount) {
    int pos = count % enumerationSplitCountHistory.length;
    enumerationSplitCountHistory[pos] = splitCount;
    count += 1;
  }

  /** @return true if split discovery should pause because assigner has too many splits already. */
  synchronized boolean shouldPauseSplitDiscovery(int pendingSplitCountFromAssigner) {
    if (count < enumerationSplitCountHistory.length) {
      // only check throttling when full history is obtained.
      return false;
    } else {
      // if ScanContext#maxPlanningSnapshotCount() is 5, each split enumeration can
      // discovery splits up to 6 snapshots. if maxHistorySize is 3, the max number of
      // splits tracked in assigner shouldn't be more than 15 snapshots worth of splits.
      // Split discovery pauses when reaching that threshold.
      int totalSplitCountFromRecentDiscovery =
          Arrays.stream(enumerationSplitCountHistory).reduce(0, Integer::sum);
      return pendingSplitCountFromAssigner >= totalSplitCountFromRecentDiscovery;
    }
  }
}
