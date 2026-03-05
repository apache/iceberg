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
import org.apache.flink.annotation.VisibleForTesting;

/**
 * This enumeration history is used for split discovery throttling. It tracks the discovered split
 * count per every non-empty enumeration.
 */
@ThreadSafe
class EnumerationHistory {

  private final int[] history;
  // int (2B) should be enough without overflow for enumeration history
  private int count;

  EnumerationHistory(int maxHistorySize) {
    this.history = new int[maxHistorySize];
  }

  synchronized void restore(int[] restoredHistory) {
    int startingOffset = 0;
    int restoreSize = restoredHistory.length;

    if (restoredHistory.length > history.length) {
      // keep the newest history
      startingOffset = restoredHistory.length - history.length;
      // only restore the latest history up to maxHistorySize
      restoreSize = history.length;
    }

    System.arraycopy(restoredHistory, startingOffset, history, 0, restoreSize);
    count = restoreSize;
  }

  synchronized int[] snapshot() {
    int len = history.length;
    if (count > len) {
      int[] copy = new int[len];
      // this is like a circular buffer
      int indexForOldest = count % len;
      System.arraycopy(history, indexForOldest, copy, 0, len - indexForOldest);
      System.arraycopy(history, 0, copy, len - indexForOldest, indexForOldest);
      return copy;
    } else {
      return Arrays.copyOfRange(history, 0, count);
    }
  }

  /** Add the split count from the last enumeration result. */
  synchronized void add(int splitCount) {
    int pos = count % history.length;
    history[pos] = splitCount;
    count += 1;
  }

  @VisibleForTesting
  synchronized boolean hasFullHistory() {
    return count >= history.length;
  }

  /**
   * Checks whether split discovery should be paused.
   *
   * @return true if split discovery should pause because assigner has too many splits already.
   */
  synchronized boolean shouldPauseSplitDiscovery(int pendingSplitCountFromAssigner) {
    if (count < history.length) {
      // only check throttling when full history is obtained.
      return false;
    } else {
      // if ScanContext#maxPlanningSnapshotCount() is 10, each split enumeration can
      // discovery splits up to 10 snapshots. if maxHistorySize is 3, the max number of
      // splits tracked in assigner shouldn't be more than 10 * (3 + 1) snapshots
      // worth of splits. +1 because there could be another enumeration when the
      // pending splits fall just below the 10 * 3.
      int totalSplitCountFromRecentDiscovery = Arrays.stream(history).reduce(0, Integer::sum);
      return pendingSplitCountFromAssigner >= totalSplitCountFromRecentDiscovery;
    }
  }
}
