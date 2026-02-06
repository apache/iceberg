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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class TestEnumerationHistory {
  private static final int MAX_HISTORY_SIZE = 3;
  private static final int FEW_PENDING_SPLITS = 2;
  private static final int TOO_MANY_PENDING_SPLITS = 100;

  @Test
  public void testEmptyHistory() {
    EnumerationHistory history = new EnumerationHistory(MAX_HISTORY_SIZE);
    int[] expectedHistorySnapshot = new int[0];
    testHistory(history, expectedHistorySnapshot);
  }

  @Test
  public void testNotFullHistory() {
    EnumerationHistory history = new EnumerationHistory(3);
    history.add(1);
    history.add(2);
    int[] expectedHistorySnapshot = {1, 2};
    testHistory(history, expectedHistorySnapshot);
  }

  @Test
  public void testExactFullHistory() {
    EnumerationHistory history = new EnumerationHistory(3);
    history.add(1);
    history.add(2);
    history.add(3);
    int[] expectedHistorySnapshot = {1, 2, 3};
    testHistory(history, expectedHistorySnapshot);
  }

  @Test
  public void testOneMoreThanFullHistory() {
    EnumerationHistory history = new EnumerationHistory(3);
    history.add(1);
    history.add(2);
    history.add(3);
    history.add(4);
    int[] expectedHistorySnapshot = {2, 3, 4};
    testHistory(history, expectedHistorySnapshot);
  }

  @Test
  public void testTwoMoreThanFullHistory() {
    EnumerationHistory history = new EnumerationHistory(3);
    history.add(1);
    history.add(2);
    history.add(3);
    history.add(4);
    history.add(5);
    int[] expectedHistorySnapshot = {3, 4, 5};
    testHistory(history, expectedHistorySnapshot);
  }

  @Test
  public void testThreeMoreThanFullHistory() {
    EnumerationHistory history = new EnumerationHistory(3);
    history.add(1);
    history.add(2);
    history.add(3);
    history.add(4);
    history.add(5);
    history.add(6);
    int[] expectedHistorySnapshot = {4, 5, 6};
    testHistory(history, expectedHistorySnapshot);
  }

  private void testHistory(EnumerationHistory history, int[] expectedHistorySnapshot) {
    assertThat(history.shouldPauseSplitDiscovery(FEW_PENDING_SPLITS)).isFalse();
    if (history.hasFullHistory()) {
      // throttle because pending split count is more than the sum of enumeration history
      assertThat(history.shouldPauseSplitDiscovery(TOO_MANY_PENDING_SPLITS)).isTrue();
    } else {
      // skipped throttling check because there is not enough history
      assertThat(history.shouldPauseSplitDiscovery(TOO_MANY_PENDING_SPLITS)).isFalse();
    }

    int[] historySnapshot = history.snapshot();
    assertThat(historySnapshot).containsExactly(expectedHistorySnapshot);

    EnumerationHistory restoredHistory = new EnumerationHistory(MAX_HISTORY_SIZE);
    restoredHistory.restore(historySnapshot);

    assertThat(history.shouldPauseSplitDiscovery(FEW_PENDING_SPLITS)).isFalse();
    if (history.hasFullHistory()) {
      // throttle because pending split count is more than the sum of enumeration history
      assertThat(history.shouldPauseSplitDiscovery(TOO_MANY_PENDING_SPLITS)).isTrue();
    } else {
      // skipped throttling check because there is not enough history
      assertThat(history.shouldPauseSplitDiscovery(30)).isFalse();
    }
  }

  @Test
  public void testRestoreDifferentSize() {
    EnumerationHistory history = new EnumerationHistory(3);
    history.add(1);
    history.add(2);
    history.add(3);
    int[] historySnapshot = history.snapshot();

    EnumerationHistory smallerHistory = new EnumerationHistory(2);
    smallerHistory.restore(historySnapshot);
    int[] expectedRestoredHistorySnapshot = {2, 3};
    assertThat(smallerHistory.snapshot()).containsExactly(expectedRestoredHistorySnapshot);

    EnumerationHistory largerHisotry = new EnumerationHistory(4);
    largerHisotry.restore(historySnapshot);
    assertThat(largerHisotry.snapshot()).containsExactly(historySnapshot);
  }
}
