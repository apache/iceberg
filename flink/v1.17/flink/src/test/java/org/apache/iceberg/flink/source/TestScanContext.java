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
package org.apache.iceberg.flink.source;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class TestScanContext {
  @Test
  void testIncrementalFromSnapshotId() {
    ScanContext context =
        ScanContext.builder()
            .streaming(true)
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_ID)
            .build();
    assertException(
        context, "Invalid starting snapshot id for SPECIFIC_START_SNAPSHOT_ID strategy: null");

    context =
        ScanContext.builder()
            .streaming(true)
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_ID)
            .startSnapshotId(1L)
            .startSnapshotTimestamp(1L)
            .build();
    assertException(
        context,
        "Invalid starting snapshot timestamp for SPECIFIC_START_SNAPSHOT_ID strategy: not null");
  }

  @Test
  void testIncrementalFromSnapshotTimestamp() {
    ScanContext context =
        ScanContext.builder()
            .streaming(true)
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP)
            .build();
    assertException(
        context,
        "Invalid starting snapshot timestamp for SPECIFIC_START_SNAPSHOT_TIMESTAMP strategy: null");

    context =
        ScanContext.builder()
            .streaming(true)
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP)
            .startSnapshotId(1L)
            .startSnapshotTimestamp(1L)
            .build();
    assertException(
        context, "Invalid starting snapshot id for SPECIFIC_START_SNAPSHOT_ID strategy: not null");
  }

  @Test
  void testStreaming() {
    ScanContext context = ScanContext.builder().streaming(true).useTag("tag").build();
    assertException(context, "Cannot scan table using ref tag configured for streaming reader");

    context = ScanContext.builder().streaming(true).useSnapshotId(1L).build();
    assertException(context, "Cannot set snapshot-id option for streaming reader");

    context = ScanContext.builder().streaming(true).asOfTimestamp(1L).build();
    assertException(context, "Cannot set as-of-timestamp option for streaming reader");

    context = ScanContext.builder().streaming(true).endSnapshotId(1L).build();
    assertException(context, "Cannot set end-snapshot-id option for streaming reader");

    context = ScanContext.builder().streaming(true).endTag("tag").build();
    assertException(context, "Cannot set end-tag option for streaming reader");
  }

  @Test
  void testStartConflict() {
    ScanContext context = ScanContext.builder().startTag("tag").startSnapshotId(1L).build();
    assertException(
        context,
        "Cannot specify more than one of start-snapshot-id, start-tag, or start-snapshot-timestamp.");

    context = ScanContext.builder().startSnapshotId(1L).startSnapshotTimestamp(1L).build();
    assertException(
        context,
        "Cannot specify more than one of start-snapshot-id, start-tag, or start-snapshot-timestamp.");

    context = ScanContext.builder().startTag("tag").startSnapshotTimestamp(1L).build();
    assertException(
        context,
        "Cannot specify more than one of start-snapshot-id, start-tag, or start-snapshot-timestamp.");
  }

  @Test
  void testEndConflict() {
    ScanContext context = ScanContext.builder().endTag("tag").endSnapshotId(1L).build();
    assertException(
        context,
        "Cannot specify more than one of end-snapshot-id, end-tag, or end-snapshot-timestamp.");

    context = ScanContext.builder().endSnapshotId(1L).endSnapshotTimestamp(1L).build();
    assertException(
        context,
        "Cannot specify more than one of end-snapshot-id, end-tag, or end-snapshot-timestamp.");

    context = ScanContext.builder().endTag("tag").endSnapshotTimestamp(1L).build();
    assertException(
        context,
        "Cannot specify more than one of end-snapshot-id, end-tag, or end-snapshot-timestamp.");
  }

  @Test
  void testMaxAllowedPlanningFailures() {
    ScanContext context = ScanContext.builder().maxAllowedPlanningFailures(-2).build();
    assertException(
        context, "Cannot set maxAllowedPlanningFailures to a negative number other than -1.");
  }

  private void assertException(ScanContext context, String message) {
    Assertions.assertThatThrownBy(() -> context.validate())
        .hasMessage(message)
        .isInstanceOf(IllegalArgumentException.class);
  }
}
