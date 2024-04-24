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
    Assertions.assertThatThrownBy(
            () ->
                ScanContext.builder()
                    .streaming(true)
                    .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_ID)
                    .build())
        .hasMessage("Invalid starting snapshot id for SPECIFIC_START_SNAPSHOT_ID strategy: null")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(
            () ->
                ScanContext.builder()
                    .streaming(true)
                    .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_ID)
                    .startSnapshotId(1L)
                    .startSnapshotTimestamp(1L)
                    .build())
        .hasMessage(
            "Invalid starting snapshot timestamp for SPECIFIC_START_SNAPSHOT_ID strategy: not null")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testIncrementalFromSnapshotTimestamp() {
    Assertions.assertThatThrownBy(
            () ->
                ScanContext.builder()
                    .streaming(true)
                    .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP)
                    .build())
        .hasMessage(
            "Invalid starting snapshot timestamp for SPECIFIC_START_SNAPSHOT_TIMESTAMP strategy: null")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(
            () ->
                ScanContext.builder()
                    .streaming(true)
                    .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP)
                    .startSnapshotId(1L)
                    .startSnapshotTimestamp(1L)
                    .build())
        .hasMessage(
            "Invalid starting snapshot id for SPECIFIC_START_SNAPSHOT_ID strategy: not null")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testStreaming() {
    Assertions.assertThatThrownBy(() -> ScanContext.builder().streaming(true).useTag("tag").build())
        .hasMessage("Cannot scan table using ref tag configured for streaming reader")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(
            () -> ScanContext.builder().streaming(true).useSnapshotId(1L).build())
        .hasMessage("Cannot set snapshot-id option for streaming reader")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(
            () -> ScanContext.builder().streaming(true).asOfTimestamp(1L).build())
        .hasMessage("Cannot set as-of-timestamp option for streaming reader")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(
            () -> ScanContext.builder().streaming(true).endSnapshotId(1L).build())
        .hasMessage("Cannot set end-snapshot-id option for streaming reader")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(() -> ScanContext.builder().streaming(true).endTag("tag").build())
        .hasMessage("Cannot set end-tag option for streaming reader")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testStartConflict() {
    Assertions.assertThatThrownBy(
            () -> ScanContext.builder().startTag("tag").startSnapshotId(1L).build())
        .hasMessage("START_SNAPSHOT_ID and START_TAG cannot both be set.")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testEndConflict() {
    Assertions.assertThatThrownBy(
            () -> ScanContext.builder().endTag("tag").endSnapshotId(1L).build())
        .hasMessage("END_SNAPSHOT_ID and END_TAG cannot both be set.")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testMaxAllowedPlanningFailures() {
    Assertions.assertThatThrownBy(
            () -> ScanContext.builder().maxAllowedPlanningFailures(-2).build())
        .hasMessage("Cannot set maxAllowedPlanningFailures to a negative number other than -1.")
        .isInstanceOf(IllegalArgumentException.class);
  }
}
