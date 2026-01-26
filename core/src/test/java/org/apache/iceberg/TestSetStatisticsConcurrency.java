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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for SetStatistics concurrency handling.
 *
 * <p>Tests that SetStatistics properly handles concurrent modifications using retry mechanism.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestSetStatisticsConcurrency extends TestBase {

  /**
   * Test that SetStatistics succeeds when there are concurrent modifications.
   *
   * <p>This reproduces the production issue where compute_table_stats fails with
   * CommitFailedException when Lambda functions write concurrently.
   */
  @TestTemplate
  public void testSetStatisticsWithConcurrentModification() {
    // Step 1: Create initial snapshot
    table.newFastAppend().appendFile(FILE_A).commit();
    assertThat(version()).isEqualTo(1);

    TableMetadata base = readMetadata();
    long snapshotId = base.currentSnapshot().snapshotId();

    // Step 2: Create statistics file for this snapshot
    GenericStatisticsFile statisticsFile =
        new GenericStatisticsFile(
            snapshotId,
            "/some/statistics/file.puffin",
            100,
            42,
            ImmutableList.of(
                new GenericBlobMetadata(
                    "ndv",
                    snapshotId,
                    base.lastSequenceNumber(),
                    ImmutableList.of(1),
                    ImmutableMap.of("ndv", "12345"))));

    // Step 3: Start statistics update
    UpdateStatistics updateStats = table.updateStatistics().setStatistics(statisticsFile);

    // Step 4: Simulate concurrent write (this changes table metadata)
    // This is what happens in production when Lambda functions write concurrently
    table.newFastAppend().appendFile(FILE_B).commit();
    assertThat(version()).isEqualTo(2); // Table version changed

    // Step 5: Commit statistics - should succeed with retry mechanism
    updateStats.commit();

    // Step 6: Verify statistics were committed
    TableMetadata metadata = readMetadata();
    assertThat(metadata.statisticsFiles()).containsExactly(statisticsFile);
    assertThat(metadata.currentSnapshot().snapshotId())
        .isNotEqualTo(snapshotId); // Current snapshot changed
  }

  /** Test that SetStatistics respects retry configuration and fails after retries exhausted. */
  @TestTemplate
  public void testSetStatisticsRespectsRetryConfiguration() {
    // Step 1: Set low retry count
    table
        .updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "2") // Only 2 retries
        .commit();

    // Step 2: Create snapshot and statistics
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotId = readMetadata().currentSnapshot().snapshotId();

    GenericStatisticsFile statisticsFile =
        new GenericStatisticsFile(
            snapshotId, "/some/statistics/file.puffin", 100, 42, ImmutableList.of());

    // Step 3: Inject more failures than retry limit
    TestTables.TestTableOperations ops = table.ops();
    ops.failCommits(3); // More failures than retries (2)

    // Step 4: Should fail after exhausting configured retries
    UpdateStatistics updateStats = table.updateStatistics().setStatistics(statisticsFile);
    assertThatThrownBy(updateStats::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected failure");
  }

  /**
   * Test that SetStatistics succeeds with retry when failures are within retry limit.
   *
   * <p>This test explicitly injects failures to verify the retry mechanism works correctly.
   */
  @TestTemplate
  public void testSetStatisticsSucceedsWithinRetryLimit() {
    // Step 1: Create snapshot
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotId = readMetadata().currentSnapshot().snapshotId();

    // Step 2: Create statistics
    GenericStatisticsFile statisticsFile =
        new GenericStatisticsFile(
            snapshotId, "/some/statistics/file.puffin", 100, 42, ImmutableList.of());

    // Step 3: Inject failures WITHIN default retry limit
    // Default COMMIT_NUM_RETRIES is 4, so inject 2 failures (less than limit)
    TestTables.TestTableOperations ops = table.ops();
    ops.failCommits(2);

    // Step 4: Commit should succeed after retries (2 failures, then success)
    UpdateStatistics updateStats = table.updateStatistics().setStatistics(statisticsFile);
    updateStats.commit(); // Should NOT throw

    // Step 5: Verify statistics committed successfully
    TableMetadata metadata = readMetadata();
    assertThat(metadata.statisticsFiles()).containsExactly(statisticsFile);
  }

  /**
   * Test that reproduces high-concurrency production scenario (AWS Glue + Lambda).
   *
   * <p>Verifies that statistics commits succeed even with multiple concurrent writes.
   */
  @TestTemplate
  public void testProductionScenarioHighConcurrency() {
    // Step 1: Table starts with some data
    table.newFastAppend().appendFile(FILE_A).commit();
    long statisticsSnapshotId = readMetadata().currentSnapshot().snapshotId();

    // Step 2: Create statistics file (mimics compute_table_stats output)
    GenericStatisticsFile statisticsFile =
        new GenericStatisticsFile(
            statisticsSnapshotId,
            String.format("/metadata/stats/%d-uuid.stats", statisticsSnapshotId),
            30720,
            156,
            ImmutableList.of(
                new GenericBlobMetadata(
                    "apache-datasketches-theta-v1",
                    statisticsSnapshotId,
                    readMetadata().lastSequenceNumber(),
                    ImmutableList.of(1, 2, 3),
                    ImmutableMap.of("sketch-type", "theta"))));

    // Step 3: Prepare statistics update
    UpdateStatistics updateStats = table.updateStatistics().setStatistics(statisticsFile);

    // Step 4: Simulate high-frequency Lambda writes
    table.newFastAppend().appendFile(FILE_B).commit(); // Snapshot 2
    table.newFastAppend().appendFile(FILE_C).commit(); // Snapshot 3
    table.newFastAppend().appendFile(FILE_D).commit(); // Snapshot 4

    // Step 5: Commit statistics - should succeed despite concurrent modifications
    updateStats.commit();

    // Step 6: Verify final state
    TableMetadata metadata = readMetadata();

    // Statistics reference exists
    assertThat(metadata.statisticsFiles()).containsExactly(statisticsFile);

    // Statistics point to original snapshot
    assertThat(statisticsFile.snapshotId()).isEqualTo(statisticsSnapshotId);

    // All concurrent snapshots are preserved
    assertThat(metadata.snapshots()).hasSize(4);

    // Current snapshot is the latest (from last write)
    assertThat(metadata.currentSnapshot().snapshotId()).isNotEqualTo(statisticsSnapshotId);
  }
}
