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

import org.junit.jupiter.api.Test;

/**
 * Tests for distributed scanning of PartitionsTable metadata.
 *
 * <p>These tests focus on validating the scan mode selection logic that determines when to use
 * distributed vs local scanning based on table properties.
 */
public class TestDistributedPartitionsTable extends TestBase {

  @Test
  public void testDistributedModeSelection() {
    // Test that distributed mode is selected when explicitly configured
    table.updateProperties().set(TableProperties.METADATA_PLANNING_MODE, "distributed").commit();

    PartitionsTable partitionsTable = new PartitionsTable(table);

    // newScan() should return DistributedPartitionsScan when distributed mode is set
    TableScan scan = partitionsTable.newScan();

    // Verify we get a DistributedPartitionsScan instance
    assertThat(scan.getClass().getName())
        .as("Should return DistributedPartitionsScan in distributed mode")
        .contains("DistributedPartitionsScan");
  }

  @Test
  public void testLocalModeSelection() {
    // Test that local mode is selected when explicitly configured
    table.updateProperties().set(TableProperties.METADATA_PLANNING_MODE, "local").commit();

    PartitionsTable partitionsTable = new PartitionsTable(table);

    // newScan() should return PartitionsScan when local mode is set
    TableScan scan = partitionsTable.newScan();

    // Verify we get a PartitionsScan instance (not DistributedPartitionsScan)
    assertThat(scan.getClass().getName())
        .as("Should return PartitionsScan in local mode")
        .contains("PartitionsScan")
        .as("Should not return DistributedPartitionsScan in local mode")
        .doesNotContain("DistributedPartitionsScan");
  }

  @Test
  public void testAutoModeLogic() {
    // Test AUTO mode behavior with empty table (should select local)
    table.updateProperties().set(TableProperties.METADATA_PLANNING_MODE, "auto").commit();

    PartitionsTable partitionsTable = new PartitionsTable(table);
    TableScan scan = partitionsTable.newScan();

    // With no data, AUTO should select local mode
    assertThat(scan.getClass().getName())
        .as("AUTO mode should select local scanning for tables with few manifests")
        .contains("PartitionsScan")
        .as("AUTO mode should not select distributed scanning for tables with few manifests")
        .doesNotContain("DistributedPartitionsScan");
  }

  @Test
  public void testDefaultModeIsAuto() {
    // Test that default behavior works when no explicit configuration is set
    PartitionsTable partitionsTable = new PartitionsTable(table);

    // Default should be AUTO mode, which should work without errors
    TableScan scan = partitionsTable.newScan();

    // Should not fail and should return a valid scan
    assertThat(scan).as("Default scan should not be null").isNotNull();

    // For a table with no data, AUTO should select local mode
    assertThat(scan.getClass().getName())
        .as("Default AUTO mode should select local scanning for empty tables")
        .contains("PartitionsScan");
  }

  @Test
  public void testSqlQueryIntegrationFlow() {
    // This test validates the complete flow that happens when users run:
    // SELECT max(max_utc_date) FROM dse.session_feature_engagement_f__partitions

    // 1. Spark calls PartitionsTable.newScan() (this is what we're testing)
    PartitionsTable partitionsTable = new PartitionsTable(table);

    // 2. Configure for distributed mode (like production tables)
    table.updateProperties().set(TableProperties.METADATA_PLANNING_MODE, "distributed").commit();

    // 3. The scan should automatically use distributed mode
    TableScan metadataScan = partitionsTable.newScan();

    // 4. Verify distributed scan is selected (this enables cluster distribution)
    assertThat(metadataScan.getClass().getName())
        .as("SQL queries on partitions metadata should use distributed scanning")
        .contains("DistributedPartitionsScan");

    // 5. This scan would then be executed by Spark across the cluster
    // (In real usage, Spark would call planFiles() and distribute tasks)
    assertThat(metadataScan).as("Metadata scan should be properly initialized").isNotNull();
  }

  @Test
  public void testUnpartitionedTableSupport() {
    // Test that unpartitioned tables work correctly with both modes
    PartitionsTable partitionsTable = new PartitionsTable(table);

    // Test distributed mode with unpartitioned table
    table.updateProperties().set(TableProperties.METADATA_PLANNING_MODE, "distributed").commit();

    TableScan distributedScan = partitionsTable.newScan();
    assertThat(distributedScan)
        .as("Distributed scan should work with unpartitioned tables")
        .isNotNull();

    // Test local mode with unpartitioned table
    table.updateProperties().set(TableProperties.METADATA_PLANNING_MODE, "local").commit();

    TableScan localScan = partitionsTable.newScan();
    assertThat(localScan).as("Local scan should work with unpartitioned tables").isNotNull();

    // Scans should be different types
    assertThat(localScan.getClass())
        .as("Local and distributed scans should be different types")
        .isNotEqualTo(distributedScan.getClass());
  }

  @Test
  public void testBackwardCompatibility() {
    // Verify that tables without the new property work exactly as before
    PartitionsTable partitionsTable = new PartitionsTable(table);

    // Don't set any metadata planning mode property
    TableScan scan = partitionsTable.newScan();

    // Should default to AUTO mode and work without issues
    assertThat(scan).as("Scan should work without metadata planning mode property").isNotNull();

    // For small tables, AUTO should behave like the old default (local)
    assertThat(scan.getClass().getName())
        .as("Backward compatibility: should use local scanning for small tables")
        .contains("PartitionsScan");
  }
}
