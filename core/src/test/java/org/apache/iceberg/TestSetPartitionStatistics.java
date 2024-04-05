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

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSetPartitionStatistics extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  @TestTemplate
  public void testEmptyUpdateStatistics() {
    assertTableMetadataVersion(0);
    TableMetadata base = readMetadata();

    table.updatePartitionStatistics().commit();

    assertThat(table.ops().current()).isSameAs(base);
    assertTableMetadataVersion(1);
  }

  @TestTemplate
  public void testEmptyTransactionalUpdateStatistics() {
    assertTableMetadataVersion(0);
    TableMetadata base = readMetadata();

    Transaction transaction = table.newTransaction();
    transaction.updatePartitionStatistics().commit();
    transaction.commitTransaction();

    assertThat(table.ops().current()).isSameAs(base);
    assertTableMetadataVersion(0);
  }

  @TestTemplate
  public void testUpdateStatistics() {
    // Create a snapshot
    table.newFastAppend().commit();
    assertTableMetadataVersion(1);

    TableMetadata base = readMetadata();
    long snapshotId = base.currentSnapshot().snapshotId();
    PartitionStatisticsFile partitionStatisticsFile =
        ImmutableGenericPartitionStatisticsFile.builder()
            .snapshotId(snapshotId)
            .path("/some/partition/statistics/file.parquet")
            .fileSizeInBytes(42L)
            .build();

    table.updatePartitionStatistics().setPartitionStatistics(partitionStatisticsFile).commit();

    TableMetadata metadata = readMetadata();
    assertTableMetadataVersion(2);
    assertThat(metadata.currentSnapshot().snapshotId()).isEqualTo(snapshotId);
    assertThat(metadata.partitionStatisticsFiles()).containsExactly(partitionStatisticsFile);
  }

  @TestTemplate
  public void testRemoveStatistics() {
    // Create a snapshot
    table.newFastAppend().commit();
    assertTableMetadataVersion(1);

    TableMetadata base = readMetadata();
    long snapshotId = base.currentSnapshot().snapshotId();
    PartitionStatisticsFile partitionStatisticsFile =
        ImmutableGenericPartitionStatisticsFile.builder()
            .snapshotId(snapshotId)
            .path("/some/partition/statistics/file.parquet")
            .fileSizeInBytes(42L)
            .build();

    table.updatePartitionStatistics().setPartitionStatistics(partitionStatisticsFile).commit();

    TableMetadata metadata = readMetadata();
    assertTableMetadataVersion(2);
    assertThat(metadata.partitionStatisticsFiles()).containsExactly(partitionStatisticsFile);

    table.updatePartitionStatistics().removePartitionStatistics(snapshotId).commit();

    metadata = readMetadata();
    assertTableMetadataVersion(3);
    assertThat(metadata.partitionStatisticsFiles()).isEmpty();
  }

  private void assertTableMetadataVersion(int expected) {
    assertThat(version()).isEqualTo(expected);
  }
}
