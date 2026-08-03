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
import static org.assertj.core.api.Assertions.within;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.SortOrderStatsHandler.PartitionOverlapStats;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSortOrderStatsHandler {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "payload", Types.StringType.get()));

  private static final SortOrder SORT_ORDER = SortOrder.builderFor(SCHEMA).asc("id").build();

  @TempDir private File temp;

  private TestTables.TestTable table;

  @AfterEach
  public void cleanup() {
    TestTables.clearTables();
  }

  private void createTable(SortOrder sortOrder) {
    this.table =
        TestTables.create(
            temp, "overlap_test", SCHEMA, PartitionSpec.unpartitioned(), sortOrder, 2);
  }

  private DataFile dataFile(String path, Long lower, Long upper) {
    DataFiles.Builder builder =
        DataFiles.builder(table.spec())
            .withPath(path)
            .withFileSizeInBytes(128L * 1024 * 1024)
            .withRecordCount(1000L);

    if (lower != null && upper != null) {
      Map<Integer, ByteBuffer> lowerBounds =
          ImmutableMap.of(1, Conversions.toByteBuffer(Types.LongType.get(), lower));
      Map<Integer, ByteBuffer> upperBounds =
          ImmutableMap.of(1, Conversions.toByteBuffer(Types.LongType.get(), upper));
      builder.withMetrics(
          new Metrics(
              1000L, null, ImmutableMap.of(1, 1000L), null, null, lowerBounds, upperBounds));
    }

    return builder.build();
  }

  private void append(DataFile... files) {
    AppendFiles append = table.newAppend();
    for (DataFile file : files) {
      append.appendFile(file);
    }
    append.commit();
  }

  @Test
  public void testFullyOverlappingFiles() {
    createTable(SORT_ORDER);
    append(
        dataFile("/data/a.parquet", 0L, 100L),
        dataFile("/data/b.parquet", 0L, 100L),
        dataFile("/data/c.parquet", 0L, 100L));

    List<PartitionOverlapStats> stats = SortOrderStatsHandler.computeStats(table);
    assertThat(stats).hasSize(1);
    PartitionOverlapStats partition = stats.get(0);
    assertThat(partition.fileCount()).isEqualTo(3);
    assertThat(partition.filesMissingBounds()).isEqualTo(0);
    assertThat(partition.maxOverlapDepth()).isEqualTo(3);
    assertThat(partition.avgOverlapDepth()).isEqualTo(3.0, within(1e-9));
  }

  @Test
  public void testDisjointFiles() {
    createTable(SORT_ORDER);
    append(
        dataFile("/data/a.parquet", 0L, 10L),
        dataFile("/data/b.parquet", 20L, 30L),
        dataFile("/data/c.parquet", 40L, 50L));

    List<PartitionOverlapStats> stats = SortOrderStatsHandler.computeStats(table);
    assertThat(stats).hasSize(1);
    PartitionOverlapStats partition = stats.get(0);
    assertThat(partition.maxOverlapDepth()).isEqualTo(1);
    assertThat(partition.avgOverlapDepth()).isEqualTo(1.0, within(1e-9));
  }

  @Test
  public void testTouchingBoundariesCountAsOverlap() {
    createTable(SORT_ORDER);
    append(dataFile("/data/a.parquet", 0L, 10L), dataFile("/data/b.parquet", 10L, 20L));

    List<PartitionOverlapStats> stats = SortOrderStatsHandler.computeStats(table);
    assertThat(stats.get(0).maxOverlapDepth()).isEqualTo(2);
  }

  @Test
  public void testPartialOverlap() {
    createTable(SORT_ORDER);
    append(
        dataFile("/data/a.parquet", 0L, 10L),
        dataFile("/data/b.parquet", 5L, 15L),
        dataFile("/data/c.parquet", 20L, 30L));

    List<PartitionOverlapStats> stats = SortOrderStatsHandler.computeStats(table);
    PartitionOverlapStats partition = stats.get(0);
    assertThat(partition.maxOverlapDepth()).isEqualTo(2);
    // a-b overlap, c alone: depths per file are 2, 2, 1 -> avg = 1 + 2*1/3
    assertThat(partition.avgOverlapDepth()).isEqualTo(1.0 + 2.0 / 3.0, within(1e-9));
  }

  @Test
  public void testFilesMissingBounds() {
    createTable(SORT_ORDER);
    append(
        dataFile("/data/a.parquet", 0L, 10L),
        dataFile("/data/b.parquet", 5L, 15L),
        dataFile("/data/no-bounds.parquet", null, null));

    List<PartitionOverlapStats> stats = SortOrderStatsHandler.computeStats(table);
    PartitionOverlapStats partition = stats.get(0);
    assertThat(partition.fileCount()).isEqualTo(3);
    assertThat(partition.filesMissingBounds()).isEqualTo(1);
    assertThat(partition.maxOverlapDepth()).isEqualTo(2);
  }

  @Test
  public void testAllFilesMissingBounds() {
    createTable(SORT_ORDER);
    append(dataFile("/data/a.parquet", null, null), dataFile("/data/b.parquet", null, null));

    List<PartitionOverlapStats> stats = SortOrderStatsHandler.computeStats(table);
    PartitionOverlapStats partition = stats.get(0);
    assertThat(partition.fileCount()).isEqualTo(2);
    assertThat(partition.filesMissingBounds()).isEqualTo(2);
    assertThat(partition.maxOverlapDepth()).isNull();
    assertThat(partition.avgOverlapDepth()).isNull();
  }

  @Test
  public void testSingleFile() {
    createTable(SORT_ORDER);
    append(dataFile("/data/a.parquet", 0L, 100L));

    List<PartitionOverlapStats> stats = SortOrderStatsHandler.computeStats(table);
    assertThat(stats.get(0).maxOverlapDepth()).isEqualTo(1);
  }

  @Test
  public void testUnsortedTableFails() {
    createTable(SortOrder.unsorted());
    assertThatThrownBy(() -> SortOrderStatsHandler.computeStats(table))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("does not declare a sort order");
  }

  @Test
  public void testNonOrderPreservingTransformFails() {
    SortOrder bucketOrder =
        SortOrder.builderFor(SCHEMA)
            .asc(org.apache.iceberg.expressions.Expressions.bucket("id", 16))
            .build();
    createTable(bucketOrder);
    assertThatThrownBy(() -> SortOrderStatsHandler.computeStats(table))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("does not preserve order");
  }

  @Test
  public void testSnapshotIdSelectsOlderState() {
    createTable(SORT_ORDER);
    append(dataFile("/data/a.parquet", 0L, 100L), dataFile("/data/b.parquet", 0L, 100L));
    long firstSnapshot = table.currentSnapshot().snapshotId();
    append(dataFile("/data/c.parquet", 0L, 100L));

    List<PartitionOverlapStats> current = SortOrderStatsHandler.computeStats(table);
    assertThat(current.get(0).maxOverlapDepth()).isEqualTo(3);

    List<PartitionOverlapStats> old = SortOrderStatsHandler.computeStats(table, firstSnapshot);
    assertThat(old.get(0).maxOverlapDepth()).isEqualTo(2);
  }
}
