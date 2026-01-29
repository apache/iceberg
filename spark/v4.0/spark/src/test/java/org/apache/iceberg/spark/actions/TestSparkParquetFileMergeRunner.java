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
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileGenerationUtil;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ImmutableRewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestSparkParquetFileMergeRunner extends TestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  @TempDir private File tableDir;
  private String tableLocation;

  @BeforeEach
  public void setupTableLocation() {
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  public void testCanMergeAndGetSchemaReturnsNullForSortedTable() {
    // Create a table with a sort order
    Table table = TABLES.create(SCHEMA, tableLocation);
    table.updateProperties().set("write.metadata.metrics.default", "full").commit();
    table
        .replaceSortOrder()
        .asc("c1") // Sort by column c1 in ascending order
        .commit();

    // Verify the table has a sort order
    assertThat(table.sortOrder().isSorted()).isTrue();

    // Create a real RewriteFileGroup with real DataFile and FileScanTask objects
    DataFile dataFile = FileGenerationUtil.generateDataFile(table, null);
    MockFileScanTask task = new MockFileScanTask(dataFile);
    RewriteDataFiles.FileGroupInfo info = fileGroupInfo(0);
    RewriteFileGroup group =
        new RewriteFileGroup(info, Lists.newArrayList(task), 0, Long.MAX_VALUE, 0, 1);

    // Create runner and test canMergeAndGetSchema
    SparkParquetFileMergeRunner runner = new SparkParquetFileMergeRunner(spark, table);

    // Should return null because table has sort order
    assertThat(runner.canMergeAndGetSchema(group)).isNull();
  }

  @Test
  public void testCanMergeAndGetSchemaReturnsNullForFilesWithDeleteFiles() {
    // Create an unsorted table
    Table table = TABLES.create(SCHEMA, tableLocation);

    // Verify the table has no sort order
    assertThat(table.sortOrder().isUnsorted()).isTrue();

    // Create a real RewriteFileGroup with files that have delete files
    DataFile dataFile = FileGenerationUtil.generateDataFile(table, null);
    DeleteFile deleteFile = FileGenerationUtil.generatePositionDeleteFile(table, (StructLike) null);
    MockFileScanTask task = new MockFileScanTask(dataFile, new DeleteFile[] {deleteFile});
    RewriteDataFiles.FileGroupInfo info = fileGroupInfo(0);
    RewriteFileGroup group =
        new RewriteFileGroup(info, Lists.newArrayList(task), 0, Long.MAX_VALUE, 0, 1);

    // Create runner and test canMergeAndGetSchema
    SparkParquetFileMergeRunner runner = new SparkParquetFileMergeRunner(spark, table);

    // Should return null because files have delete files
    assertThat(runner.canMergeAndGetSchema(group)).isNull();
  }

  @Test
  public void testCanMergeAndGetSchemaReturnsNullForTableWithMultipleColumnSort() {
    // Create a table with a multi-column sort order (similar to z-ordering)
    Table table = TABLES.create(SCHEMA, tableLocation);
    table.updateProperties().set("write.metadata.metrics.default", "full").commit();

    // Create a sort order with multiple columns
    table.replaceSortOrder().asc("c1").asc("c2").commit();

    // Verify the table has a sort order
    assertThat(table.sortOrder().isSorted()).isTrue();

    // Create a real RewriteFileGroup
    DataFile dataFile = FileGenerationUtil.generateDataFile(table, null);
    MockFileScanTask task = new MockFileScanTask(dataFile);
    RewriteDataFiles.FileGroupInfo info = fileGroupInfo(0);
    RewriteFileGroup group =
        new RewriteFileGroup(info, Lists.newArrayList(task), 0, Long.MAX_VALUE, 0, 1);

    // Create runner and test canMergeAndGetSchema
    SparkParquetFileMergeRunner runner = new SparkParquetFileMergeRunner(spark, table);

    // Should return null because table has multi-column sort order
    assertThat(runner.canMergeAndGetSchema(group)).isNull();
  }

  private static RewriteDataFiles.FileGroupInfo fileGroupInfo(int globalIndex) {
    // Create an empty partition for unpartitioned tables
    StructLike emptyPartition = GenericRecord.create(PartitionSpec.unpartitioned().partitionType());
    return ImmutableRewriteDataFiles.FileGroupInfo.builder()
        .globalIndex(globalIndex)
        .partitionIndex(0)
        .partition(emptyPartition)
        .build();
  }
}
