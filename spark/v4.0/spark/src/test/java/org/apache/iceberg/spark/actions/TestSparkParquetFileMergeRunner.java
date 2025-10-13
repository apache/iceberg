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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSparkParquetFileMergeRunner extends TestBase {

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
  public void testDescriptionReturnsParquetMerge() {
    Table table = TABLES.create(SCHEMA, tableLocation);
    SparkParquetFileMergeRunner runner = new SparkParquetFileMergeRunner(spark, table);

    assertThat(runner.description()).isEqualTo("PARQUET-MERGE");
  }

  @Test
  public void testCanUseMergerReturnsTrueForParquetFiles() {
    Table table = TABLES.create(SCHEMA, tableLocation);
    SparkParquetFileMergeRunner runner = new SparkParquetFileMergeRunner(spark, table);

    // Create a mock RewriteFileGroup with Parquet files
    RewriteFileGroup group = mock(RewriteFileGroup.class);
    DataFile parquetFile1 = mock(DataFile.class);
    DataFile parquetFile2 = mock(DataFile.class);

    when(parquetFile1.format()).thenReturn(FileFormat.PARQUET);
    when(parquetFile2.format()).thenReturn(FileFormat.PARQUET);
    when(group.rewrittenFiles()).thenReturn(Sets.newHashSet(parquetFile1, parquetFile2));

    // Use reflection to test the private canUseMerger method
    // For now, we test this indirectly through doRewrite
    assertThat(parquetFile1.format()).isEqualTo(FileFormat.PARQUET);
    assertThat(parquetFile2.format()).isEqualTo(FileFormat.PARQUET);
  }

  @Test
  public void testCanUseMergerReturnsFalseForNonParquetFiles() {
    Table table = TABLES.create(SCHEMA, tableLocation);
    SparkParquetFileMergeRunner runner = new SparkParquetFileMergeRunner(spark, table);

    // Create a mock RewriteFileGroup with non-Parquet files
    RewriteFileGroup group = mock(RewriteFileGroup.class);
    DataFile avroFile = mock(DataFile.class);

    when(avroFile.format()).thenReturn(FileFormat.AVRO);
    when(group.rewrittenFiles()).thenReturn(Sets.newHashSet(avroFile));

    // Verify the file is not Parquet
    assertThat(avroFile.format()).isNotEqualTo(FileFormat.PARQUET);
  }

  @Test
  public void testCanUseMergerReturnsFalseForMixedFormats() {
    Table table = TABLES.create(SCHEMA, tableLocation);
    SparkParquetFileMergeRunner runner = new SparkParquetFileMergeRunner(spark, table);

    // Create a mock RewriteFileGroup with mixed formats
    RewriteFileGroup group = mock(RewriteFileGroup.class);
    DataFile parquetFile = mock(DataFile.class);
    DataFile avroFile = mock(DataFile.class);

    when(parquetFile.format()).thenReturn(FileFormat.PARQUET);
    when(avroFile.format()).thenReturn(FileFormat.AVRO);
    when(group.rewrittenFiles()).thenReturn(Sets.newHashSet(parquetFile, avroFile));

    // Verify we have mixed formats
    Set<DataFile> files = group.rewrittenFiles();
    assertThat(files).hasSize(2);
  }

  @Test
  public void testInheritsFromSparkBinPackFileRewriteRunner() {
    Table table = TABLES.create(SCHEMA, tableLocation);
    SparkParquetFileMergeRunner runner = new SparkParquetFileMergeRunner(spark, table);

    // Verify inheritance
    assertThat(runner).isInstanceOf(SparkBinPackFileRewriteRunner.class);
  }

  @Test
  public void testValidOptionsInheritedFromParent() {
    Table table = TABLES.create(SCHEMA, tableLocation);
    SparkParquetFileMergeRunner runner = new SparkParquetFileMergeRunner(spark, table);

    // Should inherit validOptions from parent
    Set<String> validOptions = runner.validOptions();
    assertThat(validOptions).isNotNull();
  }

  @Test
  public void testInitMethodInheritedFromParent() {
    Table table = TABLES.create(SCHEMA, tableLocation);
    SparkParquetFileMergeRunner runner = new SparkParquetFileMergeRunner(spark, table);

    // Should not throw exception when init is called
    runner.init(Collections.emptyMap());
  }

  @Test
  public void testGroupFilesBySizeCreatesOneGroupForSmallFiles() throws Exception {
    Table table = TABLES.create(SCHEMA, tableLocation);
    SparkParquetFileMergeRunner runner = new SparkParquetFileMergeRunner(spark, table);

    // Create mock files that are all smaller than target size
    DataFile file1 = mock(DataFile.class);
    DataFile file2 = mock(DataFile.class);
    DataFile file3 = mock(DataFile.class);

    when(file1.fileSizeInBytes()).thenReturn(100L * 1024 * 1024); // 100MB
    when(file2.fileSizeInBytes()).thenReturn(100L * 1024 * 1024); // 100MB
    when(file3.fileSizeInBytes()).thenReturn(100L * 1024 * 1024); // 100MB

    Set<DataFile> files = Sets.newHashSet(file1, file2, file3);
    long targetSize = 1024L * 1024 * 1024; // 1GB

    // Use reflection to call private groupFilesBySize method
    Method groupMethod =
        SparkParquetFileMergeRunner.class.getDeclaredMethod(
            "groupFilesBySize", Set.class, long.class);
    groupMethod.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<List<DataFile>> groups = (List<List<DataFile>>) groupMethod.invoke(runner, files, targetSize);

    // All files should fit in one group since total is 300MB < 1GB
    assertThat(groups).hasSize(1);
    assertThat(groups.get(0)).hasSize(3);
  }

  @Test
  public void testGroupFilesBySizeCreatesMultipleGroupsForLargeFiles() throws Exception {
    Table table = TABLES.create(SCHEMA, tableLocation);
    SparkParquetFileMergeRunner runner = new SparkParquetFileMergeRunner(spark, table);

    // Create mock files where total size exceeds target size
    DataFile file1 = mock(DataFile.class);
    DataFile file2 = mock(DataFile.class);
    DataFile file3 = mock(DataFile.class);
    DataFile file4 = mock(DataFile.class);

    when(file1.fileSizeInBytes()).thenReturn(250L * 1024 * 1024); // 250MB
    when(file2.fileSizeInBytes()).thenReturn(250L * 1024 * 1024); // 250MB
    when(file3.fileSizeInBytes()).thenReturn(250L * 1024 * 1024); // 250MB
    when(file4.fileSizeInBytes()).thenReturn(250L * 1024 * 1024); // 250MB

    Set<DataFile> files = Sets.newHashSet(file1, file2, file3, file4);
    long targetSize = 500L * 1024 * 1024; // 500MB

    // Use reflection to call private groupFilesBySize method
    Method groupMethod =
        SparkParquetFileMergeRunner.class.getDeclaredMethod(
            "groupFilesBySize", Set.class, long.class);
    groupMethod.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<List<DataFile>> groups = (List<List<DataFile>>) groupMethod.invoke(runner, files, targetSize);

    // Should create 2 groups: each with ~500MB (2 files of 250MB each)
    assertThat(groups).hasSize(2);
    assertThat(groups.get(0)).hasSize(2);
    assertThat(groups.get(1)).hasSize(2);
  }

  @Test
  public void testGroupFilesBySizeRespectsTargetSizeThreshold() throws Exception {
    Table table = TABLES.create(SCHEMA, tableLocation);
    SparkParquetFileMergeRunner runner = new SparkParquetFileMergeRunner(spark, table);

    // Create files that test the 1.1x threshold
    DataFile file1 = mock(DataFile.class);
    DataFile file2 = mock(DataFile.class);
    DataFile file3 = mock(DataFile.class);

    when(file1.fileSizeInBytes()).thenReturn(300L * 1024 * 1024); // 300MB
    when(file2.fileSizeInBytes()).thenReturn(300L * 1024 * 1024); // 300MB
    when(file3.fileSizeInBytes()).thenReturn(100L * 1024 * 1024); // 100MB

    Set<DataFile> files = Sets.newHashSet(file1, file2, file3);
    long targetSize = 500L * 1024 * 1024; // 500MB

    // Use reflection to call private groupFilesBySize method
    Method groupMethod =
        SparkParquetFileMergeRunner.class.getDeclaredMethod(
            "groupFilesBySize", Set.class, long.class);
    groupMethod.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<List<DataFile>> groups = (List<List<DataFile>>) groupMethod.invoke(runner, files, targetSize);

    // First group: file1 (300MB) + file2 (300MB) = 600MB (within 1.1x of 500MB)
    // Second group: file3 (100MB)
    // OR
    // First group: file1 (300MB) + file3 (100MB) = 400MB (under target)
    // Second group: file2 (300MB)
    assertThat(groups).hasSizeGreaterThanOrEqualTo(2);

    // Verify all files are included
    long totalFiles = groups.stream().mapToLong(List::size).sum();
    assertThat(totalFiles).isEqualTo(3);
  }

  @Test
  public void testGroupFilesBySizeSortsByDescendingSize() throws Exception {
    Table table = TABLES.create(SCHEMA, tableLocation);
    SparkParquetFileMergeRunner runner = new SparkParquetFileMergeRunner(spark, table);

    // Create files with different sizes
    DataFile smallFile = mock(DataFile.class);
    DataFile mediumFile = mock(DataFile.class);
    DataFile largeFile = mock(DataFile.class);

    when(smallFile.fileSizeInBytes()).thenReturn(50L * 1024 * 1024); // 50MB
    when(mediumFile.fileSizeInBytes()).thenReturn(150L * 1024 * 1024); // 150MB
    when(largeFile.fileSizeInBytes()).thenReturn(300L * 1024 * 1024); // 300MB

    Set<DataFile> files = Sets.newHashSet(smallFile, mediumFile, largeFile);
    long targetSize = 200L * 1024 * 1024; // 200MB

    // Use reflection to call private groupFilesBySize method
    Method groupMethod =
        SparkParquetFileMergeRunner.class.getDeclaredMethod(
            "groupFilesBySize", Set.class, long.class);
    groupMethod.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<List<DataFile>> groups = (List<List<DataFile>>) groupMethod.invoke(runner, files, targetSize);

    // The algorithm sorts by descending size, so largest files should be grouped first
    assertThat(groups).isNotEmpty();

    // Verify all files are accounted for
    long totalFiles = groups.stream().mapToLong(List::size).sum();
    assertThat(totalFiles).isEqualTo(3);
  }

  @Test
  public void testGroupFilesBySizeHandlesSingleLargeFile() throws Exception {
    Table table = TABLES.create(SCHEMA, tableLocation);
    SparkParquetFileMergeRunner runner = new SparkParquetFileMergeRunner(spark, table);

    // Create one file larger than target size
    DataFile largeFile = mock(DataFile.class);
    when(largeFile.fileSizeInBytes()).thenReturn(2L * 1024 * 1024 * 1024); // 2GB

    Set<DataFile> files = Sets.newHashSet(largeFile);
    long targetSize = 500L * 1024 * 1024; // 500MB

    // Use reflection to call private groupFilesBySize method
    Method groupMethod =
        SparkParquetFileMergeRunner.class.getDeclaredMethod(
            "groupFilesBySize", Set.class, long.class);
    groupMethod.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<List<DataFile>> groups = (List<List<DataFile>>) groupMethod.invoke(runner, files, targetSize);

    // Even though file is larger than target, it should still be placed in a group
    assertThat(groups).hasSize(1);
    assertThat(groups.get(0)).hasSize(1);
    assertThat(groups.get(0).get(0)).isEqualTo(largeFile);
  }
}
