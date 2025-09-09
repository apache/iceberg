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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestFileSystemWalker {

  @TempDir private Path tempDir;
  private String basePath;
  private Configuration hadoopConf;
  private Map<Integer, PartitionSpec> specs;
  private HadoopFileIO fileIO;

  @BeforeEach
  public void before() throws IOException {
    this.basePath = tempDir.toAbsolutePath().toString();
    this.hadoopConf = new Configuration();
    this.fileIO = new HadoopFileIO(hadoopConf);

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").build();
    specs = ImmutableMap.of(0, spec);

    Files.createDirectories(Paths.get(basePath, "normal_dir"));
    Files.createDirectories(Paths.get(basePath, "normal_dir_1"));
    Files.createDirectories(Paths.get(basePath, "normal_dir/dep1"));
    Files.createDirectories(Paths.get(basePath, "hidden_dir/_partition"));
    Files.createFile(Paths.get(basePath, "file1.txt"));
    Files.createFile(Paths.get(basePath, "normal_dir/file2.txt"));
    Files.createFile(Paths.get(basePath, "normal_dir/dep1/file3.txt"));
    Files.createFile(Paths.get(basePath, "normal_dir_1/file4.txt"));
    Files.createFile(Paths.get(basePath, "hidden_dir/.hidden_file.txt"));
    Files.createFile(Paths.get(basePath, "hidden_dir/_partition/file3.txt"));
  }

  @Test
  public void testListDirRecursivelyWithHadoop() {
    List<String> foundFiles = Lists.newArrayList();
    List<String> remainingDirs = Lists.newArrayList();
    Predicate<FileStatus> fileFilter =
        fileStatus -> fileStatus.getPath().getName().endsWith(".txt");
    FileSystemWalker.listDirRecursivelyWithHadoop(
        basePath,
        specs,
        fileFilter,
        hadoopConf,
        Integer.MAX_VALUE,
        Integer.MAX_VALUE,
        remainingDirs::add,
        foundFiles::add);

    assertThat(foundFiles).hasSize(4);
    assertThat(foundFiles).contains(Paths.get("file://", basePath, "file1.txt").toString());
    assertThat(foundFiles)
        .contains(Paths.get("file://", basePath, "normal_dir/file2.txt").toString());
    assertThat(foundFiles)
        .contains(Paths.get("file://", basePath, "normal_dir/dep1/file3.txt").toString());
    assertThat(foundFiles)
        .contains(Paths.get("file://", basePath, "normal_dir_1/file4.txt").toString());
    assertThat(remainingDirs).isEmpty();
  }

  @Test
  public void testListDirRecursivelyWithHadoopMaxDepth() {
    List<String> foundFiles = Lists.newArrayList();
    List<String> remainingDirs = Lists.newArrayList();
    Predicate<FileStatus> fileFilter =
        fileStatus -> fileStatus.getPath().getName().endsWith(".txt");
    FileSystemWalker.listDirRecursivelyWithHadoop(
        basePath,
        specs,
        fileFilter,
        hadoopConf,
        2, // maxDepth
        10, // maxDirectSubDirs
        remainingDirs::add,
        foundFiles::add);

    assertThat(foundFiles).hasSize(3);
    assertThat(foundFiles).contains(Paths.get("file://", basePath, "file1.txt").toString());
    assertThat(foundFiles)
        .contains(Paths.get("file://", basePath, "normal_dir/file2.txt").toString());
    assertThat(foundFiles)
        .contains(Paths.get("file://", basePath, "normal_dir_1/file4.txt").toString());
    assertThat(remainingDirs).hasSize(1);
    assertThat(remainingDirs)
        .contains(Paths.get("file://", basePath, "normal_dir/dep1").toString());
  }

  @Test
  public void testListDirRecursivelyWithHadoopMaxDirectSubDirs() {
    List<String> foundFiles = Lists.newArrayList();
    List<String> remainingDirs = Lists.newArrayList();
    Predicate<FileStatus> fileFilter =
        fileStatus -> fileStatus.getPath().getName().endsWith(".txt");
    FileSystemWalker.listDirRecursivelyWithHadoop(
        basePath,
        specs,
        fileFilter,
        hadoopConf,
        2, // maxDepth
        1, // maxDirectSubDirs
        remainingDirs::add,
        foundFiles::add);

    assertThat(foundFiles).hasSize(1);
    assertThat(foundFiles).contains(Paths.get("file://", basePath, "file1.txt").toString());
    assertThat(remainingDirs).hasSize(3);
    assertThat(remainingDirs).contains(Paths.get("file://", basePath, "normal_dir").toString());
    assertThat(remainingDirs).contains(Paths.get("file://", basePath, "normal_dir_1").toString());
    assertThat(remainingDirs).contains(Paths.get("file://", basePath, "hidden_dir").toString());
  }

  @Test
  public void testListDirRecursivelyWithFileIO() {
    List<String> foundFiles = Lists.newArrayList();
    Predicate<FileInfo> fileFilter = fileInfo -> fileInfo.location().endsWith(".txt");
    FileSystemWalker.listDirRecursivelyWithFileIO(
        fileIO, basePath, specs, fileFilter, foundFiles::add);

    assertThat(foundFiles).hasSize(4);
    assertThat(foundFiles).contains(Paths.get("file://", basePath, "file1.txt").toString());
    assertThat(foundFiles)
        .contains(Paths.get("file://", basePath, "normal_dir/file2.txt").toString());
    assertThat(foundFiles)
        .contains(Paths.get("file://", basePath, "normal_dir/dep1/file3.txt").toString());
    assertThat(foundFiles)
        .contains(Paths.get("file://", basePath, "normal_dir_1/file4.txt").toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testListDirRecursivelyNotInclude(boolean useHadoop) {
    List<String> foundFiles = Lists.newArrayList();
    String path = basePath + "/normal_dir";

    listFilesRecursively(path, useHadoop, specs, foundFiles);

    assertThat(foundFiles).hasSize(2);
    assertThat(foundFiles)
        .contains(Paths.get("file://", basePath, "normal_dir/file2.txt").toString());
    assertThat(foundFiles)
        .contains(Paths.get("file://", basePath, "normal_dir/dep1/file3.txt").toString());
    assertThat(foundFiles)
        .doesNotContain(Paths.get("file://", basePath, "normal_dir_1/file4.txt").toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPartitionAwareHiddenPathFilter(boolean useHadoop) throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "_hidden_field", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("_hidden_field").build();
    Map<Integer, PartitionSpec> partitionSpecs = ImmutableMap.of(0, spec);

    Path partitionDir = Paths.get(basePath, "_hidden_field=value");
    Files.createDirectories(partitionDir);
    Path fileInPartition = partitionDir.resolve("file4.txt");
    Files.createFile(fileInPartition);

    Path partitionDir1 = Paths.get(basePath, "_show_field=value");
    Files.createDirectories(partitionDir1);
    Path fileInPartition1 = partitionDir1.resolve("file5.txt");
    Files.createFile(fileInPartition1);

    List<String> foundFiles = Lists.newArrayList();
    listFilesRecursively(basePath, useHadoop, partitionSpecs, foundFiles);

    assertThat(foundFiles).contains("file:" + fileInPartition.toAbsolutePath());
    assertThat(foundFiles).contains(Paths.get("file://", basePath, "file1.txt").toString());
    assertThat(foundFiles).doesNotContain("file:" + fileInPartition1.toAbsolutePath());
  }

  private void listFilesRecursively(
      String path,
      boolean useHadoop,
      Map<Integer, PartitionSpec> partitionSpecs,
      List<String> foundFiles) {
    List<String> remainingDirs = Lists.newArrayList();

    if (useHadoop) {
      Predicate<FileStatus> fileFilter =
          fileStatus -> fileStatus.getPath().getName().endsWith(".txt");
      FileSystemWalker.listDirRecursivelyWithHadoop(
          path,
          partitionSpecs,
          fileFilter,
          hadoopConf,
          Integer.MAX_VALUE, // maxDepth
          Integer.MAX_VALUE, // maxDirectSubDirs
          remainingDirs::add,
          foundFiles::add);
    } else {
      Predicate<FileInfo> fileFilter = fileInfo -> fileInfo.location().endsWith(".txt");
      FileSystemWalker.listDirRecursivelyWithFileIO(
          fileIO, path, partitionSpecs, fileFilter, foundFiles::add);
    }

    assertThat(remainingDirs).isEmpty();
  }
}
