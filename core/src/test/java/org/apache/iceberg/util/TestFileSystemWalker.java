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
import java.util.ArrayList;
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
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
    Files.createDirectories(Paths.get(basePath, "normal_dir/dep1"));
    Files.createDirectories(Paths.get(basePath, "hidden_dir/_partition"));
    Files.createFile(Paths.get(basePath, "file1.txt"));
    Files.createFile(Paths.get(basePath, "normal_dir/file2.txt"));
    Files.createFile(Paths.get(basePath, "normal_dir/dep1/file3.txt"));
    Files.createFile(Paths.get(basePath, "hidden_dir/.hidden_file.txt"));
    Files.createFile(Paths.get(basePath, "hidden_dir/_partition/file3.txt"));
  }

  @Test
  public void testListDirRecursivelyWithHadoop() {
    List<String> foundFiles = new ArrayList<>();
    List<String> remainingDirs = new ArrayList<>();
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

    assertThat(foundFiles).hasSize(3);
    assertThat(foundFiles).contains(Paths.get("file://", basePath, "file1.txt").toString());
    assertThat(foundFiles)
        .contains(Paths.get("file://", basePath, "normal_dir/file2.txt").toString());
    assertThat(foundFiles)
        .contains(Paths.get("file://", basePath, "normal_dir/dep1/file3.txt").toString());
    assertThat(remainingDirs).isEmpty();
  }

  @Test
  public void testListDirRecursivelyWithHadoopMaxDepth() {
    List<String> foundFiles = new ArrayList<>();
    List<String> remainingDirs = new ArrayList<>();
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

    assertThat(foundFiles).hasSize(2);
    assertThat(foundFiles).contains(Paths.get("file://", basePath, "file1.txt").toString());
    assertThat(foundFiles)
        .contains(Paths.get("file://", basePath, "normal_dir/file2.txt").toString());
    assertThat(remainingDirs).hasSize(1);
    assertThat(remainingDirs)
        .contains(Paths.get("file://", basePath, "normal_dir/dep1").toString());
  }

  @Test
  public void testListDirRecursivelyWithFileIO() {
    List<String> foundFiles = new ArrayList<>();
    Predicate<FileInfo> fileFilter = fileInfo -> fileInfo.location().endsWith(".txt");
    FileSystemWalker.listDirRecursivelyWithFileIO(
        fileIO, basePath, specs, fileFilter, foundFiles::add);

    assertThat(foundFiles).hasSize(3);
    assertThat(foundFiles).contains(Paths.get("file://", basePath, "file1.txt").toString());
    assertThat(foundFiles)
        .contains(Paths.get("file://", basePath, "normal_dir/file2.txt").toString());
    assertThat(foundFiles)
        .contains(Paths.get("file://", basePath, "normal_dir/dep1/file3.txt").toString());
  }
}
