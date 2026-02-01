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

import org.junit.jupiter.api.Test;

public class TestRewriteTablePathUtil {

  @Test
  public void testStagingPathPreservesDirectoryStructure() {
    String sourcePrefix = "/source/table";
    String stagingDir = "/staging/";

    // Two files with same name but different paths
    String file1 = "/source/table/hash1/delete_0_0_0.parquet";
    String file2 = "/source/table/hash2/delete_0_0_0.parquet";

    String stagingPath1 = RewriteTablePathUtil.stagingPath(file1, sourcePrefix, stagingDir);
    String stagingPath2 = RewriteTablePathUtil.stagingPath(file2, sourcePrefix, stagingDir);

    // Should preserve directory structure to avoid conflicts
    assertThat(stagingPath1)
        .startsWith(stagingDir)
        .isEqualTo("/staging/hash1/delete_0_0_0.parquet")
        .isNotEqualTo(stagingPath2);
    assertThat(stagingPath2)
        .startsWith(stagingDir)
        .isEqualTo("/staging/hash2/delete_0_0_0.parquet");
  }

  @Test
  public void testStagingPathBackwardCompatibility() {
    // Test that the deprecated method still works
    String originalPath = "/some/path/file.parquet";
    String sourcePrefix = "/some/path";
    String stagingDir = "/staging/";

    String result = RewriteTablePathUtil.stagingPath(originalPath, sourcePrefix, stagingDir);

    assertThat(result).isEqualTo("/staging/file.parquet");
  }

  @Test
  public void testStagingPathWithComplexPaths() {
    String sourcePrefix = "/warehouse/db/table";
    String stagingDir = "/tmp/staging/";

    String filePath = "/warehouse/db/table/data/year=2023/month=01/part-00001.parquet";
    String result = RewriteTablePathUtil.stagingPath(filePath, sourcePrefix, stagingDir);

    assertThat(result).isEqualTo("/tmp/staging/data/year=2023/month=01/part-00001.parquet");
  }

  @Test
  public void testStagingPathWithNoMiddlePart() {
    // Test case where file is directly under source prefix (no middle directory structure)
    String sourcePrefix = "/source/table";
    String stagingDir = "/staging/";
    String fileDirectlyUnderPrefix = "/source/table/file.parquet";

    String newMethodResult =
        RewriteTablePathUtil.stagingPath(fileDirectlyUnderPrefix, sourcePrefix, stagingDir);
    assertThat(newMethodResult).isEqualTo("/staging/file.parquet");
  }

  @Test
  public void testRelativize() {
    // Normal case: path is under prefix
    assertThat(RewriteTablePathUtil.relativize("/a/b/c", "/a")).isEqualTo("b/c");
    assertThat(RewriteTablePathUtil.relativize("/a/b", "/a")).isEqualTo("b");

    // Edge case: path equals prefix exactly (issue #15172)
    assertThat(RewriteTablePathUtil.relativize("/a", "/a")).isEqualTo("");
    assertThat(RewriteTablePathUtil.relativize("s3://bucket/warehouse", "s3://bucket/warehouse"))
        .isEqualTo("");

    // Trailing separator variations - all combinations should work
    assertThat(RewriteTablePathUtil.relativize("/a/", "/a")).isEqualTo("");
    assertThat(RewriteTablePathUtil.relativize("/a/", "/a/")).isEqualTo("");
    assertThat(RewriteTablePathUtil.relativize("/a", "/a/")).isEqualTo("");
  }

  @Test
  public void testRelativizeInvalid() {
    // Path does not start with prefix
    assertThatThrownBy(() -> RewriteTablePathUtil.relativize("/other/path", "/source/table"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not start with");

    // Overlapping names: /table-old should NOT match prefix /table
    assertThatThrownBy(() -> RewriteTablePathUtil.relativize("/table-old/data", "/table"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not start with");
  }

  @Test
  public void testNewPath() {
    // Normal case: path is under prefix
    assertThat(RewriteTablePathUtil.newPath("/src/data/file.parquet", "/src", "/tgt"))
        .isEqualTo("/tgt/data/file.parquet");

    // Trailing separator on path
    assertThat(RewriteTablePathUtil.newPath("/src/data/", "/src", "/tgt")).isEqualTo("/tgt/data/");

    // Both path and prefix with trailing separator
    assertThat(RewriteTablePathUtil.newPath("/src/", "/src/", "/tgt")).isEqualTo("/tgt/");
  }

  @Test
  public void testNewPathEqualsPrefix() {
    // Issue #15172: path equals prefix (e.g., write.data.path = table location)
    // Result has trailing separator since directories are represented with trailing separators
    assertThat(RewriteTablePathUtil.newPath("/src", "/src", "/tgt")).isEqualTo("/tgt/");

    // S3 paths - storage migration scenario
    assertThat(
            RewriteTablePathUtil.newPath(
                "s3://bucket/warehouse/db/table",
                "s3://bucket/warehouse/db/table",
                "s3://bucket-dr/warehouse/db/table"))
        .isEqualTo("s3://bucket-dr/warehouse/db/table/");
  }

  @Test
  public void testNewPathTrailingSeparatorCombinations() {
    // All combinations of trailing separators should work consistently
    // Path equals prefix - all should map to target with trailing separator
    assertThat(RewriteTablePathUtil.newPath("/src", "/src", "/tgt")).isEqualTo("/tgt/");
    assertThat(RewriteTablePathUtil.newPath("/src/", "/src", "/tgt")).isEqualTo("/tgt/");
    assertThat(RewriteTablePathUtil.newPath("/src", "/src/", "/tgt")).isEqualTo("/tgt/");
    assertThat(RewriteTablePathUtil.newPath("/src/", "/src/", "/tgt")).isEqualTo("/tgt/");

    // Path under prefix - all should preserve relative structure
    assertThat(RewriteTablePathUtil.newPath("/src/data", "/src", "/tgt")).isEqualTo("/tgt/data");
    assertThat(RewriteTablePathUtil.newPath("/src/data", "/src/", "/tgt")).isEqualTo("/tgt/data");

    // Target with trailing separator
    assertThat(RewriteTablePathUtil.newPath("/src", "/src", "/tgt/")).isEqualTo("/tgt/");
    assertThat(RewriteTablePathUtil.newPath("/src/data", "/src", "/tgt/")).isEqualTo("/tgt/data");
  }

  @Test
  public void testNewPathInvalid() {
    // Path does not start with source prefix
    assertThatThrownBy(() -> RewriteTablePathUtil.newPath("/other/path", "/src", "/tgt"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not start with");

    // Overlapping names: /table-old should NOT match prefix /table
    assertThatThrownBy(() -> RewriteTablePathUtil.newPath("/table-old/data", "/table", "/tgt"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not start with");
  }

  @Test
  public void testNewPathBackupRestore() {
    // Backup: rewriting to a subdirectory of the original location
    assertThat(RewriteTablePathUtil.newPath("/table/data/file.parquet", "/table", "/table/backup"))
        .isEqualTo("/table/backup/data/file.parquet");
    assertThat(RewriteTablePathUtil.newPath("/table", "/table", "/table/backup"))
        .isEqualTo("/table/backup/");

    // Restore: rewriting from subdirectory to parent
    assertThat(
            RewriteTablePathUtil.newPath(
                "/table/backup/data/file.parquet", "/table/backup", "/table"))
        .isEqualTo("/table/data/file.parquet");
    assertThat(RewriteTablePathUtil.newPath("/table/backup", "/table/backup", "/table"))
        .isEqualTo("/table/");
  }

  @Test
  public void testNewPathTableRename() {
    // Rename /tableX to /table (target is substring of source name)
    assertThat(RewriteTablePathUtil.newPath("/tableX/data/file.parquet", "/tableX", "/table"))
        .isEqualTo("/table/data/file.parquet");
    assertThat(RewriteTablePathUtil.newPath("/tableX", "/tableX", "/table")).isEqualTo("/table/");
    assertThat(RewriteTablePathUtil.newPath("/tableX/metadata/v1.json", "/tableX", "/table"))
        .isEqualTo("/table/metadata/v1.json");

    // Rename /table to /tableX (source is substring of target name)
    assertThat(RewriteTablePathUtil.newPath("/table/data/file.parquet", "/table", "/tableX"))
        .isEqualTo("/tableX/data/file.parquet");
    assertThat(RewriteTablePathUtil.newPath("/table", "/table", "/tableX")).isEqualTo("/tableX/");
  }

  @Test
  public void testCombinePaths() {
    // Normal case
    assertThat(RewriteTablePathUtil.combinePaths("/base", "relative/path"))
        .isEqualTo("/base/relative/path");

    // Base already has trailing separator
    assertThat(RewriteTablePathUtil.combinePaths("/base/", "relative/path"))
        .isEqualTo("/base/relative/path");

    // Empty relative path (optimization added for issue #15172)
    assertThat(RewriteTablePathUtil.combinePaths("/base", "")).isEqualTo("/base/");
    assertThat(RewriteTablePathUtil.combinePaths("/base/", "")).isEqualTo("/base/");
  }

  @Test
  public void testFileName() {
    // Normal file paths
    assertThat(RewriteTablePathUtil.fileName("/path/to/file.parquet")).isEqualTo("file.parquet");
    assertThat(RewriteTablePathUtil.fileName("/a/b/c/data.json")).isEqualTo("data.json");

    // S3 paths
    assertThat(RewriteTablePathUtil.fileName("s3://bucket/warehouse/file.avro"))
        .isEqualTo("file.avro");

    // File directly at root
    assertThat(RewriteTablePathUtil.fileName("/file.txt")).isEqualTo("file.txt");

    // No separator (just filename)
    assertThat(RewriteTablePathUtil.fileName("file.parquet")).isEqualTo("file.parquet");
  }
}
