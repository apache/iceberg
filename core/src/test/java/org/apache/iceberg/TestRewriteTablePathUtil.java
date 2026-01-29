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
  public void testRelativizePathUnderPrefix() {
    // Normal case: path is under prefix
    assertThat(RewriteTablePathUtil.relativize("/a/b/c", "/a")).isEqualTo("b/c");
    assertThat(RewriteTablePathUtil.relativize("/a/b", "/a")).isEqualTo("b");
    assertThat(RewriteTablePathUtil.relativize("/source/table/data/file.parquet", "/source/table"))
        .isEqualTo("data/file.parquet");
  }

  @Test
  public void testRelativizePathEqualsPrefix() {
    // Edge case: path equals prefix exactly (issue #15172)
    assertThat(RewriteTablePathUtil.relativize("/a", "/a")).isEqualTo("");
    assertThat(RewriteTablePathUtil.relativize("/source/table", "/source/table")).isEqualTo("");
    assertThat(RewriteTablePathUtil.relativize("s3://bucket/warehouse", "s3://bucket/warehouse"))
        .isEqualTo("");
  }

  @Test
  public void testRelativizePathEqualsPrefixWithTrailingSeparator() {
    // Edge case: path equals prefix with trailing separator on path
    assertThat(RewriteTablePathUtil.relativize("/a/", "/a")).isEqualTo("");
    assertThat(RewriteTablePathUtil.relativize("/source/table/", "/source/table")).isEqualTo("");

    // Edge case: prefix has trailing separator
    assertThat(RewriteTablePathUtil.relativize("/a", "/a/")).isEqualTo("");
    assertThat(RewriteTablePathUtil.relativize("/a/", "/a/")).isEqualTo("");
  }

  @Test
  public void testRelativizeInvalidPath() {
    // Error case: path does not start with prefix
    assertThatThrownBy(() -> RewriteTablePathUtil.relativize("/other/path", "/source/table"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not start with");
  }

  @Test
  public void testNewPathUnderPrefix() {
    // Normal case: path is under prefix
    assertThat(RewriteTablePathUtil.newPath("/src/data/file.parquet", "/src", "/tgt"))
        .isEqualTo("/tgt/data/file.parquet");
  }

  @Test
  public void testNewPathEqualsPrefix() {
    // Edge case: path equals prefix (issue #15172)
    // Result has trailing separator since directories are represented with trailing separators
    assertThat(RewriteTablePathUtil.newPath("/src", "/src", "/tgt")).isEqualTo("/tgt/");
    assertThat(
            RewriteTablePathUtil.newPath(
                "s3://bucket/warehouse", "s3://bucket/warehouse", "s3://bucket-dr/warehouse"))
        .isEqualTo("s3://bucket-dr/warehouse/");
  }

  @Test
  public void testNewPathTargetIsSubdirectoryOfSource() {
    // Rewriting to a subdirectory of the original location (e.g., backup scenario)
    assertThat(RewriteTablePathUtil.newPath("/table/data/file.parquet", "/table", "/table/backup"))
        .isEqualTo("/table/backup/data/file.parquet");
    // Edge case: rewriting root to subdirectory
    assertThat(RewriteTablePathUtil.newPath("/table", "/table", "/table/backup"))
        .isEqualTo("/table/backup/");
  }

  @Test
  public void testNewPathSourceIsSubdirectoryOfTarget() {
    // Rewriting from subdirectory to parent (e.g., restore from backup)
    assertThat(
            RewriteTablePathUtil.newPath(
                "/table/backup/data/file.parquet", "/table/backup", "/table"))
        .isEqualTo("/table/data/file.parquet");
    // Edge case: rewriting backup root to parent
    assertThat(RewriteTablePathUtil.newPath("/table/backup", "/table/backup", "/table"))
        .isEqualTo("/table/");
  }

  @Test
  public void testRelativizeRejectsOverlappingNames() {
    // Ensure /table-old is NOT matched by prefix /table (would be a bug)
    assertThatThrownBy(() -> RewriteTablePathUtil.relativize("/table-old/data", "/table"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not start with");
  }
}
