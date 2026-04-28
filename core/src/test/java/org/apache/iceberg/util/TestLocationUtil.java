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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class TestLocationUtil {

  @Test
  public void testStripTrailingSlash() {
    String pathWithoutTrailingSlash = "s3://bucket/db/tbl";
    assertThat(LocationUtil.stripTrailingSlash(pathWithoutTrailingSlash))
        .as("Should have no trailing slashes")
        .isEqualTo(pathWithoutTrailingSlash);

    String pathWithSingleTrailingSlash = pathWithoutTrailingSlash + "/";
    assertThat(LocationUtil.stripTrailingSlash(pathWithSingleTrailingSlash))
        .as("Should have no trailing slashes")
        .isEqualTo(pathWithoutTrailingSlash);

    String pathWithMultipleTrailingSlash = pathWithoutTrailingSlash + "////";
    assertThat(LocationUtil.stripTrailingSlash(pathWithMultipleTrailingSlash))
        .as("Should have no trailing slashes")
        .isEqualTo(pathWithoutTrailingSlash);

    String pathWithOnlySlash = "////";
    assertThat(LocationUtil.stripTrailingSlash(pathWithOnlySlash))
        .as("Should have no trailing slashes")
        .isEmpty();
  }

  @Test
  public void testStripTrailingSlashWithInvalidPath() {
    String[] invalidPaths = new String[] {null, ""};

    for (String invalidPath : invalidPaths) {
      assertThatThrownBy(() -> LocationUtil.stripTrailingSlash(invalidPath))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("path must not be null or empty");
    }
  }

  @Test
  void testDoNotStripTrailingSlashForRootPath() {
    String rootPath = "blobstore://";
    assertThat(LocationUtil.stripTrailingSlash(rootPath))
        .as("Should be root path")
        .isEqualTo(rootPath);
  }

  @Test
  void testStripTrailingSlashForRootPathWithTrailingSlash() {
    String rootPath = "blobstore://";
    String rootPathWithTrailingSlash = rootPath + "/";
    assertThat(LocationUtil.stripTrailingSlash(rootPathWithTrailingSlash))
        .as("Should be root path")
        .isEqualTo(rootPath);
  }

  @Test
  void testStripTrailingSlashForRootPathWithTrailingSlashes() {
    String rootPath = "blobstore://";
    String rootPathWithMultipleTrailingSlash = rootPath + "///";
    assertThat(LocationUtil.stripTrailingSlash(rootPathWithMultipleTrailingSlash))
        .as("Should be root path")
        .isEqualTo(rootPath);
  }

  @Test
  public void testIsAbsolute() {
    assertThat(LocationUtil.isAbsolute("s3://bucket/table/data/file.parquet")).isTrue();
    assertThat(LocationUtil.isAbsolute("file:///tmp/table/data/file.parquet")).isTrue();
    assertThat(LocationUtil.isAbsolute("file:/tmp/table/data/file.parquet")).isTrue();
    assertThat(LocationUtil.isAbsolute("hdfs://namenode/table/data/file.parquet")).isTrue();
    assertThat(LocationUtil.isAbsolute("/metadata/file.parquet")).isFalse();
    assertThat(LocationUtil.isAbsolute("metadata/file.parquet")).isFalse();
    assertThat(LocationUtil.isAbsolute(null)).isFalse();
  }

  @Test
  public void testResolve() {
    String tableLocation = "s3://bucket/table";

    // relative paths are resolved by direct concatenation
    assertThat(LocationUtil.resolve("/metadata/file.parquet", tableLocation))
        .isEqualTo("s3://bucket/table/metadata/file.parquet");

    // absolute paths are returned as-is
    assertThat(LocationUtil.resolve("s3://other/bucket/file.parquet", tableLocation))
        .isEqualTo("s3://other/bucket/file.parquet");

    // null tableLocation returns the path as-is
    assertThat(LocationUtil.resolve("/metadata/file.parquet", null))
        .isEqualTo("/metadata/file.parquet");
  }

  @Test
  public void testRelativize() {
    String tableLocation = "s3://bucket/table";

    // paths under the table location are relativized with leading /
    assertThat(LocationUtil.relativize("s3://bucket/table/metadata/file.parquet", tableLocation))
        .isEqualTo("/metadata/file.parquet");

    // paths not under the table location are returned as-is
    assertThat(LocationUtil.relativize("s3://other/bucket/file.parquet", tableLocation))
        .isEqualTo("s3://other/bucket/file.parquet");

    // null tableLocation returns the path as-is
    assertThat(LocationUtil.relativize("s3://bucket/table/metadata/file.parquet", null))
        .isEqualTo("s3://bucket/table/metadata/file.parquet");

    // path equal to table location (no trailing content) is returned as-is
    assertThat(LocationUtil.relativize("s3://bucket/table", tableLocation))
        .isEqualTo("s3://bucket/table");
  }

  @Test
  public void testRelativizeResolveRoundTrip() {
    String tableLocation = "s3://bucket/table";
    String absolutePath = "s3://bucket/table/metadata/root-manifest.parquet";

    String relativized = LocationUtil.relativize(absolutePath, tableLocation);
    assertThat(relativized).isEqualTo("/metadata/root-manifest.parquet");

    String resolved = LocationUtil.resolve(relativized, tableLocation);
    assertThat(resolved).isEqualTo(absolutePath);
  }
}
