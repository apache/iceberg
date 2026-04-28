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

public class TestRelativePathUtil {

  @Test
  public void testIsAbsolute() {
    assertThat(RelativePathUtil.isAbsolute("s3://bucket/table/data/file.parquet")).isTrue();
    assertThat(RelativePathUtil.isAbsolute("file:///tmp/table/data/file.parquet")).isTrue();
    assertThat(RelativePathUtil.isAbsolute("file:/tmp/table/data/file.parquet")).isTrue();
    assertThat(RelativePathUtil.isAbsolute("hdfs://namenode/table/data/file.parquet")).isTrue();
    assertThat(RelativePathUtil.isAbsolute("gs://bucket/table/data/file.parquet")).isTrue();
    assertThat(RelativePathUtil.isAbsolute("abfs://container@account/table/file.parquet")).isTrue();
  }

  @Test
  public void testIsAbsoluteWithRelativePaths() {
    assertThat(RelativePathUtil.isAbsolute("/metadata/file.parquet")).isFalse();
    assertThat(RelativePathUtil.isAbsolute("metadata/file.parquet")).isFalse();
    assertThat(RelativePathUtil.isAbsolute("/data/00000-0.parquet")).isFalse();
  }

  @Test
  public void testIsAbsoluteWithColonsInPathSegments() {
    assertThat(RelativePathUtil.isAbsolute("/data/partition=key:value/file.parquet")).isFalse();
    assertThat(RelativePathUtil.isAbsolute("dir/file:tag.parquet")).isFalse();
    assertThat(RelativePathUtil.isAbsolute("/metadata/snap-123:456.avro")).isFalse();
  }

  @Test
  public void testIsAbsoluteWithNull() {
    assertThat(RelativePathUtil.isAbsolute(null)).isFalse();
  }

  @Test
  public void testResolveRelativePaths() {
    String tableLocation = "s3://bucket/table";

    assertThat(RelativePathUtil.resolve("/metadata/file.parquet", tableLocation))
        .isEqualTo("s3://bucket/table/metadata/file.parquet");

    assertThat(RelativePathUtil.resolve("/data/00000-0.parquet", tableLocation))
        .isEqualTo("s3://bucket/table/data/00000-0.parquet");
  }

  @Test
  public void testResolveAbsolutePathsUnchanged() {
    String tableLocation = "s3://bucket/table";

    assertThat(RelativePathUtil.resolve("s3://other/bucket/file.parquet", tableLocation))
        .isEqualTo("s3://other/bucket/file.parquet");

    assertThat(RelativePathUtil.resolve("hdfs://namenode/path/file.parquet", tableLocation))
        .isEqualTo("hdfs://namenode/path/file.parquet");
  }

  @Test
  public void testResolveWithNullPath() {
    assertThat(RelativePathUtil.resolve(null, "s3://bucket/table")).isNull();
  }

  @Test
  public void testResolveWithNullTableLocation() {
    assertThat(RelativePathUtil.resolve("/metadata/file.parquet", null))
        .isEqualTo("/metadata/file.parquet");
  }

  @Test
  public void testResolveWithNonAbsoluteTableLocation() {
    assertThat(RelativePathUtil.resolve("/metadata/file.parquet", "/local/table"))
        .isEqualTo("/metadata/file.parquet");
  }

  @Test
  public void testResolveWithFileScheme() {
    assertThat(RelativePathUtil.resolve("/metadata/file.parquet", "file:///tmp/table"))
        .isEqualTo("file:///tmp/table/metadata/file.parquet");

    assertThat(RelativePathUtil.resolve("/metadata/file.parquet", "file:/tmp/table"))
        .isEqualTo("file:/tmp/table/metadata/file.parquet");
  }

  @Test
  public void testRelativize() {
    String tableLocation = "s3://bucket/table";

    assertThat(
            RelativePathUtil.relativize("s3://bucket/table/metadata/file.parquet", tableLocation))
        .isEqualTo("/metadata/file.parquet");

    assertThat(RelativePathUtil.relativize("s3://bucket/table/data/00000-0.parquet", tableLocation))
        .isEqualTo("/data/00000-0.parquet");
  }

  @Test
  public void testRelativizePathNotUnderTableLocation() {
    String tableLocation = "s3://bucket/table";

    assertThat(RelativePathUtil.relativize("s3://other/bucket/file.parquet", tableLocation))
        .isEqualTo("s3://other/bucket/file.parquet");
  }

  @Test
  public void testRelativizePathEqualToTableLocation() {
    String tableLocation = "s3://bucket/table";

    assertThat(RelativePathUtil.relativize("s3://bucket/table", tableLocation))
        .isEqualTo("s3://bucket/table");
  }

  @Test
  public void testRelativizeWithNullInputs() {
    assertThat(RelativePathUtil.relativize(null, "s3://bucket/table")).isNull();

    assertThat(RelativePathUtil.relativize("s3://bucket/table/metadata/file.parquet", null))
        .isEqualTo("s3://bucket/table/metadata/file.parquet");
  }

  @Test
  public void testRelativizeWithNonAbsoluteTableLocation() {
    assertThat(RelativePathUtil.relativize("s3://bucket/table/file.parquet", "/local/table"))
        .isEqualTo("s3://bucket/table/file.parquet");
  }

  @Test
  public void testRelativizeWithFileScheme() {
    assertThat(
            RelativePathUtil.relativize(
                "file:///tmp/table/metadata/file.parquet", "file:///tmp/table"))
        .isEqualTo("/metadata/file.parquet");

    assertThat(
            RelativePathUtil.relativize("file:/tmp/table/metadata/file.parquet", "file:/tmp/table"))
        .isEqualTo("/metadata/file.parquet");
  }

  @Test
  public void testRelativizeMismatchedFileSchemeNotRelativized() {
    // mixed file: variants are NOT relativized -- consistent URI forms are the caller's
    // responsibility
    assertThat(
            RelativePathUtil.relativize(
                "file:///tmp/table/metadata/file.parquet", "file:/tmp/table"))
        .isEqualTo("file:///tmp/table/metadata/file.parquet");

    assertThat(
            RelativePathUtil.relativize(
                "file:/tmp/table/metadata/file.parquet", "file:///tmp/table"))
        .isEqualTo("file:/tmp/table/metadata/file.parquet");
  }

  @Test
  public void testRelativizePrefixOverlap() {
    // table location is a prefix of the path but not a directory boundary
    String tableLocation = "s3://bucket/tab";

    assertThat(RelativePathUtil.relativize("s3://bucket/table/file.parquet", tableLocation))
        .isEqualTo("s3://bucket/table/file.parquet");
  }

  @Test
  public void testRelativizeResolveRoundTrip() {
    String tableLocation = "s3://bucket/table";
    String absolutePath = "s3://bucket/table/metadata/root-manifest.parquet";

    String relativized = RelativePathUtil.relativize(absolutePath, tableLocation);
    assertThat(relativized).isEqualTo("/metadata/root-manifest.parquet");

    String resolved = RelativePathUtil.resolve(relativized, tableLocation);
    assertThat(resolved).isEqualTo(absolutePath);
  }

  @Test
  public void testRelativizeResolveRoundTripWithFileScheme() {
    String tableLocation = "file:///tmp/warehouse/table";
    String absolutePath = "file:///tmp/warehouse/table/data/00000-0.parquet";

    String relativized = RelativePathUtil.relativize(absolutePath, tableLocation);
    assertThat(relativized).isEqualTo("/data/00000-0.parquet");

    String resolved = RelativePathUtil.resolve(relativized, tableLocation);
    assertThat(resolved).isEqualTo(absolutePath);
  }

  @Test
  public void testRelativizeResolveRoundTripWithHDFS() {
    String tableLocation = "hdfs://namenode:8020/warehouse/table";
    String absolutePath = "hdfs://namenode:8020/warehouse/table/metadata/snap-123.avro";

    String relativized = RelativePathUtil.relativize(absolutePath, tableLocation);
    assertThat(relativized).isEqualTo("/metadata/snap-123.avro");

    String resolved = RelativePathUtil.resolve(relativized, tableLocation);
    assertThat(resolved).isEqualTo(absolutePath);
  }

  @Test
  public void testResolveAlreadyAbsoluteIsNoOp() {
    String tableLocation = "s3://bucket/table";
    String absolutePath = "s3://bucket/table/metadata/file.parquet";

    assertThat(RelativePathUtil.resolve(absolutePath, tableLocation)).isEqualTo(absolutePath);
  }

  @Test
  public void testResolveWithTrailingSlashOnTableLocation() {
    assertThat(RelativePathUtil.resolve("/metadata/file.parquet", "s3://bucket/table/"))
        .isEqualTo("s3://bucket/table/metadata/file.parquet");
  }

  @Test
  public void testRelativizeWithTrailingSlashOnTableLocation() {
    assertThat(
            RelativePathUtil.relativize(
                "s3://bucket/table/metadata/file.parquet", "s3://bucket/table/"))
        .isEqualTo("/metadata/file.parquet");
  }

  @Test
  public void testRelativizeResolveRoundTripWithTrailingSlash() {
    String tableLocation = "s3://bucket/table/";
    String absolutePath = "s3://bucket/table/data/file.parquet";

    String relativized = RelativePathUtil.relativize(absolutePath, tableLocation);
    assertThat(relativized).isEqualTo("/data/file.parquet");

    String resolved = RelativePathUtil.resolve(relativized, tableLocation);
    assertThat(resolved).isEqualTo(absolutePath);
  }

  @Test
  public void testResolveRejectsRelativePathWithoutLeadingSlash() {
    assertThatThrownBy(() -> RelativePathUtil.resolve("metadata/file.parquet", "s3://bucket/table"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Relative path must start with /");
  }
}
