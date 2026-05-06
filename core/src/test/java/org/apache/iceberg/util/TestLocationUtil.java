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
  public void testResolveRelativeLocations() {
    String tableLocation = "s3://bucket/table";

    assertThat(LocationUtil.resolveLocation("/metadata/file.parquet", tableLocation))
        .isEqualTo("s3://bucket/table/metadata/file.parquet");

    assertThat(LocationUtil.resolveLocation("/data/00000-0.parquet", tableLocation))
        .isEqualTo("s3://bucket/table/data/00000-0.parquet");
  }

  @Test
  public void testResolveLocationsWithColonsInSegments() {
    String tableLocation = "s3://bucket/table";

    assertThat(
            LocationUtil.resolveLocation("/data/partition=key:value/file.parquet", tableLocation))
        .isEqualTo("s3://bucket/table/data/partition=key:value/file.parquet");

    assertThat(LocationUtil.resolveLocation("/metadata/snap-123:456.avro", tableLocation))
        .isEqualTo("s3://bucket/table/metadata/snap-123:456.avro");
  }

  @Test
  public void testResolveAbsoluteLocationsUnchanged() {
    String tableLocation = "s3://bucket/table";

    assertThat(LocationUtil.resolveLocation("s3://other/bucket/file.parquet", tableLocation))
        .isEqualTo("s3://other/bucket/file.parquet");

    assertThat(LocationUtil.resolveLocation("hdfs://namenode/path/file.parquet", tableLocation))
        .isEqualTo("hdfs://namenode/path/file.parquet");
  }

  @Test
  public void testResolveWithNullLocation() {
    assertThat(LocationUtil.resolveLocation(null, "s3://bucket/table")).isNull();
  }

  @Test
  public void testResolveWithNullTableLocation() {
    assertThatThrownBy(() -> LocationUtil.resolveLocation("/metadata/file.parquet", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Table location must not be null");
  }

  @Test
  public void testResolveWithFileScheme() {
    assertThat(LocationUtil.resolveLocation("/metadata/file.parquet", "file:///tmp/table"))
        .isEqualTo("file:///tmp/table/metadata/file.parquet");

    assertThat(LocationUtil.resolveLocation("/metadata/file.parquet", "file:/tmp/table"))
        .isEqualTo("file:/tmp/table/metadata/file.parquet");
  }

  @Test
  public void testRelativize() {
    String tableLocation = "s3://bucket/table";

    assertThat(
            LocationUtil.relativizeLocation(
                "s3://bucket/table/metadata/file.parquet", tableLocation))
        .isEqualTo("/metadata/file.parquet");

    assertThat(
            LocationUtil.relativizeLocation(
                "s3://bucket/table/data/00000-0.parquet", tableLocation))
        .isEqualTo("/data/00000-0.parquet");
  }

  @Test
  public void testRelativizeLocationNotUnderTableLocation() {
    String tableLocation = "s3://bucket/table";

    assertThat(LocationUtil.relativizeLocation("s3://other/bucket/file.parquet", tableLocation))
        .isEqualTo("s3://other/bucket/file.parquet");
  }

  @Test
  public void testRelativizeLocationEqualToTableLocation() {
    String tableLocation = "s3://bucket/table";

    assertThat(LocationUtil.relativizeLocation("s3://bucket/table", tableLocation)).isEqualTo("");
  }

  @Test
  public void testRelativizeWithNullLocation() {
    assertThat(LocationUtil.relativizeLocation(null, "s3://bucket/table")).isNull();
  }

  @Test
  public void testRelativizeWithNullTableLocation() {
    assertThatThrownBy(
            () -> LocationUtil.relativizeLocation("s3://bucket/table/metadata/file.parquet", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Table location must not be null");
  }

  @Test
  public void testRelativizeWithFileScheme() {
    assertThat(
            LocationUtil.relativizeLocation(
                "file:///tmp/table/metadata/file.parquet", "file:///tmp/table"))
        .isEqualTo("/metadata/file.parquet");

    assertThat(
            LocationUtil.relativizeLocation(
                "file:/tmp/table/metadata/file.parquet", "file:/tmp/table"))
        .isEqualTo("/metadata/file.parquet");
  }

  @Test
  public void testRelativizeMismatchedFileSchemeNotRelativized() {
    // mixed file: variants are NOT relativized. Consistent URI forms are the caller's
    // responsibility
    assertThat(
            LocationUtil.relativizeLocation(
                "file:///tmp/table/metadata/file.parquet", "file:/tmp/table"))
        .isEqualTo("file:///tmp/table/metadata/file.parquet");

    assertThat(
            LocationUtil.relativizeLocation(
                "file:/tmp/table/metadata/file.parquet", "file:///tmp/table"))
        .isEqualTo("file:/tmp/table/metadata/file.parquet");
  }

  @Test
  public void testRelativizeResolveRoundTrip() {
    String tableLocation = "s3://bucket/table";
    String absoluteLocation = "s3://bucket/table/metadata/root-manifest.parquet";

    String relativized = LocationUtil.relativizeLocation(absoluteLocation, tableLocation);
    assertThat(relativized).isEqualTo("/metadata/root-manifest.parquet");

    String resolved = LocationUtil.resolveLocation(relativized, tableLocation);
    assertThat(resolved).isEqualTo(absoluteLocation);
  }

  @Test
  public void testRelativizeResolveRoundTripWithFileScheme() {
    String tableLocation = "file:///tmp/warehouse/table";
    String absoluteLocation = "file:///tmp/warehouse/table/data/00000-0.parquet";

    String relativized = LocationUtil.relativizeLocation(absoluteLocation, tableLocation);
    assertThat(relativized).isEqualTo("/data/00000-0.parquet");

    String resolved = LocationUtil.resolveLocation(relativized, tableLocation);
    assertThat(resolved).isEqualTo(absoluteLocation);
  }

  @Test
  public void testRelativizeResolveRoundTripWithHDFS() {
    String tableLocation = "hdfs://namenode:8020/warehouse/table";
    String absoluteLocation = "hdfs://namenode:8020/warehouse/table/metadata/snap-123.avro";

    String relativized = LocationUtil.relativizeLocation(absoluteLocation, tableLocation);
    assertThat(relativized).isEqualTo("/metadata/snap-123.avro");

    String resolved = LocationUtil.resolveLocation(relativized, tableLocation);
    assertThat(resolved).isEqualTo(absoluteLocation);
  }
}
