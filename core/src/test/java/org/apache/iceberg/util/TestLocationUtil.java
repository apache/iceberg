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
    String tableLocation = "s3://bucket/db/table";

    assertThat(LocationUtil.resolveLocation(tableLocation, "metadata/file.parquet"))
        .isEqualTo("s3://bucket/db/table/metadata/file.parquet");

    assertThat(LocationUtil.resolveLocation(tableLocation, "data/00000-0.parquet"))
        .isEqualTo("s3://bucket/db/table/data/00000-0.parquet");
  }

  @Test
  public void testResolveLocationsWithColonsInSegments() {
    String tableLocation = "s3://bucket/db/table";

    assertThat(LocationUtil.resolveLocation(tableLocation, "data/partition=key:value/file.parquet"))
        .isEqualTo("s3://bucket/db/table/data/partition=key:value/file.parquet");

    assertThat(LocationUtil.resolveLocation(tableLocation, "metadata/snap-123:456.avro"))
        .isEqualTo("s3://bucket/db/table/metadata/snap-123:456.avro");
  }

  @Test
  public void testResolveAbsoluteLocationsUnchanged() {
    String tableLocation = "s3://bucket/db/table";

    // different scheme (from the spec example table)
    assertThat(
            LocationUtil.resolveLocation(tableLocation, "hdfs://wh/db/table/data/00000-0.parquet"))
        .isEqualTo("hdfs://wh/db/table/data/00000-0.parquet");

    // different bucket
    assertThat(
            LocationUtil.resolveLocation(
                tableLocation, "s3://other-bucket/db/table/data/file.parquet"))
        .isEqualTo("s3://other-bucket/db/table/data/file.parquet");

    // same bucket, different path
    assertThat(
            LocationUtil.resolveLocation(
                tableLocation, "s3://bucket/db/other-table/data/file.parquet"))
        .isEqualTo("s3://bucket/db/other-table/data/file.parquet");
  }

  @Test
  public void testRelativize() {
    String tableLocation = "s3://bucket/db/table";

    assertThat(
            LocationUtil.relativizeLocation(
                tableLocation, "s3://bucket/db/table/metadata/file.parquet"))
        .isEqualTo("metadata/file.parquet");

    assertThat(
            LocationUtil.relativizeLocation(
                tableLocation, "s3://bucket/db/table/data/00000-0.parquet"))
        .isEqualTo("data/00000-0.parquet");
  }

  @Test
  public void testRelativizeLocationNotUnderTableLocation() {
    String tableLocation = "s3://bucket/db/table";

    // different bucket
    assertThat(
            LocationUtil.relativizeLocation(
                tableLocation, "s3://other-bucket/db/table/data/file.parquet"))
        .isEqualTo("s3://other-bucket/db/table/data/file.parquet");

    // same bucket, different path
    assertThat(
            LocationUtil.relativizeLocation(
                tableLocation, "s3://bucket/db/other-table/data/file.parquet"))
        .isEqualTo("s3://bucket/db/other-table/data/file.parquet");
  }

  @Test
  public void testRelativizeLocationWithSharedPrefix() {
    // sibling locations that share a byte prefix with the table location but are not
    // children of it must not be relativized (e.g. "table" vs "table_v2")
    String tableLocation = "s3://bucket/db/table";

    assertThat(
            LocationUtil.relativizeLocation(
                tableLocation, "s3://bucket/db/table_v2/data/00000-0.parquet"))
        .isEqualTo("s3://bucket/db/table_v2/data/00000-0.parquet");
  }

  @Test
  public void testRelativizeLocationEqualToTableLocation() {
    // a location equal to the table location is not followed by a separator,
    // so it is not a child of the table location and is returned as-is
    String tableLocation = "s3://bucket/db/table";

    assertThat(LocationUtil.relativizeLocation(tableLocation, tableLocation))
        .isEqualTo(tableLocation);
  }

  @Test
  public void testRelativizeMismatchedFileSchemeNotRelativized() {
    // mixed file: variants are NOT relativized. Consistent URI forms are the caller's
    // responsibility
    assertThat(
            LocationUtil.relativizeLocation(
                "file:/tmp/table", "file:///tmp/table/metadata/file.parquet"))
        .isEqualTo("file:///tmp/table/metadata/file.parquet");

    assertThat(
            LocationUtil.relativizeLocation(
                "file:///tmp/table", "file:/tmp/table/metadata/file.parquet"))
        .isEqualTo("file:/tmp/table/metadata/file.parquet");
  }

  @Test
  public void testResolveAbsoluteLocationWithNonAlphanumericScheme() {
    String tableLocation = "s3://bucket/db/table";

    assertThat(LocationUtil.resolveLocation(tableLocation, "git+ssh://host/repo"))
        .isEqualTo("git+ssh://host/repo");
  }

  @Test
  public void testResolveTreatsNonAsciiSchemeAsRelative() {
    // RFC 3986 restricts schemes to US-ASCII; a non-ASCII letter such as the Greek alpha
    // (U+03B1) is not a valid scheme character and the location is treated as relative
    String tableLocation = "s3://bucket/db/table";
    String location = "αscheme://host/path";

    assertThat(LocationUtil.resolveLocation(tableLocation, location))
        .isEqualTo(tableLocation + "/" + location);
  }

  @Test
  public void testResolveTreatsNonAlphaLeadingCharAsRelative() {
    // RFC 3986 section 3.1 requires the first scheme character to be ALPHA; locations
    // beginning with a digit or with '+'/'-'/'.' are treated as relative
    String tableLocation = "s3://bucket/db/table";

    assertThat(LocationUtil.resolveLocation(tableLocation, "3com://host"))
        .isEqualTo("s3://bucket/db/table/3com://host");

    assertThat(LocationUtil.resolveLocation(tableLocation, "+ssh://host"))
        .isEqualTo("s3://bucket/db/table/+ssh://host");

    assertThat(LocationUtil.resolveLocation(tableLocation, "-foo://host"))
        .isEqualTo("s3://bucket/db/table/-foo://host");

    assertThat(LocationUtil.resolveLocation(tableLocation, ".bar://host"))
        .isEqualTo("s3://bucket/db/table/.bar://host");
  }

  @Test
  public void testRelativizeResolveRoundTrip() {
    String tableLocation = "s3://bucket/db/table";
    String absoluteLocation = "s3://bucket/db/table/metadata/root-manifest.parquet";

    String relativized = LocationUtil.relativizeLocation(tableLocation, absoluteLocation);
    assertThat(relativized).isEqualTo("metadata/root-manifest.parquet");

    String resolved = LocationUtil.resolveLocation(tableLocation, relativized);
    assertThat(resolved).isEqualTo(absoluteLocation);
  }

  @Test
  public void testRelativizeResolveRoundTripWithFileScheme() {
    String tableLocation = "file:///tmp/warehouse/table";
    String absoluteLocation = "file:///tmp/warehouse/table/metadata/root-manifest.parquet";

    String relativized = LocationUtil.relativizeLocation(tableLocation, absoluteLocation);
    assertThat(relativized).isEqualTo("metadata/root-manifest.parquet");

    String resolved = LocationUtil.resolveLocation(tableLocation, relativized);
    assertThat(resolved).isEqualTo(absoluteLocation);
  }

  @Test
  public void testResolveWithTrailingOrLeadingSlashProducesDuplicateSeparator() {
    // the spec documents that joining a table location ending with '/' or a relative location
    // starting with '/' yields a duplicate '//'; callers are expected to avoid this
    assertThat(LocationUtil.resolveLocation("s3://bucket/db/table/", "data/00000-0.parquet"))
        .isEqualTo("s3://bucket/db/table//data/00000-0.parquet");

    assertThat(LocationUtil.resolveLocation("s3://bucket/db/table", "/data/00000-0.parquet"))
        .isEqualTo("s3://bucket/db/table//data/00000-0.parquet");
  }

  @Test
  public void testRelativizeWithTrailingSlashTableLocationNotRelativized() {
    // a trailing '/' on the table location prevents the prefix match because relativization
    // expects the separator to follow the prefix; the location is returned as-is
    assertThat(
            LocationUtil.relativizeLocation(
                "s3://bucket/db/table/", "s3://bucket/db/table/data/00000-0.parquet"))
        .isEqualTo("s3://bucket/db/table/data/00000-0.parquet");
  }

  @Test
  public void testRelativizeResolveRoundTripWithHDFS() {
    String tableLocation = "hdfs://namenode/warehouse/table";
    String absoluteLocation = "hdfs://namenode/warehouse/table/data/00000-0.parquet";

    String relativized = LocationUtil.relativizeLocation(tableLocation, absoluteLocation);
    assertThat(relativized).isEqualTo("data/00000-0.parquet");

    String resolved = LocationUtil.resolveLocation(tableLocation, relativized);
    assertThat(resolved).isEqualTo(absoluteLocation);
  }
}
