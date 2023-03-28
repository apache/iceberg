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

import org.apache.iceberg.AssertHelpers;
import org.junit.Assert;
import org.junit.Test;

public class TestLocationUtil {

  @Test
  public void testStripTrailingSlash() {
    String pathWithoutTrailingSlash = "s3://bucket/db/tbl";
    Assert.assertEquals(
        "Should have no trailing slashes",
        pathWithoutTrailingSlash,
        LocationUtil.stripTrailingSlash(pathWithoutTrailingSlash));

    String pathWithSingleTrailingSlash = pathWithoutTrailingSlash + "/";
    Assert.assertEquals(
        "Should have no trailing slashes",
        pathWithoutTrailingSlash,
        LocationUtil.stripTrailingSlash(pathWithSingleTrailingSlash));

    String pathWithMultipleTrailingSlash = pathWithoutTrailingSlash + "////";
    Assert.assertEquals(
        "Should have no trailing slashes",
        pathWithoutTrailingSlash,
        LocationUtil.stripTrailingSlash(pathWithMultipleTrailingSlash));

    String pathWithOnlySlash = "////";
    Assert.assertEquals(
        "Should have no trailing slashes", "", LocationUtil.stripTrailingSlash(pathWithOnlySlash));
  }

  @Test
  public void testStripTrailingSlashWithInvalidPath() {
    String[] invalidPaths = new String[] {null, ""};

    for (String invalidPath : invalidPaths) {
      AssertHelpers.assertThrows(
          "path must be valid",
          IllegalArgumentException.class,
          "path must not be null or empty",
          () -> LocationUtil.stripTrailingSlash(invalidPath));
    }
  }

  @Test
  public void testPosixNormalizePathsWithSchemeAndAuthorityS3Style() {
    Assert.assertEquals(
        "Must work with a root authority",
        "s3://bucket",
        LocationUtil.posixNormalize("s3://bucket"));
    Assert.assertEquals(
        "Must remove / from the end of authority",
        "s3://bucket",
        LocationUtil.posixNormalize("s3://bucket/"));
    Assert.assertEquals(
        "Must remove / from the end of directory",
        "s3://bucket/dir",
        LocationUtil.posixNormalize("s3://bucket/dir/"));
    Assert.assertEquals(
        "Must change // to / to enforce posix path",
        "s3://bucket/foo/bar",
        LocationUtil.posixNormalize("s3://bucket/foo//bar"));
    Assert.assertEquals(
        "Must resolve .. to previous directory to enforce posix path",
        "s3://bucket/bar",
        LocationUtil.posixNormalize("s3://bucket/foo/../bar"));
    Assert.assertEquals(
        "Must resolve . to current directory to enforce posix path",
        "s3://bucket/foo/bar",
        LocationUtil.posixNormalize("s3://bucket/foo/.//bar"));
  }

  @Test
  public void testPosixNormalizePathsWithSchemeAndAuthorityHdfsStyle() {
    Assert.assertEquals(
        "Must work with a root authority",
        "hdfs://1.2.3.4",
        LocationUtil.posixNormalize("hdfs://1.2.3.4"));
    Assert.assertEquals(
        "Must remove / from the end of authority",
        "hdfs://1.2.3.4",
        LocationUtil.posixNormalize("hdfs://1.2.3.4/"));
    Assert.assertEquals(
        "Must remove / from the end of directory",
        "hdfs://1.2.3.4/dir",
        LocationUtil.posixNormalize("hdfs://1.2.3.4/dir/"));
    Assert.assertEquals(
        "Must change // to / to enforce posix path",
        "hdfs://1.2.3.4/foo/bar",
        LocationUtil.posixNormalize("hdfs://1.2.3.4/foo//bar"));
    Assert.assertEquals(
        "Must resolve .. to previous directory to enforce posix path",
        "hdfs://1.2.3.4/bar",
        LocationUtil.posixNormalize("hdfs://1.2.3.4/foo/../bar"));
    Assert.assertEquals(
        "Must resolve . to current directory to enforce posix path",
        "hdfs://1.2.3.4/foo/bar",
        LocationUtil.posixNormalize("hdfs://1.2.3.4/foo/.//bar"));
  }

  @Test
  public void testPosixNormalizePathsWithSchemeAndWithoutAuthority() {
    Assert.assertEquals(
        "Must work with the root directory representation",
        "file:///",
        LocationUtil.posixNormalize("file:///"));
    Assert.assertEquals(
        "Must resolve all root directory representations to /",
        "file:///",
        LocationUtil.posixNormalize("file:////"));
    Assert.assertEquals(
        "Must resolve all root directory representations to / with subdirectory",
        "file:///dir",
        LocationUtil.posixNormalize("file:////dir"));
    Assert.assertEquals(
        "Must remove / from the end of directory",
        "file:///dir",
        LocationUtil.posixNormalize("file:///dir/"));
    Assert.assertEquals(
        "Must change // to / to enforce posix path",
        "file:///foo/bar",
        LocationUtil.posixNormalize("file:///foo//bar"));
    Assert.assertEquals(
        "Must resolve .. to previous directory to enforce posix path",
        "file:///bar",
        LocationUtil.posixNormalize("file:///foo/../bar"));
    Assert.assertEquals(
        "Must resolve . to current directory to enforce posix path",
        "file:///foo/bar",
        LocationUtil.posixNormalize("file:///foo/.//bar"));
  }

  @Test
  public void testPosixNormalizePathsWithoutScheme() {
    Assert.assertEquals(
        "Must work with the root directory representation", "/", LocationUtil.posixNormalize("/"));
    Assert.assertEquals(
        "Must resolve all root directory representations to /",
        "/",
        LocationUtil.posixNormalize("//"));
    Assert.assertEquals(
        "Must resolve all root directory representations to / with subdirectory",
        "/dir",
        LocationUtil.posixNormalize("//dir"));
    Assert.assertEquals(
        "Must remove / from the end of directory", "/dir", LocationUtil.posixNormalize("/dir/"));
    Assert.assertEquals(
        "Must change // to / to enforce posix path",
        "/foo/bar",
        LocationUtil.posixNormalize("/foo//bar"));
    Assert.assertEquals(
        "Must resolve .. to previous directory to enforce posix path",
        "/bar",
        LocationUtil.posixNormalize("/foo/../bar"));
    Assert.assertEquals(
        "Must resolve . to current directory to enforce posix path",
        "/foo/bar",
        LocationUtil.posixNormalize("/foo/.//bar"));
  }
}
