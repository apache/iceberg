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
package org.apache.iceberg.aliyun.oss;

import static com.aliyun.oss.internal.OSSUtils.OSS_RESOURCE_MANAGER;

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class TestOSSURI {
  @Test
  public void testUrlParsing() {
    String location = "oss://bucket/path/to/file";
    OSSURI uri = new OSSURI(location);

    Assert.assertEquals("bucket", uri.bucket());
    Assert.assertEquals("path/to/file", uri.key());
    Assert.assertEquals(location, uri.toString());
  }

  @Test
  public void testEncodedString() {
    String location = "oss://bucket/path%20to%20file";
    OSSURI uri = new OSSURI(location);

    Assert.assertEquals("bucket", uri.bucket());
    Assert.assertEquals("path%20to%20file", uri.key());
    Assert.assertEquals(location, uri.toString());
  }

  @Test
  public void invalidBucket() {
    AssertHelpers.assertThrows(
        "Invalid bucket",
        IllegalArgumentException.class,
        OSS_RESOURCE_MANAGER.getFormattedString("BucketNameInvalid", "test_bucket"),
        () -> new OSSURI("https://test_bucket/path/to/file"));
  }

  @Test
  public void missingKey() {
    AssertHelpers.assertThrows(
        "Missing key",
        ValidationException.class,
        "Missing key in OSS location",
        () -> new OSSURI("https://bucket/"));
  }

  @Test
  public void invalidKey() {
    AssertHelpers.assertThrows(
        "Invalid key",
        IllegalArgumentException.class,
        OSS_RESOURCE_MANAGER.getFormattedString("ObjectKeyInvalid", "\\path/to/file"),
        () -> new OSSURI("https://bucket/\\path/to/file"));
  }

  @Test
  public void relativePathing() {
    AssertHelpers.assertThrows(
        "Cannot use relative oss location.",
        ValidationException.class,
        "Invalid OSS location",
        () -> new OSSURI("/path/to/file"));
  }

  @Test
  public void invalidScheme() {
    AssertHelpers.assertThrows(
        "Only support scheme: oss/https",
        ValidationException.class,
        "Invalid scheme",
        () -> new OSSURI("invalid://bucket/"));
  }

  @Test
  public void testFragment() {
    String location = "oss://bucket/path/to/file#print";
    OSSURI uri = new OSSURI(location);

    Assert.assertEquals("bucket", uri.bucket());
    Assert.assertEquals("path/to/file", uri.key());
    Assert.assertEquals(location, uri.toString());
  }

  @Test
  public void testQueryAndFragment() {
    String location = "oss://bucket/path/to/file?query=foo#bar";
    OSSURI uri = new OSSURI(location);

    Assert.assertEquals("bucket", uri.bucket());
    Assert.assertEquals("path/to/file", uri.key());
    Assert.assertEquals(location, uri.toString());
  }

  @Test
  public void testValidSchemes() {
    for (String scheme : Lists.newArrayList("https", "oss")) {
      OSSURI uri = new OSSURI(scheme + "://bucket/path/to/file");
      Assert.assertEquals("bucket", uri.bucket());
      Assert.assertEquals("path/to/file", uri.key());
    }
  }
}
