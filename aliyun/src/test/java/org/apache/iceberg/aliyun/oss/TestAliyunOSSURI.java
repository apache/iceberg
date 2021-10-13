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

import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class TestAliyunOSSURI {
  @Test
  public void testUrlParsing() {
    String location = "oss://bucket/path/to/file";
    AliyunOSSURI uri = new AliyunOSSURI(location);

    Assert.assertEquals("bucket", uri.getBucket());
    Assert.assertEquals("path/to/file", uri.getKey());
    Assert.assertEquals(location, uri.toString());
  }

  @Test
  public void testEncodedString() {
    String location = "oss://bucket/path%20to%20file";
    AliyunOSSURI uri = new AliyunOSSURI(location);

    Assert.assertEquals("bucket", uri.getBucket());
    Assert.assertEquals("path%20to%20file", uri.getKey());
    Assert.assertEquals(location, uri.toString());
  }

  @Test
  public void invalidOBucket() {
    Assert.assertThrows("Invalid bucket", IllegalArgumentException.class,
            () -> new AliyunOSSURI("https://test_bucket/path/to/file"));
  }

  @Test
  public void missingKey() {
    Assert.assertThrows("Missing key", ValidationException.class,
            () -> new AliyunOSSURI("https://bucket/"));
  }

  @Test
  public void invalidKey() {
    Assert.assertThrows("Invalid key", IllegalArgumentException.class,
            () -> new AliyunOSSURI("https://bucket/\\path/to/file"));
  }

  @Test
  public void relativePathing() {
    Assert.assertThrows("Cannot use relative oss path.", ValidationException.class,
        () -> new AliyunOSSURI("/path/to/file"));
  }

  @Test
  public void invalidScheme() {
    Assert.assertThrows("Invalid schema", ValidationException.class, () -> new AliyunOSSURI("invalid://bucket/"));
  }

  @Test
  public void testFragment() {
    String location = "oss://bucket/path/to/file#print";
    AliyunOSSURI uri = new AliyunOSSURI(location);

    Assert.assertEquals("bucket", uri.getBucket());
    Assert.assertEquals("path/to/file", uri.getKey());
    Assert.assertEquals(location, uri.toString());
  }

  @Test
  public void testQueryAndFragment() {
    String location = "oss://bucket/path/to/file?query=foo#bar";
    AliyunOSSURI uri = new AliyunOSSURI(location);

    Assert.assertEquals("bucket", uri.getBucket());
    Assert.assertEquals("path/to/file", uri.getKey());
    Assert.assertEquals(location, uri.toString());
  }

  @Test
  public void testValidSchemes() {
    for (String scheme : Lists.newArrayList("https", "oss")) {
      AliyunOSSURI uri = new AliyunOSSURI(scheme + "://bucket/path/to/file");
      Assert.assertEquals("bucket", uri.getBucket());
      Assert.assertEquals("path/to/file", uri.getKey());
    }
  }
}
