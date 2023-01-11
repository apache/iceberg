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
package org.apache.iceberg.huaweicloud.obs;

import static org.junit.Assert.assertEquals;

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class TestOBSURI {

  @Test
  public void testUrlParsing() {
    String location = "obs://bucket/path/to/file";
    OBSURI uri = new OBSURI(location);

    Assert.assertEquals("bucket", uri.bucket());
    Assert.assertEquals("path/to/file", uri.key());
    Assert.assertEquals(location, uri.toString());
  }

  @Test
  public void testEncodedString() {
    String location = "obs://bucket/path%20to%20file";
    OBSURI uri = new OBSURI(location);

    Assert.assertEquals("bucket", uri.bucket());
    Assert.assertEquals("path%20to%20file", uri.key());
    Assert.assertEquals(location, uri.toString());
  }

  @Test
  public void missingKey() {
    AssertHelpers.assertThrows(
        "Missing key",
        ValidationException.class,
        "Missing key in OBS location",
        () -> new OBSURI("https://bucket/"));
  }

  @Test
  public void relativePathing() {
    AssertHelpers.assertThrows(
        "Cannot use relative obs location.",
        ValidationException.class,
        "Invalid OBS location",
        () -> new OBSURI("/path/to/file"));
  }

  @Test
  public void invalidScheme() {
    AssertHelpers.assertThrows(
        "Only support scheme: obs/https",
        ValidationException.class,
        "Invalid scheme",
        () -> new OBSURI("invalid://bucket/"));
  }

  @Test
  public void testFragment() {
    String location = "obs://bucket/path/to/file#print";
    OBSURI uri = new OBSURI(location);

    Assert.assertEquals("bucket", uri.bucket());
    Assert.assertEquals("path/to/file", uri.key());
    Assert.assertEquals(location, uri.toString());
  }

  @Test
  public void testQueryAndFragment() {
    String location = "obs://bucket/path/to/file?query=foo#bar";
    OBSURI uri = new OBSURI(location);

    assertEquals("bucket", uri.bucket());
    assertEquals("path/to/file", uri.key());
    assertEquals(location, uri.toString());
  }

  @Test
  public void testValidSchemes() {
    for (String scheme : Lists.newArrayList("https", "obs")) {
      OBSURI uri = new OBSURI(scheme + "://bucket/path/to/file");
      assertEquals("bucket", uri.bucket());
      assertEquals("path/to/file", uri.key());
    }
  }
}
