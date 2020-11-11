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

package org.apache.iceberg.aws.s3;

import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class S3URITest {

  @Test
  public void testLocationParsing() {
    String p1 = "s3://bucket/path/to/file";
    S3URI uri1 = new S3URI(p1);

    assertEquals("bucket", uri1.bucket());
    assertEquals("path/to/file", uri1.key());
    assertEquals(p1, uri1.toString());
  }

  @Test
  public void testEncodedString() {
    String p1 = "s3://bucket/path%20to%20file";
    S3URI uri1 = new S3URI(p1);

    assertEquals("bucket", uri1.bucket());
    assertEquals("path%20to%20file", uri1.key());
    assertEquals(p1, uri1.toString());
  }

  @Test
  public void missingKey() {
    assertThrows(ValidationException.class, () -> new S3URI("https://bucket/"));
  }

  @Test
  public void relativePathing() {
    assertThrows(ValidationException.class, () -> new S3URI("/path/to/file"));
  }

  @Test
  public void invalidScheme() {
    assertThrows(ValidationException.class, () -> new S3URI("http://bucket/"));
  }

  @Test
  public void testQueryAndFragment() {
    String p1 = "s3://bucket/path/to/file?query=foo#bar";
    S3URI uri1 = new S3URI(p1);

    assertEquals("bucket", uri1.bucket());
    assertEquals("path/to/file", uri1.key());
    assertEquals(p1, uri1.toString());
  }

  @Test
  public void testValidSchemes() {
    for (String scheme : Lists.newArrayList("https", "s3", "s3a", "s3n")) {
      S3URI uri = new S3URI(scheme + "://bucket/path/to/file");
      assertEquals("bucket", uri.bucket());
      assertEquals("path/to/file", uri.key());
    }
  }
}
