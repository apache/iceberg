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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

public class TestS3URI {

  @Test
  public void testLocationParsing() {
    String p1 = "s3://bucket/path/to/file";
    S3URI uri1 = new S3URI(p1);

    assertThat(uri1.bucket()).isEqualTo("bucket");
    assertThat(uri1.key()).isEqualTo("path/to/file");
    assertThat(uri1.toString()).isEqualTo(p1);
  }

  @Test
  public void testEncodedString() {
    String p1 = "s3://bucket/path%20to%20file";
    S3URI uri1 = new S3URI(p1);

    assertThat(uri1.bucket()).isEqualTo("bucket");
    assertThat(uri1.key()).isEqualTo("path%20to%20file");
    assertThat(uri1.toString()).isEqualTo(p1);
  }

  @Test
  public void testMissingScheme() {

    assertThatThrownBy(() -> new S3URI("/path/to/file"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Invalid S3 URI, cannot determine scheme: /path/to/file");
  }

  @Test
  public void testOnlyBucketNameLocation() {
    String p1 = "s3://bucket";
    S3URI url1 = new S3URI(p1);

    assertThat(url1.bucket()).isEqualTo("bucket");
    assertThat(url1.key()).isEqualTo("");
    assertThat(url1.toString()).isEqualTo(p1);
  }

  @Test
  public void testQueryAndFragment() {
    String p1 = "s3://bucket/path/to/file?query=foo#bar";
    S3URI uri1 = new S3URI(p1);

    assertThat(uri1.bucket()).isEqualTo("bucket");
    assertThat(uri1.key()).isEqualTo("path/to/file");
    assertThat(uri1.toString()).isEqualTo(p1);
  }

  @Test
  public void testValidSchemes() {
    for (String scheme : Lists.newArrayList("https", "s3", "s3a", "s3n", "gs")) {
      S3URI uri = new S3URI(scheme + "://bucket/path/to/file");
      assertThat(uri.bucket()).isEqualTo("bucket");
      assertThat(uri.key()).isEqualTo("path/to/file");
    }
  }

  @Test
  public void testS3URIWithBucketToAccessPointMapping() {
    String p1 = "s3://bucket/path/to/file?query=foo#bar";
    Map<String, String> bucketToAccessPointMapping = ImmutableMap.of("bucket", "access-point");
    S3URI uri1 = new S3URI(p1, bucketToAccessPointMapping);

    assertThat(uri1.bucket()).isEqualTo("access-point");
    assertThat(uri1.key()).isEqualTo("path/to/file");
    assertThat(uri1.toString()).isEqualTo(p1);
  }
}
