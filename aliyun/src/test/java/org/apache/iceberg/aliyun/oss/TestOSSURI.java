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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

public class TestOSSURI {
  @Test
  public void testUrlParsing() {
    String location = "oss://bucket/path/to/file";
    OSSURI uri = new OSSURI(location);

    assertThat(uri.bucket()).isEqualTo("bucket");
    assertThat(uri.key()).isEqualTo("path/to/file");
    assertThat(uri.toString()).isEqualTo(location);
  }

  @Test
  public void testEncodedString() {
    String location = "oss://bucket/path%20to%20file";
    OSSURI uri = new OSSURI(location);

    assertThat(uri.bucket()).isEqualTo("bucket");
    assertThat(uri.key()).isEqualTo("path%20to%20file");
    assertThat(uri.toString()).isEqualTo(location);
  }

  @Test
  public void invalidBucket() {

    assertThatThrownBy(() -> new OSSURI("https://test_bucket/path/to/file"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            OSS_RESOURCE_MANAGER.getFormattedString("BucketNameInvalid", "test_bucket"));
  }

  @Test
  public void missingKey() {

    assertThatThrownBy(() -> new OSSURI("https://bucket/"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Missing key in OSS location");
  }

  @Test
  public void invalidKey() {
    assertThatThrownBy(() -> new OSSURI("https://bucket/\\path/to/file"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            OSS_RESOURCE_MANAGER.getFormattedString("ObjectKeyInvalid", "\\path/to/file"));
  }

  @Test
  public void relativePathing() {

    assertThatThrownBy(() -> new OSSURI("/path/to/file"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid OSS location");
  }

  @Test
  public void invalidScheme() {

    assertThatThrownBy(() -> new OSSURI("invalid://bucket/"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid scheme");
  }

  @Test
  public void testFragment() {
    String location = "oss://bucket/path/to/file#print";
    OSSURI uri = new OSSURI(location);

    assertThat(uri.bucket()).isEqualTo("bucket");
    assertThat(uri.key()).isEqualTo("path/to/file");
    assertThat(uri.toString()).isEqualTo(location);
  }

  @Test
  public void testQueryAndFragment() {
    String location = "oss://bucket/path/to/file?query=foo#bar";
    OSSURI uri = new OSSURI(location);

    assertThat(uri.bucket()).isEqualTo("bucket");
    assertThat(uri.key()).isEqualTo("path/to/file");
    assertThat(uri.toString()).isEqualTo(location);
  }

  @Test
  public void testValidSchemes() {
    for (String scheme : Lists.newArrayList("https", "oss")) {
      OSSURI uri = new OSSURI(scheme + "://bucket/path/to/file");
      assertThat(uri.bucket()).isEqualTo("bucket");
      assertThat(uri.key()).isEqualTo("path/to/file");
    }
  }
}
