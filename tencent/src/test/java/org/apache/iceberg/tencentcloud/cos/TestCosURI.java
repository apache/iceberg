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
package org.apache.iceberg.tencentcloud.cos;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCosURI {
  @Test
  public void testUrlParsing() {
    String location = "cos://bucket/path/to/file/";
    CosURI uri = new CosURI(location);

    assertThat(uri.bucket()).isEqualTo("bucket");
    assertThat(uri.key()).isEqualTo("/path/to/file");
    assertThat(uri.toString()).isEqualTo(location);
  }

  @Test
  public void testEncodedString() {
    String location = "cos://bucket/path%20to%20file";
    CosURI uri = new CosURI(location);

    assertThat(uri.bucket()).isEqualTo("bucket");
    assertThat(uri.key()).isEqualTo("/path%20to%20file");
    assertThat(uri.toString()).isEqualTo(location);
  }

  @Test
  public void invalidBucket() {
    Assertions.assertThatThrownBy(() -> new CosURI("cos://test_bucket/path/to/file"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Bucket name only should contain lowercase characters, num and -");
  }

  @Test
  public void missingKey() {
    Assertions.assertThatThrownBy(() -> new CosURI("cos://bucket/"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Missing key in Cos location");
  }

  @Test
  public void relativePathing() {
    Assertions.assertThatThrownBy(() -> new CosURI("/path/to/file"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid Cos location");
  }

  @Test
  public void invalidScheme() {
    Assertions.assertThatThrownBy(() -> new CosURI("invalid://bucket/"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid scheme");
  }

  @Test
  public void testFragment() {
    String location = "cos://bucket/path/to/file#print";
    CosURI uri = new CosURI(location);

    assertThat(uri.bucket()).isEqualTo("bucket");
    assertThat(uri.key()).isEqualTo("/path/to/file");
    assertThat(uri.location()).isEqualTo(location);
    assertThat(uri.toString()).isEqualTo(location);
  }

  @Test
  public void testQueryAndFragment() {
    String location = "cos://bucket/path/to/file?query=foo#bar";
    CosURI uri = new CosURI(location);

    assertThat(uri.bucket()).isEqualTo("bucket");
    assertThat(uri.key()).isEqualTo("/path/to/file");
    assertThat(uri.location()).isEqualTo(location);
  }

  @Test
  public void testValidSchemes() {
    for (String scheme : Lists.newArrayList("cos")) {
      CosURI uri = new CosURI(scheme + "://bucket/path/to/file");

      assertThat(uri.bucket()).isEqualTo("bucket");
      assertThat(uri.key()).isEqualTo("/path/to/file");
    }
  }
}
