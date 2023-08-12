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
package org.apache.iceberg.gcp.gcs;

import org.apache.iceberg.exceptions.ValidationException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class GCSLocationTest {
  @Test
  public void testLocationParsing() {
    String p1 = "gs://bucket/path/to/prefix";
    GCSLocation location = new GCSLocation(p1);

    Assertions.assertThat(location.bucket()).isEqualTo("bucket");
    Assertions.assertThat(location.prefix()).isEqualTo("path/to/prefix");
  }

  @Test
  public void testEncodedString() {
    String p1 = "gs://bucket/path%20to%20prefix";
    GCSLocation location = new GCSLocation(p1);

    Assertions.assertThat(location.bucket()).isEqualTo("bucket");
    Assertions.assertThat(location.prefix()).isEqualTo("path%20to%20prefix");
  }

  @Test
  public void testMissingScheme() {
    Assertions.assertThatThrownBy(() -> new GCSLocation("/path/to/prefix"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Invalid GCS URI, cannot determine scheme: /path/to/prefix");
  }

  @Test
  public void testInvalidScheme() {
    Assertions.assertThatThrownBy(() -> new GCSLocation("s3://bucket/path/to/prefix"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Invalid GCS URI, invalid scheme: s3");
  }

  @Test
  public void testOnlyBucketNameLocation() {
    String p1 = "gs://bucket";
    GCSLocation location = new GCSLocation(p1);

    Assertions.assertThat(location.bucket()).isEqualTo("bucket");
    Assertions.assertThat(location.prefix()).isEqualTo("");
  }

  @Test
  public void testQueryAndFragment() {
    String p1 = "gs://bucket/path/to/prefix?query=foo#bar";
    GCSLocation location = new GCSLocation(p1);

    Assertions.assertThat(location.bucket()).isEqualTo("bucket");
    Assertions.assertThat(location.prefix()).isEqualTo("path/to/prefix");
  }
}
