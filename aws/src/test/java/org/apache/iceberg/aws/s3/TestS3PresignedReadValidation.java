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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.Test;

class TestS3PresignedReadValidation {

  private static final Set<String> ALLOWED = Set.of("amazonaws.com", "amazonaws.com.cn");

  @Test
  void acceptsHttpsOnAllowedHostAndSubdomains() {
    assertThatCode(
            () ->
                S3PresignedReadValidation.checkTrustedHttpsUrl(
                    "https://bucket.s3.us-east-1.amazonaws.com/key?X-Amz-Signature=abc", ALLOWED))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                S3PresignedReadValidation.checkTrustedHttpsUrl(
                    "https://bucket.s3.cn-north-1.amazonaws.com.cn/key", ALLOWED))
        .doesNotThrowAnyException();
  }

  @Test
  void rejectsPlainHttp() {
    assertThatThrownBy(
            () ->
                S3PresignedReadValidation.checkTrustedHttpsUrl(
                    "http://bucket.s3.amazonaws.com/key", ALLOWED))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("only https is allowed");
  }

  @Test
  void rejectsUntrustedHost() {
    assertThatThrownBy(
            () -> S3PresignedReadValidation.checkTrustedHttpsUrl("https://evil.com/key", ALLOWED))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("evil.com");
  }

  @Test
  void rejectsLookAlikeHost() {
    // a look-alike must not match the amazonaws.com suffix at a non-dot boundary
    assertThatThrownBy(
            () ->
                S3PresignedReadValidation.checkTrustedHttpsUrl(
                    "https://evil-amazonaws.com/key", ALLOWED))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void rejectsUserInfoHost() {
    // the effective host is evil.com; amazonaws.com is only user info before '@'
    assertThatThrownBy(
            () ->
                S3PresignedReadValidation.checkTrustedHttpsUrl(
                    "https://bucket.s3.amazonaws.com@evil.com/key", ALLOWED))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("evil.com");
  }

  @Test
  void rejectsUrlWithoutHost() {
    assertThatThrownBy(
            () -> S3PresignedReadValidation.checkTrustedHttpsUrl("https:///key", ALLOWED))
        .isInstanceOf(ValidationException.class);
  }
}
