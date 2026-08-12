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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.rest.RESTCatalogInternalProperties;
import org.apache.iceberg.rest.signing.ImmutableRemoteSigningConfig;
import org.apache.iceberg.rest.signing.RemoteSigningConfig;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.S3Client;

class TestS3FileIORemoteSigningConfig {

  @Test
  void setRemoteSigningConfigNullThrows() {
    try (S3FileIO fileIO = new S3FileIO(() -> Mockito.mock(S3Client.class))) {
      assertThatThrownBy(() -> fileIO.setRemoteSigningConfig(null))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Invalid remote signing config: null");
    }
  }

  @Test
  void setAndGetRemoteSigningConfig() {
    RemoteSigningConfig config =
        ImmutableRemoteSigningConfig.builder()
            .putProperties("prop1", "val1")
            .putHeaders("Authorization", List.of("Bearer token"))
            .build();

    try (S3FileIO fileIO = new S3FileIO(() -> Mockito.mock(S3Client.class))) {
      fileIO.setRemoteSigningConfig(config);
      assertThat(fileIO.remoteSigningConfig()).isEqualTo(config);
    }
  }

  @Test
  void remoteSigningConfigInjectedWhenNonEmpty() {
    RemoteSigningConfig config =
        ImmutableRemoteSigningConfig.builder().putProperties("k", "v").build();

    try (S3FileIO fileIO = new S3FileIO(() -> Mockito.mock(S3Client.class))) {

      fileIO.setRemoteSigningConfig(config);
      fileIO.initialize(Map.of());

      PrefixedS3Client s3Client = fileIO.clientForStoragePath("s3://bucket/prefix/");

      assertThat(s3Client.s3FileIOProperties().properties())
          .containsKey(RESTCatalogInternalProperties.REMOTE_SIGNING_CONFIG);
    }
  }

  @Test
  void remoteSigningConfigNotInjectedWhenEmpty() {
    try (S3FileIO fileIO = new S3FileIO(() -> Mockito.mock(S3Client.class))) {

      fileIO.setRemoteSigningConfig(RemoteSigningConfig.EMPTY);
      fileIO.initialize(Map.of());

      PrefixedS3Client s3Client = fileIO.clientForStoragePath("s3://bucket/prefix/");

      assertThat(s3Client.s3FileIOProperties().properties())
          .doesNotContainKey(RESTCatalogInternalProperties.REMOTE_SIGNING_CONFIG);
    }
  }
}
