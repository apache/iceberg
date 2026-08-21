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
package org.apache.iceberg.aws.lakeformation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import org.apache.iceberg.aws.AwsProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsRequest;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsResponse;

@ExtendWith(MockitoExtension.class)
class TestLakeFormationCredentialsProvider {

  private static final String TABLE_ARN = "arn:aws:glue:eu-central-1:123456789012:table/db/table";

  @Mock private LakeFormationClient lakeFormationClient;

  private GetTemporaryGlueTableCredentialsResponse responseWithExpiry(
      String accessKeyId, Instant expiration) {
    return GetTemporaryGlueTableCredentialsResponse.builder()
        .accessKeyId(accessKeyId)
        .secretAccessKey("secret")
        .sessionToken("token")
        .expiration(expiration)
        .build();
  }

  // ---- caching enabled (default) ----

  @Test
  void cachingEnabledReturnsCredentials() {
    when(lakeFormationClient.getTemporaryGlueTableCredentials(
            any(GetTemporaryGlueTableCredentialsRequest.class)))
        .thenReturn(responseWithExpiry("AKID1", Instant.now().plus(Duration.ofHours(1))));

    LakeFormationAwsClientFactory.LakeFormationCredentialsProvider provider =
        new LakeFormationAwsClientFactory.LakeFormationCredentialsProvider(
            lakeFormationClient,
            TABLE_ARN,
            /* cacheEnabled= */ true,
            AwsProperties.LAKE_FORMATION_CACHE_REFRESH_LEAD_TIME_MS_DEFAULT);

    AwsSessionCredentials creds = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(creds.accessKeyId()).isEqualTo("AKID1");
  }

  @Test
  void cachingEnabledCallsLakeFormationOnlyOnce() {
    when(lakeFormationClient.getTemporaryGlueTableCredentials(
            any(GetTemporaryGlueTableCredentialsRequest.class)))
        .thenReturn(responseWithExpiry("AKID1", Instant.now().plus(Duration.ofHours(1))));

    LakeFormationAwsClientFactory.LakeFormationCredentialsProvider provider =
        new LakeFormationAwsClientFactory.LakeFormationCredentialsProvider(
            lakeFormationClient,
            TABLE_ARN,
            /* cacheEnabled= */ true,
            AwsProperties.LAKE_FORMATION_CACHE_REFRESH_LEAD_TIME_MS_DEFAULT);

    provider.resolveCredentials();
    provider.resolveCredentials();
    provider.resolveCredentials();

    verify(lakeFormationClient, times(1))
        .getTemporaryGlueTableCredentials(any(GetTemporaryGlueTableCredentialsRequest.class));
  }

  @Test
  void cachingEnabledRefreshesAfterStaleTime() throws InterruptedException {
    // Credential expires in 2s with a 1.5s lead time -> staleTime = now + 500ms.
    // After sleeping 700ms past construction the cache must be stale and refetch.
    Instant firstExpiry = Instant.now().plusMillis(2000);
    Instant secondExpiry = Instant.now().plus(Duration.ofHours(1));

    when(lakeFormationClient.getTemporaryGlueTableCredentials(
            any(GetTemporaryGlueTableCredentialsRequest.class)))
        .thenReturn(responseWithExpiry("AKID1", firstExpiry))
        .thenReturn(responseWithExpiry("AKID2", secondExpiry));

    LakeFormationAwsClientFactory.LakeFormationCredentialsProvider provider =
        new LakeFormationAwsClientFactory.LakeFormationCredentialsProvider(
            lakeFormationClient,
            TABLE_ARN,
            /* cacheEnabled= */ true,
            /* refreshLeadTimeMs= */ 1500L);

    AwsSessionCredentials first = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(first.accessKeyId()).isEqualTo("AKID1");

    // Sleep past staleTime (firstExpiry - 1500ms ~= now + 500ms); 700ms gives ample margin.
    Thread.sleep(700);

    AwsSessionCredentials refreshed = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(refreshed.accessKeyId()).isEqualTo("AKID2");

    verify(lakeFormationClient, times(2))
        .getTemporaryGlueTableCredentials(any(GetTemporaryGlueTableCredentialsRequest.class));
  }

  // ---- caching disabled ----

  @Test
  void cachingDisabledCallsLakeFormationEveryTime() {
    when(lakeFormationClient.getTemporaryGlueTableCredentials(
            any(GetTemporaryGlueTableCredentialsRequest.class)))
        .thenReturn(responseWithExpiry("AKID1", Instant.now().plus(Duration.ofHours(1))));

    LakeFormationAwsClientFactory.LakeFormationCredentialsProvider provider =
        new LakeFormationAwsClientFactory.LakeFormationCredentialsProvider(
            lakeFormationClient,
            TABLE_ARN,
            /* cacheEnabled= */ false,
            AwsProperties.LAKE_FORMATION_CACHE_REFRESH_LEAD_TIME_MS_DEFAULT);

    provider.resolveCredentials();
    provider.resolveCredentials();
    provider.resolveCredentials();

    verify(lakeFormationClient, times(3))
        .getTemporaryGlueTableCredentials(any(GetTemporaryGlueTableCredentialsRequest.class));
  }

  @Test
  void cachingDisabledReturnsCredentials() {
    when(lakeFormationClient.getTemporaryGlueTableCredentials(
            any(GetTemporaryGlueTableCredentialsRequest.class)))
        .thenReturn(responseWithExpiry("AKID1", Instant.now().plus(Duration.ofHours(1))));

    LakeFormationAwsClientFactory.LakeFormationCredentialsProvider provider =
        new LakeFormationAwsClientFactory.LakeFormationCredentialsProvider(
            lakeFormationClient,
            TABLE_ARN,
            /* cacheEnabled= */ false,
            AwsProperties.LAKE_FORMATION_CACHE_REFRESH_LEAD_TIME_MS_DEFAULT);

    AwsSessionCredentials creds = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(creds.accessKeyId()).isEqualTo("AKID1");
  }
}
