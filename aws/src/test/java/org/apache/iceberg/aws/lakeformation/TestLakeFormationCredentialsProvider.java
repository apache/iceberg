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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.aws.AwsProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsRequest;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsResponse;

@ExtendWith(MockitoExtension.class)
class TestLakeFormationCredentialsProvider {

  private static final String TABLE_ARN = "arn:aws:glue:eu-central-1:123456789012:table/db/table";
  private static final long CACHE_EXPIRATION_MS =
      TimeUnit.SECONDS.toMillis(
          AwsProperties.LAKE_FORMATION_CREDENTIAL_CACHE_EXPIRATION_SECONDS_DEFAULT);

  @Mock private LakeFormationClient lakeFormationClient;

  @AfterEach
  void clearCredentialCaches() {
    LakeFormationAwsClientFactory.clearCredentialCaches();
  }

  private GetTemporaryGlueTableCredentialsResponse responseWithExpiry(
      String accessKeyId, Instant expiration) {
    return GetTemporaryGlueTableCredentialsResponse.builder()
        .accessKeyId(accessKeyId)
        .secretAccessKey("secret")
        .sessionToken("token")
        .expiration(expiration)
        .build();
  }

  @Test
  void returnsLakeFormationCredentials() {
    when(lakeFormationClient.getTemporaryGlueTableCredentials(
            any(GetTemporaryGlueTableCredentialsRequest.class)))
        .thenReturn(responseWithExpiry("AKID1", Instant.now().plus(Duration.ofHours(1))));

    LakeFormationAwsClientFactory.LakeFormationCredentialsProvider provider =
        new LakeFormationAwsClientFactory.LakeFormationCredentialsProvider(
            lakeFormationClient, TABLE_ARN);

    AwsSessionCredentials creds = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(creds.accessKeyId()).isEqualTo("AKID1");
  }

  @Test
  void sharesCredentialsAcrossProvidersWithSameContext() {
    when(lakeFormationClient.getTemporaryGlueTableCredentials(
            any(GetTemporaryGlueTableCredentialsRequest.class)))
        .thenReturn(responseWithExpiry("AKID1", Instant.now().plus(Duration.ofHours(1))));

    AwsCredentialsProvider first =
        LakeFormationAwsClientFactory.cachedCredentialsProvider(
            TABLE_ARN, Map.of("role", "reader"), CACHE_EXPIRATION_MS, () -> lakeFormationClient);
    AwsCredentialsProvider second =
        LakeFormationAwsClientFactory.cachedCredentialsProvider(
            TABLE_ARN, Map.of("role", "reader"), CACHE_EXPIRATION_MS, () -> lakeFormationClient);

    first.resolveCredentials();
    second.resolveCredentials();

    verify(lakeFormationClient, times(1))
        .getTemporaryGlueTableCredentials(any(GetTemporaryGlueTableCredentialsRequest.class));
  }

  @Test
  void doesNotShareCredentialsAcrossContexts() {
    LakeFormationClient otherLakeFormationClient = mock(LakeFormationClient.class);
    when(lakeFormationClient.getTemporaryGlueTableCredentials(
            any(GetTemporaryGlueTableCredentialsRequest.class)))
        .thenReturn(responseWithExpiry("AKID1", Instant.now().plus(Duration.ofHours(1))));
    when(otherLakeFormationClient.getTemporaryGlueTableCredentials(
            any(GetTemporaryGlueTableCredentialsRequest.class)))
        .thenReturn(responseWithExpiry("AKID2", Instant.now().plus(Duration.ofHours(1))));

    AwsCredentialsProvider first =
        LakeFormationAwsClientFactory.cachedCredentialsProvider(
            TABLE_ARN, Map.of("role", "reader"), CACHE_EXPIRATION_MS, () -> lakeFormationClient);
    AwsCredentialsProvider second =
        LakeFormationAwsClientFactory.cachedCredentialsProvider(
            TABLE_ARN,
            Map.of("role", "writer"),
            CACHE_EXPIRATION_MS,
            () -> otherLakeFormationClient);

    AwsSessionCredentials firstCredentials = (AwsSessionCredentials) first.resolveCredentials();
    AwsSessionCredentials secondCredentials = (AwsSessionCredentials) second.resolveCredentials();

    assertThat(firstCredentials.accessKeyId()).isEqualTo("AKID1");
    assertThat(secondCredentials.accessKeyId()).isEqualTo("AKID2");
    verify(lakeFormationClient, times(1))
        .getTemporaryGlueTableCredentials(any(GetTemporaryGlueTableCredentialsRequest.class));
    verify(otherLakeFormationClient, times(1))
        .getTemporaryGlueTableCredentials(any(GetTemporaryGlueTableCredentialsRequest.class));
  }

  @Test
  void sharesCredentialsAcrossConcurrentProviders() throws Exception {
    CountDownLatch requestStarted = new CountDownLatch(1);
    CountDownLatch releaseRequest = new CountDownLatch(1);
    when(lakeFormationClient.getTemporaryGlueTableCredentials(
            any(GetTemporaryGlueTableCredentialsRequest.class)))
        .thenAnswer(
            ignored -> {
              requestStarted.countDown();
              assertThat(releaseRequest.await(10, TimeUnit.SECONDS)).isTrue();
              return responseWithExpiry("AKID1", Instant.now().plus(Duration.ofHours(1)));
            });

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      executor.submit(
          () ->
              LakeFormationAwsClientFactory.cachedCredentialsProvider(
                      TABLE_ARN,
                      Map.of("role", "reader"),
                      CACHE_EXPIRATION_MS,
                      () -> lakeFormationClient)
                  .resolveCredentials());
      assertThat(requestStarted.await(10, TimeUnit.SECONDS)).isTrue();
      executor.submit(
          () ->
              LakeFormationAwsClientFactory.cachedCredentialsProvider(
                      TABLE_ARN,
                      Map.of("role", "reader"),
                      CACHE_EXPIRATION_MS,
                      () -> lakeFormationClient)
                  .resolveCredentials());
      releaseRequest.countDown();
    } finally {
      executor.shutdown();
    }
    assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

    verify(lakeFormationClient, times(1))
        .getTemporaryGlueTableCredentials(any(GetTemporaryGlueTableCredentialsRequest.class));
  }

  @Test
  void directProviderDoesNotCacheCredentials() {
    when(lakeFormationClient.getTemporaryGlueTableCredentials(
            any(GetTemporaryGlueTableCredentialsRequest.class)))
        .thenReturn(responseWithExpiry("AKID1", Instant.now().plus(Duration.ofHours(1))));

    LakeFormationAwsClientFactory.LakeFormationCredentialsProvider provider =
        new LakeFormationAwsClientFactory.LakeFormationCredentialsProvider(
            lakeFormationClient, TABLE_ARN);

    provider.resolveCredentials();
    provider.resolveCredentials();

    verify(lakeFormationClient, times(2))
        .getTemporaryGlueTableCredentials(any(GetTemporaryGlueTableCredentialsRequest.class));
  }
}
