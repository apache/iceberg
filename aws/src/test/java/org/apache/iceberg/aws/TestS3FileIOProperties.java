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
package org.apache.iceberg.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.aws.s3.signer.S3V4RestSignerClient;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

public class TestS3FileIOProperties {

  @Test
  public void testS3FileIoSseCustom_mustHaveCustomKey() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.SSE_TYPE, S3FileIOProperties.SSE_TYPE_CUSTOM);

    assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot initialize SSE-C S3FileIO with null encryption key");
  }

  @Test
  public void testS3FileIoSseCustom_mustHaveCustomMd5() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.SSE_TYPE, S3FileIOProperties.SSE_TYPE_CUSTOM);
    map.put(S3FileIOProperties.SSE_KEY, "something");

    assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot initialize SSE-C S3FileIO with null encryption key MD5");
  }

  @Test
  public void testS3FileIoAcl() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.ACL, ObjectCannedACL.AUTHENTICATED_READ.toString());
    S3FileIOProperties properties = new S3FileIOProperties(map);
    assertThat(properties.acl()).isEqualTo(ObjectCannedACL.AUTHENTICATED_READ);
  }

  @Test
  public void testS3FileIoAcl_unknownType() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.ACL, "bad-input");

    assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot support S3 CannedACL bad-input");
  }

  @Test
  public void testS3MultipartSizeTooSmall() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.MULTIPART_SIZE, "1");

    assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Minimum multipart upload object size must be larger than 5 MB.");
  }

  @Test
  public void testS3MultipartSizeTooLarge() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.MULTIPART_SIZE, "5368709120"); // 5GB

    assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Input malformed or exceeded maximum multipart upload size 5GB: 5368709120");
  }

  @Test
  public void testS3MultipartThresholdFactorLessThanOne() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.MULTIPART_THRESHOLD_FACTOR, "0.9");

    assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Multipart threshold factor must be >= to 1.0");
  }

  @Test
  public void testS3FileIoDeleteBatchSizeTooLarge() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.DELETE_BATCH_SIZE, "2000");

    assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Deletion batch size must be between 1 and 1000");
  }

  @Test
  public void testS3FileIoDeleteBatchSizeTooSmall() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.DELETE_BATCH_SIZE, "0");

    assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Deletion batch size must be between 1 and 1000");
  }

  @Test
  public void testS3FileIoDefaultCredentialsConfiguration() {
    // set nothing
    Map<String, String> properties = Maps.newHashMap();
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties(properties);
    AwsClientProperties awsClientProperties = new AwsClientProperties(properties);
    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);
    ArgumentCaptor<AwsCredentialsProvider> awsCredentialsProviderCaptor =
        ArgumentCaptor.forClass(AwsCredentialsProvider.class);

    s3FileIOProperties.applyCredentialConfigurations(awsClientProperties, mockS3ClientBuilder);
    Mockito.verify(mockS3ClientBuilder).credentialsProvider(awsCredentialsProviderCaptor.capture());
    AwsCredentialsProvider capturedAwsCredentialsProvider = awsCredentialsProviderCaptor.getValue();

    assertThat(capturedAwsCredentialsProvider)
        .as("Should use default credentials if nothing is set")
        .isInstanceOf(DefaultCredentialsProvider.class);
  }

  @Test
  public void testS3FileIoBasicCredentialsConfiguration() {
    // set access key id and secret access key
    Map<String, String> properties = Maps.newHashMap();
    properties.put(S3FileIOProperties.ACCESS_KEY_ID, "key");
    properties.put(S3FileIOProperties.SECRET_ACCESS_KEY, "secret");
    S3FileIOProperties s3PropertiesTwoSet = new S3FileIOProperties(properties);
    AwsClientProperties awsClientProperties = new AwsClientProperties(properties);
    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);
    ArgumentCaptor<AwsCredentialsProvider> awsCredentialsProviderCaptor =
        ArgumentCaptor.forClass(AwsCredentialsProvider.class);

    s3PropertiesTwoSet.applyCredentialConfigurations(awsClientProperties, mockS3ClientBuilder);
    Mockito.verify(mockS3ClientBuilder).credentialsProvider(awsCredentialsProviderCaptor.capture());
    AwsCredentialsProvider capturedAwsCredentialsProvider = awsCredentialsProviderCaptor.getValue();

    assertThat(capturedAwsCredentialsProvider.resolveCredentials())
        .as("Should use basic credentials if access key ID and secret access key are set")
        .isInstanceOf(AwsBasicCredentials.class);
    assertThat(capturedAwsCredentialsProvider.resolveCredentials().accessKeyId())
        .as("The access key id should be the same as the one set by tag S3FILEIO_ACCESS_KEY_ID")
        .isEqualTo("key");
    assertThat(capturedAwsCredentialsProvider.resolveCredentials().secretAccessKey())
        .as(
            "The secret access key should be the same as the one set by tag S3FILEIO_SECRET_ACCESS_KEY")
        .isEqualTo("secret");
  }

  @Test
  public void testS3FileIoSessionCredentialsConfiguration() {
    // set access key id, secret access key, and session token
    Map<String, String> properties = Maps.newHashMap();
    properties.put(S3FileIOProperties.ACCESS_KEY_ID, "key");
    properties.put(S3FileIOProperties.SECRET_ACCESS_KEY, "secret");
    properties.put(S3FileIOProperties.SESSION_TOKEN, "token");
    S3FileIOProperties s3Properties = new S3FileIOProperties(properties);
    AwsClientProperties awsClientProperties = new AwsClientProperties(properties);
    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);
    ArgumentCaptor<AwsCredentialsProvider> awsCredentialsProviderCaptor =
        ArgumentCaptor.forClass(AwsCredentialsProvider.class);

    s3Properties.applyCredentialConfigurations(awsClientProperties, mockS3ClientBuilder);
    Mockito.verify(mockS3ClientBuilder).credentialsProvider(awsCredentialsProviderCaptor.capture());
    AwsCredentialsProvider capturedAwsCredentialsProvider = awsCredentialsProviderCaptor.getValue();

    assertThat(capturedAwsCredentialsProvider.resolveCredentials())
        .as("Should use session credentials if session token is set")
        .isInstanceOf(AwsSessionCredentials.class);
    assertThat(capturedAwsCredentialsProvider.resolveCredentials().accessKeyId())
        .as("The access key id should be the same as the one set by tag S3FILEIO_ACCESS_KEY_ID")
        .isEqualTo("key");
    assertThat(capturedAwsCredentialsProvider.resolveCredentials().secretAccessKey())
        .as(
            "The secret access key should be the same as the one set by tag S3FILEIO_SECRET_ACCESS_KEY")
        .isEqualTo("secret");
  }

  @Test
  public void testS3RemoteSignerWithoutUri() {
    Map<String, String> properties =
        ImmutableMap.of(S3FileIOProperties.REMOTE_SIGNING_ENABLED, "true");
    S3FileIOProperties s3Properties = new S3FileIOProperties(properties);

    assertThatThrownBy(() -> s3Properties.applySignerConfiguration(S3Client.builder()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("S3 signer service URI is required");
  }

  @Test
  public void testS3RemoteSigningEnabled() {
    String uri = "http://localhost:12345";
    Map<String, String> properties =
        ImmutableMap.of(
            S3FileIOProperties.REMOTE_SIGNING_ENABLED, "true", CatalogProperties.URI, uri);
    S3FileIOProperties s3Properties = new S3FileIOProperties(properties);
    S3ClientBuilder builder = S3Client.builder();

    s3Properties.applySignerConfiguration(builder);

    Optional<Signer> signer =
        builder.overrideConfiguration().advancedOption(SdkAdvancedClientOption.SIGNER);
    assertThat(signer).isPresent().get().isInstanceOf(S3V4RestSignerClient.class);
    S3V4RestSignerClient signerClient = (S3V4RestSignerClient) signer.get();
    assertThat(signerClient.baseSignerUri()).isEqualTo(uri);
    assertThat(signerClient.properties()).isEqualTo(properties);
  }

  @Test
  public void s3RemoteSigningEnabledWithUserAgentAndRetryPolicy() {
    String uri = "http://localhost:12345";
    Map<String, String> properties =
        ImmutableMap.of(
            S3FileIOProperties.REMOTE_SIGNING_ENABLED, "true", CatalogProperties.URI, uri);
    S3FileIOProperties s3Properties = new S3FileIOProperties(properties);
    S3ClientBuilder builder = S3Client.builder();

    s3Properties.applySignerConfiguration(builder);
    s3Properties.applyUserAgentConfigurations(builder);
    s3Properties.applyRetryConfigurations(builder);

    Optional<String> userAgent =
        builder.overrideConfiguration().advancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX);
    assertThat(userAgent).isPresent().get().satisfies(x -> assertThat(x).startsWith("s3fileio"));

    Optional<Signer> signer =
        builder.overrideConfiguration().advancedOption(SdkAdvancedClientOption.SIGNER);
    assertThat(signer).isPresent().get().isInstanceOf(S3V4RestSignerClient.class);
    S3V4RestSignerClient signerClient = (S3V4RestSignerClient) signer.get();
    assertThat(signerClient.baseSignerUri()).isEqualTo(uri);
    assertThat(signerClient.properties()).isEqualTo(properties);

    Optional<RetryPolicy> retryPolicy = builder.overrideConfiguration().retryPolicy();
    assertThat(retryPolicy).isPresent().get().isInstanceOf(RetryPolicy.class);
  }

  @Test
  public void testS3RemoteSigningDisabled() {
    Map<String, String> properties =
        ImmutableMap.of(S3FileIOProperties.REMOTE_SIGNING_ENABLED, "false");
    S3FileIOProperties s3Properties = new S3FileIOProperties(properties);
    S3ClientBuilder builder = S3Client.builder();

    s3Properties.applySignerConfiguration(builder);

    Optional<Signer> signer =
        builder.overrideConfiguration().advancedOption(SdkAdvancedClientOption.SIGNER);
    assertThat(signer).isNotPresent();
  }

  @Test
  public void testS3AccessGrantsEnabled() {
    // Explicitly true
    Map<String, String> properties =
        ImmutableMap.of(S3FileIOProperties.S3_ACCESS_GRANTS_ENABLED, "true");
    S3FileIOProperties s3Properties = new S3FileIOProperties(properties);
    S3ClientBuilder builder = S3Client.builder();

    s3Properties.applyS3AccessGrantsConfigurations(builder);
    assertThat(builder.plugins().size()).isEqualTo(1);
  }

  @Test
  public void testS3AccessGrantsDisabled() {
    // Explicitly false
    Map<String, String> properties =
        ImmutableMap.of(S3FileIOProperties.S3_ACCESS_GRANTS_ENABLED, "false");
    S3FileIOProperties s3Properties = new S3FileIOProperties(properties);
    S3ClientBuilder builder = S3Client.builder();

    s3Properties.applyS3AccessGrantsConfigurations(builder);
    assertThat(builder.plugins().size()).isEqualTo(0);

    // Implicitly false
    properties = ImmutableMap.of();
    s3Properties = new S3FileIOProperties(properties);
    builder = S3Client.builder();

    s3Properties.applyS3AccessGrantsConfigurations(builder);
    assertThat(builder.plugins().size()).isEqualTo(0);
  }
}
