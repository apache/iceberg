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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.aws.s3.signer.S3V4RestSignerClient;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

public class TestAwsProperties {
  @Test
  public void testS3FileIoSseCustom_mustHaveCustomKey() {
    Map<String, String> map = Maps.newHashMap();
    map.put(AwsProperties.S3FILEIO_SSE_TYPE, AwsProperties.S3FILEIO_SSE_TYPE_CUSTOM);

    Assertions.assertThatThrownBy(() -> new AwsProperties(map))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot initialize SSE-C S3FileIO with null encryption key");
  }

  @Test
  public void testS3FileIoSseCustom_mustHaveCustomMd5() {
    Map<String, String> map = Maps.newHashMap();
    map.put(AwsProperties.S3FILEIO_SSE_TYPE, AwsProperties.S3FILEIO_SSE_TYPE_CUSTOM);
    map.put(AwsProperties.S3FILEIO_SSE_KEY, "something");

    Assertions.assertThatThrownBy(() -> new AwsProperties(map))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot initialize SSE-C S3FileIO with null encryption key MD5");
  }

  @Test
  public void testS3FileIoAcl() {
    Map<String, String> map = Maps.newHashMap();
    map.put(AwsProperties.S3FILEIO_ACL, ObjectCannedACL.AUTHENTICATED_READ.toString());
    AwsProperties properties = new AwsProperties(map);
    Assertions.assertThat(properties.s3FileIoAcl()).isEqualTo(ObjectCannedACL.AUTHENTICATED_READ);
  }

  @Test
  public void testS3FileIoAcl_unknownType() {
    Map<String, String> map = Maps.newHashMap();
    map.put(AwsProperties.S3FILEIO_ACL, "bad-input");

    Assertions.assertThatThrownBy(() -> new AwsProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot support S3 CannedACL bad-input");
  }

  @Test
  public void testS3MultipartSizeTooSmall() {
    Map<String, String> map = Maps.newHashMap();
    map.put(AwsProperties.S3FILEIO_MULTIPART_SIZE, "1");

    Assertions.assertThatThrownBy(() -> new AwsProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Minimum multipart upload object size must be larger than 5 MB.");
  }

  @Test
  public void testS3MultipartSizeTooLarge() {
    Map<String, String> map = Maps.newHashMap();
    map.put(AwsProperties.S3FILEIO_MULTIPART_SIZE, "5368709120"); // 5GB

    Assertions.assertThatThrownBy(() -> new AwsProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Input malformed or exceeded maximum multipart upload size 5GB: 5368709120");
  }

  @Test
  public void testS3MultipartThresholdFactorLessThanOne() {
    Map<String, String> map = Maps.newHashMap();
    map.put(AwsProperties.S3FILEIO_MULTIPART_THRESHOLD_FACTOR, "0.9");

    Assertions.assertThatThrownBy(() -> new AwsProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Multipart threshold factor must be >= to 1.0");
  }

  @Test
  public void testS3FileIoDeleteBatchSizeTooLarge() {
    Map<String, String> map = Maps.newHashMap();
    map.put(AwsProperties.S3FILEIO_DELETE_BATCH_SIZE, "2000");

    Assertions.assertThatThrownBy(() -> new AwsProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Deletion batch size must be between 1 and 1000");
  }

  @Test
  public void testS3FileIoDeleteBatchSizeTooSmall() {
    Map<String, String> map = Maps.newHashMap();
    map.put(AwsProperties.S3FILEIO_DELETE_BATCH_SIZE, "0");

    Assertions.assertThatThrownBy(() -> new AwsProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Deletion batch size must be between 1 and 1000");
  }

  @Test
  public void testS3FileIoDefaultCredentialsConfiguration() {
    // set nothing
    Map<String, String> properties = Maps.newHashMap();
    AwsProperties awsProperties = new AwsProperties(properties);
    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);
    ArgumentCaptor<AwsCredentialsProvider> awsCredentialsProviderCaptor =
        ArgumentCaptor.forClass(AwsCredentialsProvider.class);

    awsProperties.applyS3CredentialConfigurations(mockS3ClientBuilder);
    Mockito.verify(mockS3ClientBuilder).credentialsProvider(awsCredentialsProviderCaptor.capture());
    AwsCredentialsProvider capturedAwsCredentialsProvider = awsCredentialsProviderCaptor.getValue();

    Assertions.assertThat(capturedAwsCredentialsProvider)
        .as("Should use default credentials if nothing is set")
        .isInstanceOf(DefaultCredentialsProvider.class);
  }

  @Test
  public void testS3FileIoBasicCredentialsConfiguration() {
    // set access key id and secret access key
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.S3FILEIO_ACCESS_KEY_ID, "key");
    properties.put(AwsProperties.S3FILEIO_SECRET_ACCESS_KEY, "secret");
    AwsProperties awsPropertiesTwoSet = new AwsProperties(properties);
    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);
    ArgumentCaptor<AwsCredentialsProvider> awsCredentialsProviderCaptor =
        ArgumentCaptor.forClass(AwsCredentialsProvider.class);

    awsPropertiesTwoSet.applyS3CredentialConfigurations(mockS3ClientBuilder);
    Mockito.verify(mockS3ClientBuilder).credentialsProvider(awsCredentialsProviderCaptor.capture());
    AwsCredentialsProvider capturedAwsCredentialsProvider = awsCredentialsProviderCaptor.getValue();

    Assertions.assertThat(capturedAwsCredentialsProvider.resolveCredentials())
        .as("Should use basic credentials if access key ID and secret access key are set")
        .isInstanceOf(AwsBasicCredentials.class);
    Assertions.assertThat(capturedAwsCredentialsProvider.resolveCredentials().accessKeyId())
        .as("The access key id should be the same as the one set by tag S3FILEIO_ACCESS_KEY_ID")
        .isEqualTo("key");
    Assertions.assertThat(capturedAwsCredentialsProvider.resolveCredentials().secretAccessKey())
        .as(
            "The secret access key should be the same as the one set by tag S3FILEIO_SECRET_ACCESS_KEY")
        .isEqualTo("secret");
  }

  @Test
  public void testS3FileIoSessionCredentialsConfiguration() {
    // set access key id, secret access key, and session token
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.S3FILEIO_ACCESS_KEY_ID, "key");
    properties.put(AwsProperties.S3FILEIO_SECRET_ACCESS_KEY, "secret");
    properties.put(AwsProperties.S3FILEIO_SESSION_TOKEN, "token");
    AwsProperties awsProperties = new AwsProperties(properties);
    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);
    ArgumentCaptor<AwsCredentialsProvider> awsCredentialsProviderCaptor =
        ArgumentCaptor.forClass(AwsCredentialsProvider.class);

    awsProperties.applyS3CredentialConfigurations(mockS3ClientBuilder);
    Mockito.verify(mockS3ClientBuilder).credentialsProvider(awsCredentialsProviderCaptor.capture());
    AwsCredentialsProvider capturedAwsCredentialsProvider = awsCredentialsProviderCaptor.getValue();

    Assertions.assertThat(capturedAwsCredentialsProvider.resolveCredentials())
        .as("Should use session credentials if session token is set")
        .isInstanceOf(AwsSessionCredentials.class);
    Assertions.assertThat(capturedAwsCredentialsProvider.resolveCredentials().accessKeyId())
        .as("The access key id should be the same as the one set by tag S3FILEIO_ACCESS_KEY_ID")
        .isEqualTo("key");
    Assertions.assertThat(capturedAwsCredentialsProvider.resolveCredentials().secretAccessKey())
        .as(
            "The secret access key should be the same as the one set by tag S3FILEIO_SECRET_ACCESS_KEY")
        .isEqualTo("secret");
  }

  @Test
  public void testUrlHttpClientConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_TYPE, "urlconnection");
    AwsProperties awsProperties = new AwsProperties(properties);
    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);
    ArgumentCaptor<SdkHttpClient.Builder> httpClientBuilderCaptor =
        ArgumentCaptor.forClass(SdkHttpClient.Builder.class);

    awsProperties.applyHttpClientConfigurations(mockS3ClientBuilder);
    Mockito.verify(mockS3ClientBuilder).httpClientBuilder(httpClientBuilderCaptor.capture());
    SdkHttpClient.Builder capturedHttpClientBuilder = httpClientBuilderCaptor.getValue();

    Assertions.assertThat(capturedHttpClientBuilder)
        .as("Should use url connection http client")
        .isInstanceOf(UrlConnectionHttpClient.Builder.class);
  }

  @Test
  public void testApacheHttpClientConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_TYPE, "apache");
    AwsProperties awsProperties = new AwsProperties(properties);
    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);
    ArgumentCaptor<SdkHttpClient.Builder> httpClientBuilderCaptor =
        ArgumentCaptor.forClass(SdkHttpClient.Builder.class);

    awsProperties.applyHttpClientConfigurations(mockS3ClientBuilder);
    Mockito.verify(mockS3ClientBuilder).httpClientBuilder(httpClientBuilderCaptor.capture());
    SdkHttpClient.Builder capturedHttpClientBuilder = httpClientBuilderCaptor.getValue();
    Assertions.assertThat(capturedHttpClientBuilder)
        .as("Should use apache http client")
        .isInstanceOf(ApacheHttpClient.Builder.class);
  }

  @Test
  public void testInvalidHttpClientType() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_TYPE, "test");
    AwsProperties awsProperties = new AwsProperties(properties);
    S3ClientBuilder s3ClientBuilder = S3Client.builder();

    Assertions.assertThatThrownBy(
            () -> awsProperties.applyHttpClientConfigurations(s3ClientBuilder))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unrecognized HTTP client type test");
  }

  @Test
  public void testKryoSerialization() throws IOException {
    AwsProperties awsProperties = new AwsProperties();
    AwsProperties deSerializedAwsProperties =
        TestHelpers.KryoHelpers.roundTripSerialize(awsProperties);
    Assertions.assertThat(deSerializedAwsProperties.s3BucketToAccessPointMapping())
        .isEqualTo(awsProperties.s3BucketToAccessPointMapping());
    Assertions.assertThat(deSerializedAwsProperties.httpClientProperties())
        .isEqualTo(awsProperties.httpClientProperties());

    AwsProperties awsPropertiesWithProps = new AwsProperties(ImmutableMap.of("a", "b"));
    AwsProperties deSerializedAwsPropertiesWithProps =
        TestHelpers.KryoHelpers.roundTripSerialize(awsPropertiesWithProps);
    Assertions.assertThat(deSerializedAwsPropertiesWithProps.s3BucketToAccessPointMapping())
        .isEqualTo(awsPropertiesWithProps.s3BucketToAccessPointMapping());
    Assertions.assertThat(deSerializedAwsPropertiesWithProps.httpClientProperties())
        .isEqualTo(awsProperties.httpClientProperties());

    AwsProperties awsPropertiesWithEmptyProps = new AwsProperties(Collections.emptyMap());
    AwsProperties deSerializedAwsPropertiesWithEmptyProps =
        TestHelpers.KryoHelpers.roundTripSerialize(awsPropertiesWithProps);
    Assertions.assertThat(deSerializedAwsPropertiesWithEmptyProps.s3BucketToAccessPointMapping())
        .isEqualTo(awsPropertiesWithEmptyProps.s3BucketToAccessPointMapping());
    Assertions.assertThat(deSerializedAwsPropertiesWithEmptyProps.httpClientProperties())
        .isEqualTo(awsProperties.httpClientProperties());
  }

  @Test
  public void testS3RemoteSignerWithoutUri() {
    Map<String, String> properties =
        ImmutableMap.of(AwsProperties.S3_REMOTE_SIGNING_ENABLED, "true");
    AwsProperties awsProperties = new AwsProperties(properties);

    Assertions.assertThatThrownBy(
            () -> awsProperties.applyS3SignerConfiguration(S3Client.builder()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("S3 signer service URI is required");
  }

  @Test
  public void testS3RemoteSigningEnabled() {
    String uri = "http://localhost:12345";
    Map<String, String> properties =
        ImmutableMap.of(
            AwsProperties.S3_REMOTE_SIGNING_ENABLED, "true", CatalogProperties.URI, uri);
    AwsProperties awsProperties = new AwsProperties(properties);
    S3ClientBuilder builder = S3Client.builder();

    awsProperties.applyS3SignerConfiguration(builder);

    Optional<Signer> signer =
        builder.overrideConfiguration().advancedOption(SdkAdvancedClientOption.SIGNER);
    Assertions.assertThat(signer).isPresent().get().isInstanceOf(S3V4RestSignerClient.class);
    S3V4RestSignerClient signerClient = (S3V4RestSignerClient) signer.get();
    Assertions.assertThat(signerClient.baseSignerUri()).isEqualTo(uri);
    Assertions.assertThat(signerClient.properties()).isEqualTo(properties);
  }

  @Test
  public void testS3RemoteSigningDisabled() {
    Map<String, String> properties =
        ImmutableMap.of(AwsProperties.S3_REMOTE_SIGNING_ENABLED, "false");
    AwsProperties awsProperties = new AwsProperties(properties);
    S3ClientBuilder builder = S3Client.builder();

    awsProperties.applyS3SignerConfiguration(builder);

    Optional<Signer> signer =
        builder.overrideConfiguration().advancedOption(SdkAdvancedClientOption.SIGNER);
    Assertions.assertThat(signer).isNotPresent();
  }
}
