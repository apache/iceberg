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
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.aws.s3.signer.S3V4RestSignerClient;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
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
    AssertHelpers.assertThrows(
        "must have key for SSE-C",
        NullPointerException.class,
        "Cannot initialize SSE-C S3FileIO with null encryption key",
        () -> new AwsProperties(map));
  }

  @Test
  public void testS3FileIoSseCustom_mustHaveCustomMd5() {
    Map<String, String> map = Maps.newHashMap();
    map.put(AwsProperties.S3FILEIO_SSE_TYPE, AwsProperties.S3FILEIO_SSE_TYPE_CUSTOM);
    map.put(AwsProperties.S3FILEIO_SSE_KEY, "something");
    AssertHelpers.assertThrows(
        "must have md5 for SSE-C",
        NullPointerException.class,
        "Cannot initialize SSE-C S3FileIO with null encryption key MD5",
        () -> new AwsProperties(map));
  }

  @Test
  public void testS3FileIoAcl() {
    Map<String, String> map = Maps.newHashMap();
    map.put(AwsProperties.S3FILEIO_ACL, ObjectCannedACL.AUTHENTICATED_READ.toString());
    AwsProperties properties = new AwsProperties(map);
    Assert.assertEquals(ObjectCannedACL.AUTHENTICATED_READ, properties.s3FileIoAcl());
  }

  @Test
  public void testS3FileIoAcl_unknownType() {
    Map<String, String> map = Maps.newHashMap();
    map.put(AwsProperties.S3FILEIO_ACL, "bad-input");
    AssertHelpers.assertThrows(
        "should not accept bad input",
        IllegalArgumentException.class,
        "Cannot support S3 CannedACL bad-input",
        () -> new AwsProperties(map));
  }

  @Test
  public void testS3MultipartSizeTooSmall() {
    Map<String, String> map = Maps.newHashMap();
    map.put(AwsProperties.S3FILEIO_MULTIPART_SIZE, "1");
    AssertHelpers.assertThrows(
        "should not accept small part size",
        IllegalArgumentException.class,
        "Minimum multipart upload object size must be larger than 5 MB",
        () -> new AwsProperties(map));
  }

  @Test
  public void testS3MultipartSizeTooLarge() {
    Map<String, String> map = Maps.newHashMap();
    map.put(AwsProperties.S3FILEIO_MULTIPART_SIZE, "5368709120"); // 5GB
    AssertHelpers.assertThrows(
        "should not accept too big part size",
        IllegalArgumentException.class,
        "Input malformed or exceeded maximum multipart upload size 5GB",
        () -> new AwsProperties(map));
  }

  @Test
  public void testS3MultipartThresholdFactorLessThanOne() {
    Map<String, String> map = Maps.newHashMap();
    map.put(AwsProperties.S3FILEIO_MULTIPART_THRESHOLD_FACTOR, "0.9");
    AssertHelpers.assertThrows(
        "should not accept factor less than 1",
        IllegalArgumentException.class,
        "Multipart threshold factor must be >= to 1.0",
        () -> new AwsProperties(map));
  }

  @Test
  public void testS3FileIoDeleteBatchSizeTooLarge() {
    Map<String, String> map = Maps.newHashMap();
    map.put(AwsProperties.S3FILEIO_DELETE_BATCH_SIZE, "2000");
    AssertHelpers.assertThrows(
        "should not accept batch size greater than 1000",
        IllegalArgumentException.class,
        "Deletion batch size must be between 1 and 1000",
        () -> new AwsProperties(map));
  }

  @Test
  public void testS3FileIoDeleteBatchSizeTooSmall() {
    Map<String, String> map = Maps.newHashMap();
    map.put(AwsProperties.S3FILEIO_DELETE_BATCH_SIZE, "0");
    AssertHelpers.assertThrows(
        "should not accept batch size less than 1",
        IllegalArgumentException.class,
        "Deletion batch size must be between 1 and 1000",
        () -> new AwsProperties(map));
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

    Assert.assertTrue(
        "Should use default credentials if nothing is set",
        capturedAwsCredentialsProvider instanceof DefaultCredentialsProvider);
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

    Assert.assertTrue(
        "Should use basic credentials if access key ID and secret access key are set",
        capturedAwsCredentialsProvider.resolveCredentials() instanceof AwsBasicCredentials);
    Assert.assertEquals(
        "The access key id should be the same as the one set by tag S3FILEIO_ACCESS_KEY_ID",
        "key",
        capturedAwsCredentialsProvider.resolveCredentials().accessKeyId());
    Assert.assertEquals(
        "The secret access key should be the same as the one set by tag S3FILEIO_SECRET_ACCESS_KEY",
        "secret",
        capturedAwsCredentialsProvider.resolveCredentials().secretAccessKey());
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

    Assert.assertTrue(
        "Should use session credentials if session token is set",
        capturedAwsCredentialsProvider.resolveCredentials() instanceof AwsSessionCredentials);
    Assert.assertEquals(
        "The access key id should be the same as the one set by tag S3FILEIO_ACCESS_KEY_ID",
        "key",
        capturedAwsCredentialsProvider.resolveCredentials().accessKeyId());
    Assert.assertEquals(
        "The secret access key should be the same as the one set by tag S3FILEIO_SECRET_ACCESS_KEY",
        "secret",
        capturedAwsCredentialsProvider.resolveCredentials().secretAccessKey());
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

    Assert.assertTrue(
        "Should use url connection http client",
        capturedHttpClientBuilder instanceof UrlConnectionHttpClient.Builder);
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
    Assert.assertTrue(
        "Should use apache http client",
        capturedHttpClientBuilder instanceof ApacheHttpClient.Builder);
  }

  @Test
  public void testInvalidHttpClientType() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.HTTP_CLIENT_TYPE, "test");
    AwsProperties awsProperties = new AwsProperties(properties);
    S3ClientBuilder s3ClientBuilder = S3Client.builder();

    AssertHelpers.assertThrows(
        "should not support http client types other than urlconnection and apache",
        IllegalArgumentException.class,
        "Unrecognized HTTP client type",
        () -> awsProperties.applyHttpClientConfigurations(s3ClientBuilder));
  }

  @Test
  public void testKryoSerialization() throws IOException {
    AwsProperties awsProperties = new AwsProperties();
    AwsProperties deSerializedAwsProperties =
        TestHelpers.KryoHelpers.roundTripSerialize(awsProperties);
    Assert.assertEquals(
        awsProperties.s3BucketToAccessPointMapping(),
        deSerializedAwsProperties.s3BucketToAccessPointMapping());

    AwsProperties awsPropertiesWithProps = new AwsProperties(ImmutableMap.of("a", "b"));
    AwsProperties deSerializedAwsPropertiesWithProps =
        TestHelpers.KryoHelpers.roundTripSerialize(awsPropertiesWithProps);
    Assert.assertEquals(
        awsPropertiesWithProps.s3BucketToAccessPointMapping(),
        deSerializedAwsPropertiesWithProps.s3BucketToAccessPointMapping());

    AwsProperties awsPropertiesWithEmptyProps = new AwsProperties(Collections.emptyMap());
    AwsProperties deSerializedAwsPropertiesWithEmptyProps =
        TestHelpers.KryoHelpers.roundTripSerialize(awsPropertiesWithProps);
    Assert.assertEquals(
        awsPropertiesWithEmptyProps.s3BucketToAccessPointMapping(),
        deSerializedAwsPropertiesWithEmptyProps.s3BucketToAccessPointMapping());
  }

  @Test
  public void testS3RemoteSignerWithoutUri() {
    Map<String, String> properties =
        ImmutableMap.of(AwsProperties.S3_SIGNER_IMPL, S3V4RestSignerClient.class.getName());
    AwsProperties awsProperties = new AwsProperties(properties);

    Assertions.assertThatThrownBy(
            () -> awsProperties.applyS3SignerConfiguration(S3Client.builder()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("S3 signer service URI is required");
  }

  @Test
  public void testS3RemoteSigner() {
    String uri = "http://localhost:12345";
    Map<String, String> properties =
        ImmutableMap.of(
            AwsProperties.S3_SIGNER_IMPL,
            S3V4RestSignerClient.class.getName(),
            CatalogProperties.URI,
            uri);
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
  public void testS3LocalSigner() {
    Map<String, String> properties =
        ImmutableMap.of(AwsProperties.S3_SIGNER_IMPL, AwsS3V4Signer.class.getName());
    AwsProperties awsProperties = new AwsProperties(properties);
    S3ClientBuilder builder = S3Client.builder();

    awsProperties.applyS3SignerConfiguration(builder);

    Optional<Signer> signer =
        builder.overrideConfiguration().advancedOption(SdkAdvancedClientOption.SIGNER);
    Assertions.assertThat(signer).isPresent().get().isInstanceOf(AwsS3V4Signer.class);
  }

  @Test
  public void testS3WrongSigner() {
    Map<String, String> properties = ImmutableMap.of(AwsProperties.S3_SIGNER_IMPL, "WrongSigner");
    AwsProperties awsProperties = new AwsProperties(properties);

    Assertions.assertThatThrownBy(
            () -> awsProperties.applyS3SignerConfiguration(S3Client.builder()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot instantiate custom signer: WrongSigner");
  }

  @Test
  public void testS3SignerWrongSubclass() {
    // we're just passing some random class as a signer impl
    Map<String, String> properties =
        ImmutableMap.of(AwsProperties.S3_SIGNER_IMPL, AwsProperties.class.getName());
    AwsProperties awsProperties = new AwsProperties(properties);

    Assertions.assertThatThrownBy(
            () -> awsProperties.applyS3SignerConfiguration(S3Client.builder()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Custom signer org.apache.iceberg.aws.AwsProperties must be an instance of software.amazon.awssdk.core.signer.Signer");
  }
}
