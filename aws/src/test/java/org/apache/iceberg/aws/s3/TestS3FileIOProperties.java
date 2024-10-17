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

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.Tag;

public class TestS3FileIOProperties {
  private static final String S3_TEST_BUCKET_NAME = "my_bucket";
  private static final String S3_TEST_BUCKET_ACCESS_POINT = "access_point";
  private static final String S3_WRITE_TAG_KEY = "my_key";
  private static final String S3_WRITE_TAG_VALUE = "my_value";
  private static final String S3_DELETE_TAG_KEY = "my_key";
  private static final String S3_DELETE_TAG_VALUE = "my_value";

  @Test
  public void testS3FileIOPropertiesDefaultValues() {
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties();

    assertThat(S3FileIOProperties.SSE_TYPE_NONE).isEqualTo(s3FileIOProperties.sseType());

    assertThat(s3FileIOProperties.sseKey()).isNull();
    assertThat(s3FileIOProperties.sseMd5()).isNull();
    assertThat(s3FileIOProperties.accessKeyId()).isNull();
    assertThat(s3FileIOProperties.secretAccessKey()).isNull();
    assertThat(s3FileIOProperties.sessionToken()).isNull();
    assertThat(s3FileIOProperties.acl()).isNull();
    assertThat(s3FileIOProperties.endpoint()).isNull();
    assertThat(s3FileIOProperties.writeStorageClass()).isNull();

    assertThat(S3FileIOProperties.PRELOAD_CLIENT_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.isPreloadClientEnabled());

    assertThat(S3FileIOProperties.DUALSTACK_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.isDualStackEnabled());

    assertThat(S3FileIOProperties.CROSS_REGION_ACCESS_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.isCrossRegionAccessEnabled());

    assertThat(S3FileIOProperties.PATH_STYLE_ACCESS_DEFAULT)
        .isEqualTo(s3FileIOProperties.isPathStyleAccess());

    assertThat(S3FileIOProperties.USE_ARN_REGION_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.isUseArnRegionEnabled());

    assertThat(S3FileIOProperties.ACCELERATION_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.isAccelerationEnabled());

    assertThat(S3FileIOProperties.REMOTE_SIGNING_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.isRemoteSigningEnabled());

    assertThat(Runtime.getRuntime().availableProcessors())
        .isEqualTo(s3FileIOProperties.multipartUploadThreads());

    assertThat(S3FileIOProperties.MULTIPART_SIZE_DEFAULT)
        .isEqualTo(s3FileIOProperties.multiPartSize());

    assertThat(S3FileIOProperties.MULTIPART_THRESHOLD_FACTOR_DEFAULT)
        .isEqualTo(s3FileIOProperties.multipartThresholdFactor());

    assertThat(S3FileIOProperties.DELETE_BATCH_SIZE_DEFAULT)
        .isEqualTo(s3FileIOProperties.deleteBatchSize());

    assertThat(System.getProperty("java.io.tmpdir"))
        .isEqualTo(s3FileIOProperties.stagingDirectory());

    assertThat(S3FileIOProperties.CHECKSUM_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.isChecksumEnabled());

    assertThat(Sets.newHashSet()).isEqualTo(s3FileIOProperties.writeTags());

    assertThat(S3FileIOProperties.WRITE_TABLE_TAG_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.writeTableTagEnabled());

    assertThat(S3FileIOProperties.WRITE_NAMESPACE_TAG_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.isWriteNamespaceTagEnabled());

    assertThat(Sets.newHashSet()).isEqualTo(s3FileIOProperties.deleteTags());

    assertThat(Runtime.getRuntime().availableProcessors())
        .isEqualTo(s3FileIOProperties.deleteThreads());

    assertThat(S3FileIOProperties.DELETE_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.isDeleteEnabled());

    assertThat(Collections.emptyMap()).isEqualTo(s3FileIOProperties.bucketToAccessPointMapping());
  }

  @Test
  public void testS3FileIOProperties() {
    Map<String, String> map = getTestProperties();
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties(map);

    assertThat(map).containsEntry(S3FileIOProperties.SSE_TYPE, s3FileIOProperties.sseType());

    assertThat(map).containsEntry(S3FileIOProperties.SSE_KEY, s3FileIOProperties.sseKey());

    assertThat(map).containsEntry(S3FileIOProperties.SSE_MD5, s3FileIOProperties.sseMd5());

    assertThat(map)
        .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, s3FileIOProperties.accessKeyId());

    assertThat(map)
        .containsEntry(S3FileIOProperties.SECRET_ACCESS_KEY, s3FileIOProperties.secretAccessKey());

    assertThat(map)
        .containsEntry(S3FileIOProperties.SESSION_TOKEN, s3FileIOProperties.sessionToken());

    assertThat(map).containsEntry(S3FileIOProperties.ACL, s3FileIOProperties.acl().toString());

    assertThat(map).containsEntry(S3FileIOProperties.ENDPOINT, s3FileIOProperties.endpoint());

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.PRELOAD_CLIENT_ENABLED,
            String.valueOf(s3FileIOProperties.isPreloadClientEnabled()));

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.DUALSTACK_ENABLED,
            String.valueOf(s3FileIOProperties.isDualStackEnabled()));

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.CROSS_REGION_ACCESS_ENABLED,
            String.valueOf(s3FileIOProperties.isCrossRegionAccessEnabled()));

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.PATH_STYLE_ACCESS,
            String.valueOf(s3FileIOProperties.isPathStyleAccess()));

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.USE_ARN_REGION_ENABLED,
            String.valueOf(s3FileIOProperties.isUseArnRegionEnabled()));

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.ACCELERATION_ENABLED,
            String.valueOf(s3FileIOProperties.isAccelerationEnabled()));

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.REMOTE_SIGNING_ENABLED,
            String.valueOf(s3FileIOProperties.isRemoteSigningEnabled()));

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.MULTIPART_UPLOAD_THREADS,
            String.valueOf(s3FileIOProperties.multipartUploadThreads()));

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.MULTIPART_SIZE, String.valueOf(s3FileIOProperties.multiPartSize()));

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.MULTIPART_THRESHOLD_FACTOR,
            String.valueOf(s3FileIOProperties.multipartThresholdFactor()));

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.DELETE_BATCH_SIZE,
            String.valueOf(s3FileIOProperties.deleteBatchSize()));

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.STAGING_DIRECTORY,
            String.valueOf(s3FileIOProperties.stagingDirectory()));

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.CHECKSUM_ENABLED,
            String.valueOf(s3FileIOProperties.isChecksumEnabled()));

    List<String> writeTagValues =
        s3FileIOProperties.writeTags().stream().map(Tag::value).collect(Collectors.toList());
    writeTagValues.forEach(
        value ->
            assertThat(map)
                .containsEntry(S3FileIOProperties.WRITE_TAGS_PREFIX + S3_WRITE_TAG_KEY, value));

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.WRITE_TABLE_TAG_ENABLED,
            String.valueOf(s3FileIOProperties.writeTableTagEnabled()));

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.WRITE_NAMESPACE_TAG_ENABLED,
            String.valueOf(s3FileIOProperties.isWriteNamespaceTagEnabled()));

    List<String> deleteTagValues =
        s3FileIOProperties.deleteTags().stream().map(Tag::value).collect(Collectors.toList());
    deleteTagValues.forEach(
        value ->
            assertThat(map)
                .containsEntry(S3FileIOProperties.DELETE_TAGS_PREFIX + S3_DELETE_TAG_KEY, value));

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.DELETE_THREADS, String.valueOf(s3FileIOProperties.deleteThreads()));

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.DELETE_ENABLED,
            String.valueOf(s3FileIOProperties.isDeleteEnabled()));

    s3FileIOProperties
        .bucketToAccessPointMapping()
        .values()
        .forEach(
            value ->
                assertThat(map)
                    .containsEntry(
                        S3FileIOProperties.ACCESS_POINTS_PREFIX + S3_TEST_BUCKET_NAME, value));

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.PRELOAD_CLIENT_ENABLED,
            String.valueOf(s3FileIOProperties.isPreloadClientEnabled()));

    assertThat(map)
        .containsEntry(
            S3FileIOProperties.REMOTE_SIGNING_ENABLED,
            String.valueOf(s3FileIOProperties.isRemoteSigningEnabled()));

    assertThat(map).containsEntry(S3FileIOProperties.WRITE_STORAGE_CLASS, "INTELLIGENT_TIERING");
  }

  @Test
  public void testS3AccessKeySet_secretKeyNotSet() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.ACCESS_KEY_ID, "s3-access-key");

    assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 client access key ID and secret access key must be set at the same time");
  }

  @Test
  public void testS3SecretKeySet_accessKeyNotSet() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.SECRET_ACCESS_KEY, "s3-secret-key");

    assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 client access key ID and secret access key must be set at the same time");
  }

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
    assertThat(ObjectCannedACL.AUTHENTICATED_READ).isEqualTo(properties.acl());
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

  private Map<String, String> getTestProperties() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.SSE_TYPE, "sse_type");
    map.put(S3FileIOProperties.SSE_KEY, "sse_key");
    map.put(S3FileIOProperties.SSE_MD5, "sse_md5");
    map.put(S3FileIOProperties.ACCESS_KEY_ID, "access_key_id");
    map.put(S3FileIOProperties.SECRET_ACCESS_KEY, "secret_access_key");
    map.put(S3FileIOProperties.SESSION_TOKEN, "session_token");
    map.put(S3FileIOProperties.ENDPOINT, "s3_endpoint");
    map.put(S3FileIOProperties.MULTIPART_UPLOAD_THREADS, "1");
    map.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
    map.put(S3FileIOProperties.USE_ARN_REGION_ENABLED, "true");
    map.put(S3FileIOProperties.ACCELERATION_ENABLED, "true");
    map.put(S3FileIOProperties.DUALSTACK_ENABLED, "true");
    map.put(S3FileIOProperties.CROSS_REGION_ACCESS_ENABLED, "true");
    map.put(
        S3FileIOProperties.MULTIPART_SIZE,
        String.valueOf(S3FileIOProperties.MULTIPART_SIZE_DEFAULT));
    map.put(S3FileIOProperties.MULTIPART_THRESHOLD_FACTOR, "1.0");
    map.put(S3FileIOProperties.STAGING_DIRECTORY, "test_staging_directorty");
    /*  value has to be one of {@link software.amazon.awssdk.services.s3.model.ObjectCannedACL}
    https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html*/
    map.put(S3FileIOProperties.ACL, "public-read-write");
    map.put(S3FileIOProperties.CHECKSUM_ENABLED, "true");
    map.put(S3FileIOProperties.DELETE_BATCH_SIZE, "1");
    map.put(S3FileIOProperties.WRITE_TAGS_PREFIX + S3_WRITE_TAG_KEY, S3_WRITE_TAG_VALUE);
    map.put(S3FileIOProperties.WRITE_TABLE_TAG_ENABLED, "true");
    map.put(S3FileIOProperties.WRITE_NAMESPACE_TAG_ENABLED, "true");
    map.put(S3FileIOProperties.DELETE_TAGS_PREFIX + S3_DELETE_TAG_KEY, S3_DELETE_TAG_VALUE);
    map.put(S3FileIOProperties.DELETE_THREADS, "1");
    map.put(S3FileIOProperties.DELETE_ENABLED, "true");
    map.put(
        S3FileIOProperties.ACCESS_POINTS_PREFIX + S3_TEST_BUCKET_NAME, S3_TEST_BUCKET_ACCESS_POINT);
    map.put(S3FileIOProperties.PRELOAD_CLIENT_ENABLED, "true");
    map.put(S3FileIOProperties.REMOTE_SIGNING_ENABLED, "true");
    map.put(S3FileIOProperties.WRITE_STORAGE_CLASS, "INTELLIGENT_TIERING");
    return map;
  }

  @Test
  public void testApplyCredentialConfigurations() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(S3FileIOProperties.ACCESS_KEY_ID, "access");
    properties.put(S3FileIOProperties.SECRET_ACCESS_KEY, "secret");
    properties.put(S3FileIOProperties.SESSION_TOKEN, "session-token");

    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties(properties);
    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);
    AwsClientProperties mockClientProps = Mockito.mock(AwsClientProperties.class);

    s3FileIOProperties.applyCredentialConfigurations(mockClientProps, mockS3ClientBuilder);
    Mockito.verify(mockS3ClientBuilder).credentialsProvider(Mockito.any());
    Mockito.verify(mockClientProps)
        .credentialsProvider(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
  }

  @Test
  public void testApplyS3ServiceConfigurations() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(S3FileIOProperties.DUALSTACK_ENABLED, "true");
    properties.put(S3FileIOProperties.CROSS_REGION_ACCESS_ENABLED, "true");
    properties.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
    properties.put(S3FileIOProperties.USE_ARN_REGION_ENABLED, "true");
    // acceleration enabled has to be set to false if path style is true
    properties.put(S3FileIOProperties.ACCELERATION_ENABLED, "false");
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties(properties);
    S3ClientBuilder mockA = Mockito.mock(S3ClientBuilder.class);

    ArgumentCaptor<S3Configuration> s3ConfigurationCaptor =
        ArgumentCaptor.forClass(S3Configuration.class);

    Mockito.doReturn(mockA).when(mockA).dualstackEnabled(Mockito.anyBoolean());
    Mockito.doReturn(mockA).when(mockA).crossRegionAccessEnabled(Mockito.anyBoolean());
    Mockito.doReturn(mockA).when(mockA).serviceConfiguration(Mockito.any(S3Configuration.class));

    s3FileIOProperties.applyServiceConfigurations(mockA);

    Mockito.verify(mockA).serviceConfiguration(s3ConfigurationCaptor.capture());

    S3Configuration s3Configuration = s3ConfigurationCaptor.getValue();
    assertThat(s3Configuration.pathStyleAccessEnabled())
        .as("s3 path style access enabled parameter should be set to true")
        .isTrue();
    assertThat(s3Configuration.useArnRegionEnabled())
        .as("s3 use arn region enabled parameter should be set to true")
        .isTrue();
    assertThat(s3Configuration.accelerateModeEnabled())
        .as("s3 acceleration mode enabled parameter should be set to true")
        .isFalse();
  }

  @Test
  public void testApplySignerConfiguration() {
    Map<String, String> properties =
        ImmutableMap.of(
            S3FileIOProperties.REMOTE_SIGNING_ENABLED,
            "true",
            CatalogProperties.URI,
            "http://localhost:12345");
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties(properties);
    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);
    s3FileIOProperties.applySignerConfiguration(mockS3ClientBuilder);

    Mockito.verify(mockS3ClientBuilder)
        .overrideConfiguration(Mockito.any(ClientOverrideConfiguration.class));
  }

  @Test
  public void testApplyEndpointConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(S3FileIOProperties.ENDPOINT, "endpoint");
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties(properties);
    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);

    s3FileIOProperties.applyEndpointConfigurations(mockS3ClientBuilder);
    Mockito.verify(mockS3ClientBuilder).endpointOverride(Mockito.any(URI.class));
  }

  @Test
  public void testApplyUserAgentConfigurations() {
    Map<String, String> properties = Maps.newHashMap();
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties(properties);
    S3ClientBuilder mockS3ClientBuilder = Mockito.mock(S3ClientBuilder.class);
    s3FileIOProperties.applyUserAgentConfigurations(mockS3ClientBuilder);

    Mockito.verify(mockS3ClientBuilder)
        .overrideConfiguration(Mockito.any(ClientOverrideConfiguration.class));
  }

  @Test
  public void testApplyRetryConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(S3FileIOProperties.S3_RETRY_NUM_RETRIES, "999");
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties(properties);

    S3ClientBuilder builder = S3Client.builder();
    s3FileIOProperties.applyRetryConfigurations(builder);

    RetryPolicy retryPolicy = builder.overrideConfiguration().retryPolicy().get();
    assertThat(retryPolicy.numRetries()).as("retries was not set").isEqualTo(999);
  }
}
