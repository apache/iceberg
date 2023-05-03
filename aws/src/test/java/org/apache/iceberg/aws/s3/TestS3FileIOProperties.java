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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
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

    Assertions.assertThat(S3FileIOProperties.S3FILEIO_SSE_TYPE_NONE)
        .isEqualTo(s3FileIOProperties.s3FileIoSseType());

    Assertions.assertThat(s3FileIOProperties.s3FileIoSseKey()).isNull();
    Assertions.assertThat(s3FileIOProperties.s3FileIoSseMd5()).isNull();
    Assertions.assertThat(s3FileIOProperties.s3AccessKeyId()).isNull();
    Assertions.assertThat(s3FileIOProperties.s3SecretAccessKey()).isNull();
    Assertions.assertThat(s3FileIOProperties.s3SessionToken()).isNull();
    Assertions.assertThat(s3FileIOProperties.s3FileIoAcl()).isNull();
    Assertions.assertThat(s3FileIOProperties.s3Endpoint()).isNull();

    Assertions.assertThat(S3FileIOProperties.S3_PRELOAD_CLIENT_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.s3PreloadClientEnabled());

    Assertions.assertThat(S3FileIOProperties.S3_DUALSTACK_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.isS3DualStackEnabled());

    Assertions.assertThat(S3FileIOProperties.S3FILEIO_PATH_STYLE_ACCESS_DEFAULT)
        .isEqualTo(s3FileIOProperties.isS3PathStyleAccess());

    Assertions.assertThat(S3FileIOProperties.S3_USE_ARN_REGION_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.isS3UseArnRegionEnabled());

    Assertions.assertThat(S3FileIOProperties.S3_ACCELERATION_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.isS3AccelerationEnabled());

    Assertions.assertThat(S3FileIOProperties.S3_REMOTE_SIGNING_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.isS3RemoteSigningEnabled());

    Assertions.assertThat(Runtime.getRuntime().availableProcessors())
        .isEqualTo(s3FileIOProperties.s3FileIoMultipartUploadThreads());

    Assertions.assertThat(S3FileIOProperties.S3FILEIO_MULTIPART_SIZE_DEFAULT)
        .isEqualTo(s3FileIOProperties.s3FileIoMultiPartSize());

    Assertions.assertThat(S3FileIOProperties.S3FILEIO_MULTIPART_THRESHOLD_FACTOR_DEFAULT)
        .isEqualTo(s3FileIOProperties.s3FileIOMultipartThresholdFactor());

    Assertions.assertThat(S3FileIOProperties.S3FILEIO_DELETE_BATCH_SIZE_DEFAULT)
        .isEqualTo(s3FileIOProperties.s3FileIoDeleteBatchSize());

    Assertions.assertThat(System.getProperty("java.io.tmpdir"))
        .isEqualTo(s3FileIOProperties.s3fileIoStagingDirectory());

    Assertions.assertThat(S3FileIOProperties.S3_CHECKSUM_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.isS3ChecksumEnabled());

    Assertions.assertThat(Sets.newHashSet()).isEqualTo(s3FileIOProperties.s3WriteTags());

    Assertions.assertThat(S3FileIOProperties.S3_WRITE_TABLE_TAG_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.s3WriteTableTagEnabled());

    Assertions.assertThat(S3FileIOProperties.S3_WRITE_NAMESPACE_TAG_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.s3WriteNamespaceTagEnabled());

    Assertions.assertThat(Sets.newHashSet()).isEqualTo(s3FileIOProperties.s3DeleteTags());

    Assertions.assertThat(Runtime.getRuntime().availableProcessors())
        .isEqualTo(s3FileIOProperties.s3FileIoDeleteThreads());

    Assertions.assertThat(S3FileIOProperties.S3_DELETE_ENABLED_DEFAULT)
        .isEqualTo(s3FileIOProperties.isS3DeleteEnabled());

    Assertions.assertThat(Collections.emptyMap())
        .isEqualTo(s3FileIOProperties.s3BucketToAccessPointMapping());
  }

  @Test
  public void testS3FileIOProperties() {
    Map<String, String> map = getTestProperties();
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties(map);

    Assertions.assertThat(map)
        .containsEntry(S3FileIOProperties.S3FILEIO_SSE_TYPE, s3FileIOProperties.s3FileIoSseType());

    Assertions.assertThat(map)
        .containsEntry(S3FileIOProperties.S3FILEIO_SSE_KEY, s3FileIOProperties.s3FileIoSseKey());

    Assertions.assertThat(map)
        .containsEntry(S3FileIOProperties.S3FILEIO_SSE_MD5, s3FileIOProperties.s3FileIoSseMd5());

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3FILEIO_ACCESS_KEY_ID, s3FileIOProperties.s3AccessKeyId());

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3FILEIO_SECRET_ACCESS_KEY, s3FileIOProperties.s3SecretAccessKey());

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3FILEIO_SESSION_TOKEN, s3FileIOProperties.s3SessionToken());

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3FILEIO_ACL, s3FileIOProperties.s3FileIoAcl().toString());

    Assertions.assertThat(map)
        .containsEntry(S3FileIOProperties.S3FILEIO_ENDPOINT, s3FileIOProperties.s3Endpoint());

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3_PRELOAD_CLIENT_ENABLED,
            String.valueOf(s3FileIOProperties.s3PreloadClientEnabled()));

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3_DUALSTACK_ENABLED,
            String.valueOf(s3FileIOProperties.isS3DualStackEnabled()));

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3FILEIO_PATH_STYLE_ACCESS,
            String.valueOf(s3FileIOProperties.isS3PathStyleAccess()));

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3_USE_ARN_REGION_ENABLED,
            String.valueOf(s3FileIOProperties.isS3UseArnRegionEnabled()));

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3_ACCELERATION_ENABLED,
            String.valueOf(s3FileIOProperties.isS3AccelerationEnabled()));

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3_REMOTE_SIGNING_ENABLED,
            String.valueOf(s3FileIOProperties.isS3RemoteSigningEnabled()));

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3FILEIO_MULTIPART_UPLOAD_THREADS,
            String.valueOf(s3FileIOProperties.s3FileIoMultipartUploadThreads()));

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3FILEIO_MULTIPART_SIZE,
            String.valueOf(s3FileIOProperties.s3FileIoMultiPartSize()));

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3FILEIO_MULTIPART_THRESHOLD_FACTOR,
            String.valueOf(s3FileIOProperties.s3FileIOMultipartThresholdFactor()));

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3FILEIO_DELETE_BATCH_SIZE,
            String.valueOf(s3FileIOProperties.s3FileIoDeleteBatchSize()));

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3FILEIO_STAGING_DIRECTORY,
            String.valueOf(s3FileIOProperties.s3fileIoStagingDirectory()));

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3_CHECKSUM_ENABLED,
            String.valueOf(s3FileIOProperties.isS3ChecksumEnabled()));

    List<String> writeTagValues =
        s3FileIOProperties.s3WriteTags().stream().map(Tag::value).collect(Collectors.toList());
    writeTagValues.forEach(
        value ->
            Assertions.assertThat(map)
                .containsEntry(S3FileIOProperties.S3_WRITE_TAGS_PREFIX + S3_WRITE_TAG_KEY, value));

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3_WRITE_TABLE_TAG_ENABLED,
            String.valueOf(s3FileIOProperties.s3WriteTableTagEnabled()));

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3_WRITE_NAMESPACE_TAG_ENABLED,
            String.valueOf(s3FileIOProperties.s3WriteNamespaceTagEnabled()));

    List<String> deleteTagValues =
        s3FileIOProperties.s3DeleteTags().stream().map(Tag::value).collect(Collectors.toList());
    deleteTagValues.forEach(
        value ->
            Assertions.assertThat(map)
                .containsEntry(
                    S3FileIOProperties.S3_DELETE_TAGS_PREFIX + S3_DELETE_TAG_KEY, value));

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3FILEIO_DELETE_THREADS,
            String.valueOf(s3FileIOProperties.s3FileIoDeleteThreads()));

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3_DELETE_ENABLED,
            String.valueOf(s3FileIOProperties.isS3DeleteEnabled()));

    s3FileIOProperties
        .s3BucketToAccessPointMapping()
        .values()
        .forEach(
            value ->
                Assertions.assertThat(map)
                    .containsEntry(
                        S3FileIOProperties.S3_ACCESS_POINTS_PREFIX + S3_TEST_BUCKET_NAME, value));

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3_PRELOAD_CLIENT_ENABLED,
            String.valueOf(s3FileIOProperties.s3PreloadClientEnabled()));

    Assertions.assertThat(map)
        .containsEntry(
            S3FileIOProperties.S3_REMOTE_SIGNING_ENABLED,
            String.valueOf(s3FileIOProperties.isS3RemoteSigningEnabled()));
  }

  @Test
  public void testS3AccessKeySet_secretKeyNotSet() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.S3FILEIO_ACCESS_KEY_ID, "s3-access-key");

    Assertions.assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 client access key ID and secret access key must be set at the same time");
  }

  @Test
  public void testS3SecretKeySet_accessKeyNotSet() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.S3FILEIO_SECRET_ACCESS_KEY, "s3-secret-key");

    Assertions.assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 client access key ID and secret access key must be set at the same time");
  }

  @Test
  public void testS3FileIoSseCustom_mustHaveCustomKey() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.S3FILEIO_SSE_TYPE, S3FileIOProperties.S3FILEIO_SSE_TYPE_CUSTOM);

    Assertions.assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot initialize SSE-C S3FileIO with null encryption key");
  }

  @Test
  public void testS3FileIoSseCustom_mustHaveCustomMd5() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.S3FILEIO_SSE_TYPE, S3FileIOProperties.S3FILEIO_SSE_TYPE_CUSTOM);
    map.put(S3FileIOProperties.S3FILEIO_SSE_KEY, "something");

    Assertions.assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot initialize SSE-C S3FileIO with null encryption key MD5");
  }

  @Test
  public void testS3FileIoAcl() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.S3FILEIO_ACL, ObjectCannedACL.AUTHENTICATED_READ.toString());
    S3FileIOProperties properties = new S3FileIOProperties(map);
    Assertions.assertThat(ObjectCannedACL.AUTHENTICATED_READ).isEqualTo(properties.s3FileIoAcl());
  }

  @Test
  public void testS3FileIoAcl_unknownType() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.S3FILEIO_ACL, "bad-input");

    Assertions.assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot support S3 CannedACL bad-input");
  }

  @Test
  public void testS3MultipartSizeTooSmall() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.S3FILEIO_MULTIPART_SIZE, "1");

    Assertions.assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Minimum multipart upload object size must be larger than 5 MB.");
  }

  @Test
  public void testS3MultipartSizeTooLarge() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.S3FILEIO_MULTIPART_SIZE, "5368709120"); // 5GB

    Assertions.assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Input malformed or exceeded maximum multipart upload size 5GB: 5368709120");
  }

  @Test
  public void testS3MultipartThresholdFactorLessThanOne() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.S3FILEIO_MULTIPART_THRESHOLD_FACTOR, "0.9");

    Assertions.assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Multipart threshold factor must be >= to 1.0");
  }

  @Test
  public void testS3FileIoDeleteBatchSizeTooLarge() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.S3FILEIO_DELETE_BATCH_SIZE, "2000");

    Assertions.assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Deletion batch size must be between 1 and 1000");
  }

  @Test
  public void testS3FileIoDeleteBatchSizeTooSmall() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.S3FILEIO_DELETE_BATCH_SIZE, "0");

    Assertions.assertThatThrownBy(() -> new S3FileIOProperties(map))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Deletion batch size must be between 1 and 1000");
  }

  private Map<String, String> getTestProperties() {
    Map<String, String> map = Maps.newHashMap();
    map.put(S3FileIOProperties.S3FILEIO_SSE_TYPE, "sse_type");
    map.put(S3FileIOProperties.S3FILEIO_SSE_KEY, "sse_key");
    map.put(S3FileIOProperties.S3FILEIO_SSE_MD5, "sse_md5");
    map.put(S3FileIOProperties.S3FILEIO_ACCESS_KEY_ID, "access_key_id");
    map.put(S3FileIOProperties.S3FILEIO_SECRET_ACCESS_KEY, "secret_access_key");
    map.put(S3FileIOProperties.S3FILEIO_SESSION_TOKEN, "session_token");
    map.put(S3FileIOProperties.S3FILEIO_ENDPOINT, "s3_endpoint");
    map.put(S3FileIOProperties.S3FILEIO_MULTIPART_UPLOAD_THREADS, "1");
    map.put(S3FileIOProperties.S3FILEIO_PATH_STYLE_ACCESS, "true");
    map.put(S3FileIOProperties.S3_USE_ARN_REGION_ENABLED, "true");
    map.put(S3FileIOProperties.S3_ACCELERATION_ENABLED, "true");
    map.put(S3FileIOProperties.S3_DUALSTACK_ENABLED, "true");
    map.put(
        S3FileIOProperties.S3FILEIO_MULTIPART_SIZE,
        String.valueOf(S3FileIOProperties.S3FILEIO_MULTIPART_SIZE_DEFAULT));
    map.put(S3FileIOProperties.S3FILEIO_MULTIPART_THRESHOLD_FACTOR, "1.0");
    map.put(S3FileIOProperties.S3FILEIO_STAGING_DIRECTORY, "test_staging_directorty");
    /*  value has to be one of {@link software.amazon.awssdk.services.s3.model.ObjectCannedACL}
    https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html*/
    map.put(S3FileIOProperties.S3FILEIO_ACL, "public-read-write");
    map.put(S3FileIOProperties.S3_CHECKSUM_ENABLED, "true");
    map.put(S3FileIOProperties.S3FILEIO_DELETE_BATCH_SIZE, "1");
    map.put(S3FileIOProperties.S3_WRITE_TAGS_PREFIX + S3_WRITE_TAG_KEY, S3_WRITE_TAG_VALUE);
    map.put(S3FileIOProperties.S3_WRITE_TABLE_TAG_ENABLED, "true");
    map.put(S3FileIOProperties.S3_WRITE_NAMESPACE_TAG_ENABLED, "true");
    map.put(S3FileIOProperties.S3_DELETE_TAGS_PREFIX + S3_DELETE_TAG_KEY, S3_DELETE_TAG_VALUE);
    map.put(S3FileIOProperties.S3FILEIO_DELETE_THREADS, "1");
    map.put(S3FileIOProperties.S3_DELETE_ENABLED, "true");
    map.put(
        S3FileIOProperties.S3_ACCESS_POINTS_PREFIX + S3_TEST_BUCKET_NAME,
        S3_TEST_BUCKET_ACCESS_POINT);
    map.put(S3FileIOProperties.S3_PRELOAD_CLIENT_ENABLED, "true");
    map.put(S3FileIOProperties.S3_REMOTE_SIGNING_ENABLED, "true");
    return map;
  }
}
