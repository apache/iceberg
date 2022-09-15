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

import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
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
  public void testS3FileIoCredentialsConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    ArgumentCaptor<AwsCredentialsProvider> awsCredentialsProviderArgumentCaptor =
        ArgumentCaptor.forClass(AwsCredentialsProvider.class);

    // set nothing
    AwsProperties testAwsPropertiesAllNull = new AwsProperties(properties);
    S3ClientBuilder mockS3ClientBuilderAllNull = Mockito.mock(S3ClientBuilder.class);
    testAwsPropertiesAllNull.applyS3CredentialConfigurations(mockS3ClientBuilderAllNull);
    Mockito.verify(mockS3ClientBuilderAllNull)
        .credentialsProvider(awsCredentialsProviderArgumentCaptor.capture());
    AwsCredentialsProvider capturedAwsCredentialsProviderAllNull =
        awsCredentialsProviderArgumentCaptor.getValue();
    Assert.assertTrue(
        "Should use default credentials if nothing is set",
        capturedAwsCredentialsProviderAllNull instanceof DefaultCredentialsProvider);

    // set access key id and secret access key
    properties.put(AwsProperties.S3FILEIO_ACCESS_KEY_ID, "key");
    properties.put(AwsProperties.S3FILEIO_SECRET_ACCESS_KEY, "secret");
    AwsProperties testAwsPropertiesTwoSet = new AwsProperties(properties);
    S3ClientBuilder mockS3ClientBuilderTwoSet = Mockito.mock(S3ClientBuilder.class);
    ArgumentCaptor<AwsCredentialsProvider> awsCredentialsProviderArgumentCaptorTwoSet =
        ArgumentCaptor.forClass(AwsCredentialsProvider.class);
    testAwsPropertiesTwoSet.applyS3CredentialConfigurations(mockS3ClientBuilderTwoSet);
    Mockito.verify(mockS3ClientBuilderTwoSet)
        .credentialsProvider(awsCredentialsProviderArgumentCaptorTwoSet.capture());

    AwsCredentialsProvider capturedAwsCredentialsProviderTwoSet =
        awsCredentialsProviderArgumentCaptorTwoSet.getValue();
    Assert.assertTrue(
        "Should use basic credentials if access key ID and secret access key are set",
        capturedAwsCredentialsProviderTwoSet.resolveCredentials() instanceof AwsBasicCredentials);
    Assert.assertTrue(
        "The access key id should be the same as the one set by tag S3FILEIO_ACCESS_KEY_ID",
        capturedAwsCredentialsProviderTwoSet.resolveCredentials().accessKeyId().equals("key"));
    Assert.assertTrue(
        "The secret access key should be the same as the one set by tag S3FILEIO_SECRET_ACCESS_KEY",
        capturedAwsCredentialsProviderTwoSet
            .resolveCredentials()
            .secretAccessKey()
            .equals("secret"));

    // set access key id, secret access key, and session token
    properties.put(AwsProperties.S3FILEIO_SESSION_TOKEN, "token");
    AwsProperties testAwsPropertiesThreeSet = new AwsProperties(properties);
    S3ClientBuilder mockS3ClientBuilderThreeSet = Mockito.mock(S3ClientBuilder.class);
    ArgumentCaptor<AwsCredentialsProvider> awsCredentialsProviderArgumentCaptorThreeSet =
        ArgumentCaptor.forClass(AwsCredentialsProvider.class);
    testAwsPropertiesThreeSet.applyS3CredentialConfigurations(mockS3ClientBuilderThreeSet);
    Mockito.verify(mockS3ClientBuilderThreeSet)
        .credentialsProvider(awsCredentialsProviderArgumentCaptorThreeSet.capture());
    AwsCredentialsProvider capturedAwsCredentialsProviderThreeSet =
        awsCredentialsProviderArgumentCaptorThreeSet.getValue();
    Assert.assertTrue(
        "Should use session credentials if session token is set",
        capturedAwsCredentialsProviderThreeSet.resolveCredentials()
            instanceof AwsSessionCredentials);
    Assert.assertTrue(
        "The access key id should be the same as the one set by tag S3FILEIO_ACCESS_KEY_ID",
        capturedAwsCredentialsProviderThreeSet.resolveCredentials().accessKeyId().equals("key"));
    Assert.assertTrue(
        "The secret access key should be the same as the one set by tag S3FILEIO_SECRET_ACCESS_KEY",
        capturedAwsCredentialsProviderThreeSet
            .resolveCredentials()
            .secretAccessKey()
            .equals("secret"));
  }
}
