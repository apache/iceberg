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

import static org.apache.iceberg.aws.s3.S3TestUtil.skipIfAnalyticsAcceleratorEnabled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsIntegTestUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariables;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.PartitionMetadata;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.ListAliasesRequest;
import software.amazon.awssdk.services.kms.model.ListAliasesResponse;
import software.amazon.awssdk.services.kms.model.ScheduleKeyDeletionRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.GetObjectAclRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAclResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.services.s3.model.PutBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.VersioningConfiguration;
import software.amazon.awssdk.services.s3control.S3ControlClient;
import software.amazon.awssdk.utils.IoUtils;
import software.amazon.s3.analyticsaccelerator.util.PrefetchMode;

@EnabledIfEnvironmentVariables({
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_ACCESS_KEY_ID, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_SECRET_ACCESS_KEY, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_SESSION_TOKEN, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_TEST_ACCOUNT_ID, matches = "\\d{12}"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_REGION, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_CROSS_REGION, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_TEST_BUCKET, matches = ".*"),
  @EnabledIfEnvironmentVariable(
      named = AwsIntegTestUtil.AWS_TEST_CROSS_REGION_BUCKET,
      matches = ".*")
})
public class TestS3FileIOIntegration {

  private final Random random = new Random(1);
  private static AwsClientFactory clientFactory;
  private static S3Client s3;
  private static S3ControlClient s3Control;
  private static S3ControlClient crossRegionS3Control;
  private static KmsClient kms;
  private static String bucketName;
  private static String crossRegionBucketName;
  private static String accessPointName;
  private static String crossRegionAccessPointName;
  private static String multiRegionAccessPointAlias;
  private static String prefix;
  private static byte[] contentBytes;
  private static String content;
  private static String kmsKeyArn;
  private static int deletionBatchSize;
  private String objectKey;
  private String objectUri;

  @BeforeAll
  public static void beforeClass() {
    clientFactory = AwsClientFactories.defaultFactory();
    s3 = clientFactory.s3();
    kms = clientFactory.kms();
    s3Control = AwsIntegTestUtil.createS3ControlClient(AwsIntegTestUtil.testRegion());
    crossRegionS3Control =
        AwsIntegTestUtil.createS3ControlClient(AwsIntegTestUtil.testCrossRegion());
    bucketName = AwsIntegTestUtil.testBucketName();
    crossRegionBucketName = AwsIntegTestUtil.testCrossRegionBucketName();
    accessPointName = UUID.randomUUID().toString();
    crossRegionAccessPointName = UUID.randomUUID().toString();
    prefix = UUID.randomUUID().toString();
    contentBytes = new byte[1024 * 1024 * 10];
    deletionBatchSize = 3;
    content = new String(contentBytes, StandardCharsets.UTF_8);
    kmsKeyArn = kms.createKey().keyMetadata().arn();

    if (!S3URI.isS3DirectoryBucket(bucketName)) {
      s3.putBucketVersioning(
          PutBucketVersioningRequest.builder()
              .bucket(bucketName)
              .versioningConfiguration(
                  VersioningConfiguration.builder().status(BucketVersioningStatus.ENABLED).build())
              .build());
      AwsIntegTestUtil.createAccessPoint(s3Control, accessPointName, bucketName);
      AwsIntegTestUtil.createAccessPoint(
          crossRegionS3Control, crossRegionAccessPointName, crossRegionBucketName);
      multiRegionAccessPointAlias = AwsIntegTestUtil.testMultiRegionAccessPointAlias();
    }
  }

  @AfterAll
  public static void afterClass() {
    if (S3URI.isS3DirectoryBucket(bucketName)) {
      S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
      s3FileIO.initialize(ImmutableMap.of());
      AwsIntegTestUtil.cleanS3DirectoryBucket(s3FileIO.client(), bucketName, prefix);
    } else {
      AwsIntegTestUtil.cleanS3GeneralPurposeBucket(s3, bucketName, prefix);
      AwsIntegTestUtil.deleteAccessPoint(s3Control, accessPointName);
      AwsIntegTestUtil.deleteAccessPoint(crossRegionS3Control, crossRegionAccessPointName);
      kms.scheduleKeyDeletion(
          ScheduleKeyDeletionRequest.builder().keyId(kmsKeyArn).pendingWindowInDays(7).build());
    }
  }

  @BeforeEach
  public void beforeEach() {
    objectKey = String.format("%s/%s", prefix, UUID.randomUUID());
    objectUri = String.format("s3://%s/%s", bucketName, objectKey);
    clientFactory.initialize(Maps.newHashMap());
  }

  @Test
  public void testNewInputStream() throws Exception {
    s3.putObject(
        PutObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
        RequestBody.fromBytes(contentBytes));
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(ImmutableMap.of());
    validateRead(s3FileIO);
  }

  @Test
  public void testS3FileIOWithS3FileIOAwsClientFactoryImpl() throws Exception {
    s3.putObject(
        PutObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
        RequestBody.fromBytes(contentBytes));
    S3FileIO s3FileIO = new S3FileIO();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(
        S3FileIOProperties.CLIENT_FACTORY,
        "org.apache.iceberg.aws.s3.DefaultS3FileIOAwsClientFactory");
    s3FileIO.initialize(properties);
    validateRead(s3FileIO);
  }

  @Test
  public void testS3FileIOWithDefaultAwsClientFactoryImpl() throws Exception {
    s3.putObject(
        PutObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
        RequestBody.fromBytes(contentBytes));
    S3FileIO s3FileIO = new S3FileIO();
    s3FileIO.initialize(Maps.newHashMap());
    validateRead(s3FileIO);
  }

  @Test
  public void testNewInputStreamWithAccessPoint() throws Exception {
    requireAccessPointSupport();
    s3.putObject(
        PutObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
        RequestBody.fromBytes(contentBytes));
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(
        ImmutableMap.of(
            S3FileIOProperties.ACCESS_POINTS_PREFIX + bucketName,
            testAccessPointARN(AwsIntegTestUtil.testRegion(), accessPointName)));
    validateRead(s3FileIO);
  }

  @Test
  public void testCrossRegionAccessEnabled() throws Exception {
    Map<String, String> properties =
        ImmutableMap.of(S3FileIOProperties.CROSS_REGION_ACCESS_ENABLED, "true");
    skipIfAnalyticsAcceleratorEnabled(
        new S3FileIOProperties(properties),
        "S3 Async Clients needed for Analytics Accelerator Library does not support Cross Region Access");
    clientFactory.initialize(properties);
    S3Client s3Client = clientFactory.s3();
    String crossBucketObjectKey = String.format("%s/%s", prefix, UUID.randomUUID());
    String crossBucketObjectUri =
        String.format("s3://%s/%s", crossRegionBucketName, crossBucketObjectKey);
    try {
      s3Client.putObject(
          PutObjectRequest.builder()
              .bucket(crossRegionBucketName)
              .key(crossBucketObjectKey)
              .build(),
          RequestBody.fromBytes(contentBytes));
      // make a copy in cross-region bucket
      S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
      validateRead(s3FileIO, crossBucketObjectUri);
    } finally {
      AwsIntegTestUtil.cleanS3GeneralPurposeBucket(
          s3Client, crossRegionBucketName, crossBucketObjectKey);
    }
  }

  @Test
  public void testNewInputStreamWithCrossRegionAccessPoint() throws Exception {
    requireAccessPointSupport();
    Map<String, String> properties =
        ImmutableMap.of(S3FileIOProperties.USE_ARN_REGION_ENABLED, "true");
    skipIfAnalyticsAcceleratorEnabled(
        new S3FileIOProperties(properties),
        "S3 Async Clients needed for Analytics Accelerator Library does not support Cross Region Access Points");
    clientFactory.initialize(properties);
    S3Client s3Client = clientFactory.s3();
    s3Client.putObject(
        PutObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
        RequestBody.fromBytes(contentBytes));
    // make a copy in cross-region bucket
    s3Client.putObject(
        PutObjectRequest.builder()
            .bucket(
                testAccessPointARN(AwsIntegTestUtil.testCrossRegion(), crossRegionAccessPointName))
            .key(objectKey)
            .build(),
        RequestBody.fromBytes(contentBytes));
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(
        ImmutableMap.of(
            S3FileIOProperties.ACCESS_POINTS_PREFIX + bucketName,
            testAccessPointARN(AwsIntegTestUtil.testCrossRegion(), crossRegionAccessPointName)));
    validateRead(s3FileIO);
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.aws.s3.S3TestUtil#analyticsAcceleratorLibraryProperties")
  public void testNewInputStreamWithMultiRegionAccessPoint(Map<String, String> aalProperties)
      throws Exception {
    assumeThat(multiRegionAccessPointAlias).isNotEmpty();
    Map<String, String> testProperties =
        ImmutableMap.of(S3FileIOProperties.USE_ARN_REGION_ENABLED, "true");
    clientFactory.initialize(mergeProperties(aalProperties, testProperties));
    S3Client s3Client = clientFactory.s3();
    s3Client.putObject(
        PutObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
        RequestBody.fromBytes(contentBytes));
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(
        ImmutableMap.of(
            S3FileIOProperties.ACCESS_POINTS_PREFIX + bucketName,
            testMultiRegionAccessPointARN(
                AwsIntegTestUtil.testRegion(), multiRegionAccessPointAlias)));
    validateRead(s3FileIO);
  }

  @Test
  public void testNewInputStreamWithAnalyticsAccelerator() throws Exception {
    s3.putObject(
        PutObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
        RequestBody.fromBytes(contentBytes));
    S3FileIO s3FileIO = new S3FileIO();
    s3FileIO.initialize(
        ImmutableMap.of(S3FileIOProperties.S3_ANALYTICS_ACCELERATOR_ENABLED, String.valueOf(true)));
    validateRead(s3FileIO);
  }

  @Test
  public void testNewInputStreamWithAnalyticsAcceleratorAndCRT() throws Exception {
    s3.putObject(
        PutObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
        RequestBody.fromBytes(contentBytes));
    S3FileIO s3FileIO = new S3FileIO();
    s3FileIO.initialize(
        ImmutableMap.of(
            S3FileIOProperties.S3_ANALYTICS_ACCELERATOR_ENABLED,
            String.valueOf(true),
            S3FileIOProperties.S3_CRT_ENABLED,
            String.valueOf(true)));
    validateRead(s3FileIO);
  }

  @Test
  public void testNewInputStreamWithAnalyticsAcceleratorCustomConfigured() throws Exception {
    final String prefetchingMode = "logicalio.prefetching.mode";
    final String s3Uri = String.format("s3://%s/%s/%s.parquet", bucketName, prefix, objectKey);
    S3FileIO s3FileIO = new S3FileIO();
    s3FileIO.initialize(
        ImmutableMap.of(
            S3FileIOProperties.S3_ANALYTICS_ACCELERATOR_ENABLED,
            String.valueOf(true),
            S3FileIOProperties.S3_ANALYTICS_ACCELERATOR_PROPERTIES_PREFIX + prefetchingMode,
            PrefetchMode.ALL.name()));
    write(s3FileIO, s3Uri);
    validateRead(s3FileIO, s3Uri);
    s3FileIO.deleteFile(s3Uri);
  }

  @Test
  public void testNewOutputStream() throws Exception {
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(ImmutableMap.of());
    write(s3FileIO);
    try (InputStream stream =
        s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(objectKey).build())) {
      String result = IoUtils.toUtf8String(stream);
      assertThat(result).isEqualTo(content);
    }
  }

  @Test
  public void testNewOutputStreamWithAccessPoint() throws Exception {
    requireAccessPointSupport();
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(
        ImmutableMap.of(
            S3FileIOProperties.ACCESS_POINTS_PREFIX + bucketName,
            testAccessPointARN(AwsIntegTestUtil.testRegion(), accessPointName)));
    write(s3FileIO);
    try (InputStream stream =
        s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(objectKey).build())) {
      String result = IoUtils.toUtf8String(stream);
      assertThat(result).isEqualTo(content);
    }
  }

  @Test
  public void testNewOutputStreamWithCrossRegionAccessPoint() throws Exception {
    requireAccessPointSupport();
    clientFactory.initialize(ImmutableMap.of(S3FileIOProperties.USE_ARN_REGION_ENABLED, "true"));
    S3Client s3Client = clientFactory.s3();
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(
        ImmutableMap.of(
            S3FileIOProperties.ACCESS_POINTS_PREFIX + bucketName,
            testAccessPointARN(AwsIntegTestUtil.testCrossRegion(), crossRegionAccessPointName)));
    write(s3FileIO);
    try (InputStream stream =
        s3Client.getObject(
            GetObjectRequest.builder()
                .bucket(
                    testAccessPointARN(
                        AwsIntegTestUtil.testCrossRegion(), crossRegionAccessPointName))
                .key(objectKey)
                .build())) {
      String result = IoUtils.toUtf8String(stream);
      assertThat(result).isEqualTo(content);
    }
  }

  @Test
  public void testNewOutputStreamWithMultiRegionAccessPoint() throws Exception {
    assumeThat(multiRegionAccessPointAlias).isNotEmpty();
    clientFactory.initialize(ImmutableMap.of(S3FileIOProperties.USE_ARN_REGION_ENABLED, "true"));
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(
        ImmutableMap.of(
            S3FileIOProperties.ACCESS_POINTS_PREFIX + bucketName,
            testMultiRegionAccessPointARN(
                AwsIntegTestUtil.testRegion(), multiRegionAccessPointAlias)));
    write(s3FileIO);
    try (InputStream stream =
        s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(objectKey).build())) {
      String result = IoUtils.toUtf8String(stream);
      assertThat(result).isEqualTo(content);
    }
  }

  @Test
  public void testNewOutputStreamWithAnalyticsAccelerator() throws Exception {
    S3FileIO s3FileIO = new S3FileIO();
    s3FileIO.initialize(
        ImmutableMap.of(S3FileIOProperties.S3_ANALYTICS_ACCELERATOR_ENABLED, String.valueOf(true)));
    write(s3FileIO);
    try (InputStream stream =
        s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(objectKey).build())) {
      String result = IoUtils.toUtf8String(stream);
      assertThat(result).isEqualTo(content);
    }
  }

  @Test
  public void testServerSideS3Encryption() throws Exception {
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(
        ImmutableMap.of(S3FileIOProperties.SSE_TYPE, S3FileIOProperties.SSE_TYPE_S3));
    write(s3FileIO);
    validateRead(s3FileIO);
    GetObjectResponse response =
        s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(objectKey).build())
            .response();
    assertThat(response.serverSideEncryption()).isEqualTo(ServerSideEncryption.AES256);
  }

  @Test
  public void testServerSideKmsEncryption() throws Exception {
    requireKMSEncryptionSupport();
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(
        ImmutableMap.of(
            S3FileIOProperties.SSE_TYPE,
            S3FileIOProperties.SSE_TYPE_KMS,
            S3FileIOProperties.SSE_KEY,
            kmsKeyArn));
    write(s3FileIO);
    validateRead(s3FileIO);
    GetObjectResponse response =
        s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(objectKey).build())
            .response();
    assertThat(response.serverSideEncryption()).isEqualTo(ServerSideEncryption.AWS_KMS);
    assertThat(kmsKeyArn).isEqualTo(response.ssekmsKeyId());
  }

  @Test
  public void testServerSideKmsEncryptionWithDefaultKey() throws Exception {
    requireKMSEncryptionSupport();
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(
        ImmutableMap.of(S3FileIOProperties.SSE_TYPE, S3FileIOProperties.SSE_TYPE_KMS));
    write(s3FileIO);
    validateRead(s3FileIO);
    GetObjectResponse response =
        s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(objectKey).build())
            .response();
    assertThat(response.serverSideEncryption()).isEqualTo(ServerSideEncryption.AWS_KMS);
    ListAliasesResponse listAliasesResponse =
        kms.listAliases(ListAliasesRequest.builder().keyId(response.ssekmsKeyId()).build());
    assertThat(listAliasesResponse.hasAliases()).isTrue();
    assertThat(listAliasesResponse.aliases())
        .hasSize(1)
        .first()
        .satisfies(
            aliasListEntry -> assertThat(aliasListEntry.aliasName()).isEqualTo("alias/aws/s3"));
  }

  @Test
  public void testDualLayerServerSideKmsEncryption() throws Exception {
    requireKMSEncryptionSupport();
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(
        ImmutableMap.of(
            S3FileIOProperties.SSE_TYPE,
            S3FileIOProperties.DSSE_TYPE_KMS,
            S3FileIOProperties.SSE_KEY,
            kmsKeyArn));
    write(s3FileIO);
    validateRead(s3FileIO);
    GetObjectResponse response =
        s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(objectKey).build())
            .response();
    assertThat(response.serverSideEncryption()).isEqualTo(ServerSideEncryption.AWS_KMS_DSSE);
    assertThat(response.ssekmsKeyId()).isEqualTo(kmsKeyArn);
  }

  @Test
  public void testServerSideCustomEncryption() throws Exception {
    requireKMSEncryptionSupport();
    // generate key
    KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
    keyGenerator.init(256, new SecureRandom());
    SecretKey secretKey = keyGenerator.generateKey();
    Base64.Encoder encoder = Base64.getEncoder();
    String encodedKey = new String(encoder.encode(secretKey.getEncoded()), StandardCharsets.UTF_8);
    // generate md5
    MessageDigest digest = MessageDigest.getInstance("MD5");
    String md5 =
        new String(encoder.encode(digest.digest(secretKey.getEncoded())), StandardCharsets.UTF_8);

    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    Map<String, String> properties =
        ImmutableMap.of(
            S3FileIOProperties.SSE_TYPE,
            S3FileIOProperties.SSE_TYPE_CUSTOM,
            S3FileIOProperties.SSE_KEY,
            encodedKey,
            S3FileIOProperties.SSE_MD5,
            md5);
    skipIfAnalyticsAcceleratorEnabled(
        new S3FileIOProperties(properties),
        "Analytics Accelerator Library does not support Server Side Custom Encryption");
    s3FileIO.initialize(properties);
    write(s3FileIO);
    validateRead(s3FileIO);
    GetObjectResponse response =
        s3.getObject(
                GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
                    .sseCustomerKey(encodedKey)
                    .sseCustomerKeyMD5(md5)
                    .build())
            .response();
    assertThat(response.serverSideEncryption()).isNull();
    assertThat(response.sseCustomerAlgorithm()).isEqualTo(ServerSideEncryption.AES256.toString());
    assertThat(response.sseCustomerKeyMD5()).isEqualTo(md5);
  }

  @Test
  public void testACL() throws Exception {
    requireACLSupport();
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(
        ImmutableMap.of(
            S3FileIOProperties.ACL, ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL.toString()));

    write(s3FileIO);
    validateRead(s3FileIO);
    GetObjectAclResponse response =
        s3.getObjectAcl(GetObjectAclRequest.builder().bucket(bucketName).key(objectKey).build());
    assertThat(response.hasGrants()).isTrue();
    assertThat(response.grants())
        .hasSize(1)
        .first()
        .satisfies(grant -> assertThat(grant.permission()).isEqualTo(Permission.FULL_CONTROL));
  }

  @Test
  public void testClientFactorySerialization() throws Exception {
    S3FileIO fileIO = new S3FileIO(clientFactory::s3);
    fileIO.initialize(ImmutableMap.of());
    write(fileIO);
    byte[] data = TestHelpers.serialize(fileIO);
    S3FileIO fileIO2 = TestHelpers.deserialize(data);
    validateRead(fileIO2);
  }

  @Test
  public void testDeleteFilesMultipleBatches() throws Exception {
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(
        ImmutableMap.of(S3FileIOProperties.DELETE_BATCH_SIZE, Integer.toString(deletionBatchSize)));

    testDeleteFiles(deletionBatchSize * 2, s3FileIO);
  }

  @Test
  public void testDeleteFilesMultipleBatchesWithAccessPoints() throws Exception {
    requireAccessPointSupport();
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(
        ImmutableMap.of(
            S3FileIOProperties.DELETE_BATCH_SIZE,
            Integer.toString(deletionBatchSize),
            S3FileIOProperties.ACCESS_POINTS_PREFIX + bucketName,
            testAccessPointARN(AwsIntegTestUtil.testRegion(), accessPointName)));
    testDeleteFiles(deletionBatchSize * 2, s3FileIO);
  }

  @Test
  public void testDeleteFilesMultipleBatchesWithCrossRegionAccessPoints() throws Exception {
    requireKMSEncryptionSupport();
    clientFactory.initialize(ImmutableMap.of(S3FileIOProperties.USE_ARN_REGION_ENABLED, "true"));
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(
        ImmutableMap.of(
            S3FileIOProperties.DELETE_BATCH_SIZE,
            Integer.toString(deletionBatchSize),
            S3FileIOProperties.ACCESS_POINTS_PREFIX + bucketName,
            testAccessPointARN(AwsIntegTestUtil.testCrossRegion(), crossRegionAccessPointName)));
    testDeleteFiles(deletionBatchSize * 2, s3FileIO);
  }

  @Test
  public void testDeleteFilesLessThanBatchSize() throws Exception {
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(
        ImmutableMap.of(S3FileIOProperties.DELETE_BATCH_SIZE, Integer.toString(deletionBatchSize)));
    testDeleteFiles(deletionBatchSize - 1, s3FileIO);
  }

  @Test
  public void testDeleteFilesSingleBatchWithRemainder() throws Exception {
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(
        ImmutableMap.of(S3FileIOProperties.DELETE_BATCH_SIZE, Integer.toString(deletionBatchSize)));
    testDeleteFiles(5, s3FileIO);
  }

  @SuppressWarnings("DangerousParallelStreamUsage")
  @Test
  public void testPrefixList() {
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(ImmutableMap.of());
    List<Integer> scaleSizes = Lists.newArrayList(1, 1000, 2500);
    String listPrefix = String.format("s3://%s/%s/%s", bucketName, prefix, "prefix-list-test");

    scaleSizes.parallelStream()
        .forEach(
            scale -> {
              String scalePrefix = String.format("%s/%s/", listPrefix, scale);
              createRandomObjects(scalePrefix, scale);
              assertThat(s3FileIO.listPrefix(scalePrefix)).hasSize(scale);
            });

    long totalFiles = scaleSizes.stream().mapToLong(Integer::longValue).sum();
    assertThat(s3FileIO.listPrefix(listPrefix)).hasSize((int) totalFiles);
  }

  @SuppressWarnings("DangerousParallelStreamUsage")
  @Test
  public void testPrefixDelete() {
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(ImmutableMap.of(S3FileIOProperties.DELETE_BATCH_SIZE, "100"));
    String deletePrefix = String.format("s3://%s/%s/%s", bucketName, prefix, "prefix-delete-test");

    List<Integer> scaleSizes = Lists.newArrayList(0, 5, 1000, 2500);
    scaleSizes.parallelStream()
        .forEach(
            scale -> {
              String scalePrefix = String.format("%s/%s/", deletePrefix, scale);
              createRandomObjects(scalePrefix, scale);
              s3FileIO.deletePrefix(scalePrefix);
              assertThat(s3FileIO.listPrefix(scalePrefix)).isEmpty();
            });
  }

  @Test
  public void testFileRecoveryHappyPath() throws Exception {
    requireVersioningSupport();
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(ImmutableMap.of());
    String filePath = String.format("s3://%s/%s/%s", bucketName, prefix, "someFile.parquet");
    write(s3FileIO, filePath);
    s3FileIO.deleteFile(filePath);
    assertThat(s3FileIO.newInputFile(filePath).exists()).isFalse();

    assertThat(s3FileIO.recoverFile(filePath)).isTrue();
    assertThat(s3FileIO.newInputFile(filePath).exists()).isTrue();
  }

  @Test
  public void testFileRecoveryFailsToRecover() throws Exception {
    requireVersioningSupport();
    S3FileIO s3FileIO = new S3FileIO(clientFactory::s3);
    s3FileIO.initialize(ImmutableMap.of());
    s3.putBucketVersioning(
        PutBucketVersioningRequest.builder()
            .bucket(bucketName)
            .versioningConfiguration(
                VersioningConfiguration.builder().status(BucketVersioningStatus.SUSPENDED).build())
            .build());
    String filePath = String.format("s3://%s/%s/%s", bucketName, prefix, "unversionedFile.parquet");
    write(s3FileIO, filePath);
    s3FileIO.deleteFile(filePath);
    assertThat(s3FileIO.newInputFile(filePath).exists()).isFalse();

    assertThat(s3FileIO.recoverFile(filePath)).isFalse();
  }

  private void testDeleteFiles(int numObjects, S3FileIO s3FileIO) throws Exception {
    List<String> paths = Lists.newArrayList();
    for (int i = 1; i <= numObjects; i++) {
      String deletionKey = objectKey + "-deletion-" + i;
      write(s3FileIO, String.format("s3://%s/%s/%s", bucketName, prefix, deletionKey));
      paths.add(String.format("s3://%s/%s/%s", bucketName, prefix, deletionKey));
    }
    s3FileIO.deleteFiles(paths);
    for (String path : paths) {
      assertThat(s3FileIO.newInputFile(path).exists()).isFalse();
    }
  }

  private void write(S3FileIO s3FileIO) throws Exception {
    write(s3FileIO, objectUri);
  }

  private void write(S3FileIO s3FileIO, String uri) throws Exception {
    OutputFile outputFile = s3FileIO.newOutputFile(uri);
    try (OutputStream outputStream = outputFile.create()) {
      IoUtils.copy(new ByteArrayInputStream(contentBytes), outputStream);
    }
  }

  private void validateRead(S3FileIO s3FileIO) throws Exception {
    validateRead(s3FileIO, objectUri);
  }

  private void validateRead(S3FileIO s3FileIO, String s3Uri) throws Exception {
    InputFile file = s3FileIO.newInputFile(s3Uri);
    assertThat(file.getLength()).isEqualTo(contentBytes.length);
    try (InputStream stream = file.newStream()) {
      String result = IoUtils.toUtf8String(stream);
      assertThat(result).isEqualTo(content);
    }
  }

  private String testAccessPointARN(String region, String accessPoint) {
    // format: arn:aws:s3:region:account-id:accesspoint/resource
    return String.format(
        "arn:%s:s3:%s:%s:accesspoint/%s",
        PartitionMetadata.of(Region.of(region)).id(),
        region,
        AwsIntegTestUtil.testAccountId(),
        accessPoint);
  }

  private String testMultiRegionAccessPointARN(String region, String alias) {
    // format: arn:aws:s3::account-id:accesspoint/MultiRegionAccessPoint_alias
    return String.format(
        "arn:%s:s3::%s:accesspoint/%s",
        PartitionMetadata.of(Region.of(region)).id(), AwsIntegTestUtil.testAccountId(), alias);
  }

  private void createRandomObjects(String objectPrefix, int count) {
    S3URI s3URI = new S3URI(objectPrefix);
    random
        .ints(count)
        .parallel()
        .forEach(
            i ->
                s3.putObject(
                    builder -> builder.bucket(s3URI.bucket()).key(s3URI.key() + i).build(),
                    RequestBody.empty()));
  }

  /** S3 Express doesn't support access points */
  private void requireAccessPointSupport() {
    assumeThat(S3URI.isS3DirectoryBucket(bucketName)).isFalse();
  }

  /** S3 Express doesn’t support KMS/custom encryption */
  private void requireKMSEncryptionSupport() {
    assumeThat(S3URI.isS3DirectoryBucket(bucketName)).isFalse();
  }

  /** S3 Express doesn't support versioning */
  private void requireVersioningSupport() {
    assumeThat(S3URI.isS3DirectoryBucket(bucketName)).isFalse();
  }

  /** File ACLs aren’t supported by S3 Express */
  private void requireACLSupport() {
    assumeThat(S3URI.isS3DirectoryBucket(bucketName)).isFalse();
  }
}
